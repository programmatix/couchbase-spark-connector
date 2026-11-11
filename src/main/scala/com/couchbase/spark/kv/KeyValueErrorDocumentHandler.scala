/*
 * Copyright (c) 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.spark.kv

import com.couchbase.client.core.error.{
  CouchbaseException,
  DocumentNotFoundException,
  TimeoutException
}
import com.couchbase.client.core.retry.RetryReason
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.diagnostics.WaitUntilReadyOptions
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.{RemoveOptions, UpsertOptions}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class KeyValueErrorDocumentHandler(
    writeConfig: KeyValueWriteConfig,
    couchbaseConfig: CouchbaseConfig
) extends Logging
    with Serializable {

   private def bucketName: String = writeConfig.errorBucket.getOrElse(
    throw new IllegalArgumentException(
      "errorBucket must be specified when using ErrorDocumentHandler"
    )
  )
  private def scopeName: String =
    writeConfig.errorScope.getOrElse(DefaultConstants.DefaultScopeName)
  private def collectionName: String =
    writeConfig.errorCollection.getOrElse(DefaultConstants.DefaultCollectionName)

  // Transient, as the SDK's Collection can't be sent to tasks: each task opens its own.
  @transient private lazy val errorCollection =
    CouchbaseConnection(writeConfig.connectionIdentifier)
      .cluster(couchbaseConfig)
      .bucket(bucketName)
      .scope(scopeName)
      .collection(collectionName)

  /** Checks, before the write starts, that error documents can be written: that the bucket, scope
    * and collection exist, and the user can write to them. Otherwise every failure would retry its
    * error document for [[KeyValueOptions.ErrorHandlerTimeout]] before failing the task.
    *
    * It removes a document that doesn't exist, which changes nothing, and needs no more permission
    * than writing error documents does. Listing the collections (`getAllScopes`), which would be
    * more direct, needs the `collections!read` permission, which the Data Reader and Data Writer
    * roles don't have.
    *
    * The bucket is waited for first, for its KV service only, so that a bucket that is slow to open
    * isn't mistaken for one that doesn't exist. If it can't be opened, the write fails: a missing
    * bucket, or one the user can't access, is the likely reason, though it can't be told apart from
    * a cluster too slow to open it.
    *
    * Throws if error documents can't be written. If it can't tell, as the cluster isn't responding,
    * it logs a warning and carries on: writing the error documents is retried anyway.
    */
  private[kv] def checkCanWrite(): Unit = {
    import KeyValueErrorDocumentHandler._
    val where      = s"collection '$collectionName' of scope '$scopeName' in bucket '$bucketName'"
    val connection = new CouchbaseConnection(s"cbspark-error-bucket-check-${UUID.randomUUID()}")
    val readyTimeout =
      couchbaseConfig.waitUntilReadyTimeout.map(s => Duration(s)).getOrElse(1.minute)
    val outcome =
      try {
        val bucket = connection.cluster(couchbaseConfig).bucket(bucketName)
        bucket.waitUntilReady(
          readyTimeout,
          WaitUntilReadyOptions().serviceTypes(Set(ServiceType.KV))
        ) match {
          case Failure(e) =>
            NotWritable(
              s"the bucket could not be opened within $readyTimeout " +
                "(spark.couchbase.waitUntilReadyTimeout): check that it exists, and that the " +
                "user can access it",
              e
            )
          case Success(_) =>
            checkResult(
              bucket
                .scope(scopeName)
                .collection(collectionName)
                .remove(s"cbspark-check-${UUID.randomUUID()}", RemoveOptions().timeout(CheckTimeout))
            )
        }
      } catch {
        case NonFatal(e) => Unknown(e)
      } finally connection.stop()
    outcome match {
      case Writable =>
      case NotWritable(reason, cause) =>
        throw new IllegalArgumentException(
          s"KeyValueOptions.ErrorBucket: error documents cannot be written to the $where: " +
            s"$reason",
          cause
        )
      case Unknown(cause) =>
        logWarning(
          s"Could not check that KeyValueOptions.ErrorBucket documents can be written to the " +
            s"$where, so carrying on: $cause"
        )
    }
  }

  /** Writes the error document for a failed write, retrying until it is written or
    * [[KeyValueOptions.ErrorHandlerTimeout]] has nearly passed. Throws if it can't be written.
    */
  def handleError(
      documentId: String,
      originalBucket: String,
      originalScope: String,
      originalCollection: String,
      error: Throwable
  ): Unit =
    handleError(documentId, originalBucket, originalScope, originalCollection, error, _ => ())

  /** @param onRetry
    *   called with the failure before each retry.
    */
  private[kv] def handleError(
      documentId: String,
      originalBucket: String,
      originalScope: String,
      originalCollection: String,
      error: Throwable,
      onRetry: Throwable => Unit
  ): Unit = {
    val deadline =
      System.nanoTime() +
        KeyValueErrorDocumentHandler.retryFor(writeConfig.errorHandlerTimeout).toNanos

    // Created once, so that every attempt writes the same document: an attempt that timed out may
    // have written it, and a retry then just writes it again.
    val errorDocId = KeyValueErrorDocumentHandler.errorDocumentId(
      originalBucket,
      originalScope,
      originalCollection,
      documentId,
      System.currentTimeMillis()
    )

    val errorJson = JsonObject.create
      .put("class", error.getClass.getName)
      .put("simpleClass", error.getClass.getSimpleName)

    error match {
      case e: CouchbaseException => errorJson.put("context", e.context.exportAsMap)
      // Logging this for CouchbaseException would include the context again
      case _ => errorJson.put("message", error.getMessage)
    }

    val errorDocJson = JsonObject.create
      .put("timestamp", java.time.Instant.now().toString)
      .put("documentId", documentId)
      .put("bucket", originalBucket)
      .put("scope", originalScope)
      .put("collection", originalCollection)
      .put("error", errorJson)

    val collection = errorCollection
    val kvTimeout =
      collection.async.core.context().environment().timeoutConfig().kvTimeout().toNanos.nanos

    // Throws if it can't be written, which fails the task: the failure must not be lost.
    KeyValueErrorDocumentHandler.writeWithRetry(
      documentId,
      deadline,
      onRetry = e => {
        logInfo(s"Retrying the error document for document '$documentId': $e")
        onRetry(e)
      }
    ) { timeLeft =>
      collection
        .upsert(errorDocId, errorDocJson, UpsertOptions().timeout(timeLeft.min(kvTimeout)))
        .map(_ => ())
    }
  }
}

object KeyValueErrorDocumentHandler {

  /** How long [[KeyValueErrorDocumentHandler.checkCanWrite]] waits. */
  private[kv] val CheckTimeout: FiniteDuration = 10.seconds

  private[kv] sealed trait CheckResult
  private[kv] case object Writable                                         extends CheckResult
  private[kv] case class NotWritable(reason: String, cause: Throwable) extends CheckResult
  private[kv] case class Unknown(cause: Throwable)                         extends CheckResult

  /** What the result of removing a document that doesn't exist, in a bucket that's open, says about
    * whether error documents can be written.
    *
    * A missing scope or collection is reported as a timeout, as the SDK waits in case it's being
    * created: what it was waiting for says so.
    */
  private[kv] def checkResult(result: Try[Any]): CheckResult = result match {
    case Success(_) | Failure(_: DocumentNotFoundException) => Writable
    case Failure(e: CouchbaseException) if e.isInstanceOf[TimeoutException] =>
      if (WriteRetry.retryReasons(e).contains(RetryReason.COLLECTION_NOT_FOUND)) {
        NotWritable("the scope or collection does not exist", e)
      } else Unknown(e)
    // Such as the user not having permission to write to it.
    case Failure(e: CouchbaseException) => NotWritable(e.toString, e)
    case Failure(e)                     => Unknown(e)
  }

  /** How long an error document is retried for, given how long a handler call may take: a little
    * less, so that giving up is reported as such, rather than as the call timing out.
    */
  private[kv] def retryFor(callTimeout: FiniteDuration): FiniteDuration =
    callTimeout - (callTimeout / 10).min(1.second)

  /** Whether retrying the error document's write could help. Error documents are written while
    * writes are failing, quite likely because the cluster is struggling, so every Couchbase error
    * is retried.
    */
  private[kv] def isRetryable(err: Throwable): Boolean = err match {
    case _: CouchbaseException => true
    case _                     => false
  }

  /** Runs `attempt` until it succeeds, fails with an error that isn't [[isRetryable]], or there
    * isn't time before `deadline` (from `nanoTime`) to wait and try again. Waits between attempts
    * as [[WriteRetry.backoff]] does. Throws if it doesn't succeed.
    *
    * @param attempt
    *   is passed the time left until the deadline, to time the attempt out by.
    */
  private[kv] def writeWithRetry(
      documentId: String,
      deadline: Long,
      nanoTime: () => Long = () => System.nanoTime(),
      sleep: FiniteDuration => Unit = d => Thread.sleep(d.toMillis),
      random: () => Double = () => ThreadLocalRandom.current().nextDouble(),
      onRetry: Throwable => Unit = _ => ()
  )(attempt: FiniteDuration => Try[Unit]): Unit = {
    val started  = nanoTime()
    var attempts = 0

    def notWritten(e: Throwable, retryable: Boolean) = {
      val elapsed = f"${(nanoTime() - started) / 1e9}%.1f"
      new ErrorDocumentNotWrittenException(
        s"Could not write the KeyValueOptions.ErrorBucket document for document '$documentId' " +
          s"in $attempts attempt${if (attempts == 1) "" else "s"} over ${elapsed}s" +
          (if (retryable) ", within KeyValueOptions.ErrorHandlerTimeout"
           else ", as retrying would not help") +
          s": $e",
        e
      )
    }

    while (true) {
      attempts += 1
      attempt((deadline - nanoTime()).nanos) match {
        case Success(_) => return
        case Failure(e) =>
          if (!isRetryable(e)) throw notWritten(e, retryable = false)
          val wait = WriteRetry.backoff(attempts, random())
          // Only worth waiting if there would be time left to try again.
          if (deadline - nanoTime() <= wait.toNanos) throw notWritten(e, retryable = true)
          onRetry(e)
          sleep(wait)
      }
    }
  }

  /** SPARKC-243: The ID of the error document for one failed operation.
    *
    * The original document's location and ID are hashed, rather than included verbatim, so the ID is always
    * well within the 250-byte key limit whatever the length of the original ID.  All of them are in the error
    * document's body.  All failures of one document share the same hash, and the timestamp separates them.
    *
    * '/' cannot appear in bucket, scope or collection names, so it separates them unambiguously.
    */
  private[kv] def errorDocumentId(
      bucket: String,
      scope: String,
      collection: String,
      documentId: String,
      timestampMillis: Long
  ): String = {
    val digest = MessageDigest
      .getInstance("SHA-256")
      .digest(s"$bucket/$scope/$collection/$documentId".getBytes(StandardCharsets.UTF_8))
    // 128 bits is plenty to avoid collisions.
    val hash = digest.take(16).map(b => f"${b & 0xff}%02x").mkString
    s"error_${hash}_$timestampMillis"
  }
}

/** A [[KeyValueOptions.ErrorBucket]] document could not be written. */
private[kv] class ErrorDocumentNotWrittenException(message: String, cause: Throwable)
    extends RuntimeException(message, cause)
