/*
 * Copyright (c) 2026 Couchbase, Inc.
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

import com.couchbase.client.core.error.context.{CancellationErrorContext, SubDocumentErrorContext}
import com.couchbase.client.core.error.subdoc.PathExistsException
import com.couchbase.client.core.retry.RetryReason
import com.couchbase.client.core.error.{
  AmbiguousTimeoutException,
  CouchbaseException,
  DocumentLockedException,
  DurabilityAmbiguousException,
  DurableWriteInProgressException,
  DurableWriteReCommitInProgressException,
  RequestCanceledException,
  ServerOutOfMemoryException,
  TemporaryFailureException,
  TimeoutException,
  UnambiguousTimeoutException
}
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.codec.RawJsonTranscoder
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.{GetOptions, LookupInOptions, LookupInSpec, MutateInSpec}
import com.fasterxml.jackson.databind.ObjectMapper

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import java.util.{Base64, UUID}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Support for [[KeyValueOptions.Idempotency]]: the key that identifies each row, and the
  * sub-document specs that record it in the document.
  */
private[kv] object IdempotentWrites {

  /** The user xattr holding the keys. A user xattr (no leading underscore) only needs the normal
    * data-write permission.
    */
  val XattrName = "cbspark"

  private val ValidWriteId = "[A-Za-z0-9_-]{1,64}".r

  def generateWriteId(): String = UUID.randomUUID().toString

  /** Returns the id unchanged if it's valid, otherwise throws. The restricted character set means
    * the id can be used as a sub-document path component without escaping.
    */
  def validateWriteId(writeId: String): String = {
    if (!ValidWriteId.matches(writeId)) {
      throw new IllegalArgumentException(
        s"KeyValueOptions.WriteId must be 1 to 64 characters from A-Z, a-z, 0-9, '-' and '_', " +
          s"but was: '$writeId'"
      )
    }
    writeId
  }

  private val digest =
    ThreadLocal.withInitial[MessageDigest](() => MessageDigest.getInstance("SHA-256"))

  /** Identifies a row within a write: a hash of the row's full JSON encoding, which includes the
    * document id.
    *
    * It must not depend on where the row is in the input. After a failure Spark may re-run a stage
    * whose output is indeterminate (e.g. a round-robin `repartition`), so a row can end up in a
    * different partition, at a different position.
    *
    * 128 bits of SHA-256, as 22 characters of URL-safe base64 (A-Z, a-z, 0-9, '-' and '_'), so it
    * can also be used as a path component without escaping.
    */
  def rowKey(encodedRow: String): String = {
    val hash = digest.get().digest(encodedRow.getBytes(StandardCharsets.UTF_8))
    Base64.getUrlEncoder.withoutPadding().encodeToString(java.util.Arrays.copyOf(hash, 16))
  }

  def markerPath(writeId: String, rowKey: String): String = s"$XattrName.$writeId.$rowKey"

  /** The specs to put before the user's specs.
    *
    * The first is the guard: inserting the key, which fails with PathExistsException if the row has
    * already been applied, causing the whole write to be rejected.
    *
    * The second replaces the whole xattr with just this key, so it stays small however often the
    * document is written. Specs are applied in order, so the guard still checks the previous
    * contents. The trade-off is that only the most recent row written to each document is
    * protected.
    *
    * Afterwards the document's xattrs contain, for example (about 80 bytes):
    * {{{
    * "cbspark": {
    *   "6f1c2a9e-4b7d-4e2a-9c31-0d8f5e7a2b14": {   // writeId
    *     "f2EVTea6bD3nj_mEJeQ0vQ": 1763030400000   // rowKey: time written (epoch millis)
    *   }
    * }
    * }}}
    * The timestamp is only for debugging; nothing reads it.
    */
  def markerSpecs(writeId: String, rowKey: String, nowMillis: Long): Seq[MutateInSpec] =
    Seq(
      MutateInSpec.insert(markerPath(writeId, rowKey), nowMillis).xattr.createPath,
      MutateInSpec
        .upsert(XattrName, JsonObject(writeId -> JsonObject(rowKey -> nowMillis)))
        .xattr
    )

  private val mapper = new ObjectMapper()

  /** Whether `stored` (a document's raw content) is the same JSON as `written`, ignoring the order
    * of fields. Both are parsed the same way, so numbers compare as they were written.
    */
  def sameJson(stored: Array[Byte], written: String): Boolean =
    mapper.readTree(stored) == mapper.readTree(written)

  /** Whether the document has this row's idempotency key: this row has already been applied. */
  def markerPresent(
      coll: Collection,
      id: String,
      path: String,
      timeout: Option[Duration]
  ): Try[Boolean] = {
    var opts = LookupInOptions()
    timeout.foreach(t => opts = opts.timeout(t))
    coll.lookupIn(id, Seq(LookupInSpec.exists(path).xattr), opts).map(_.exists(0))
  }

  /** Whether the document's current content is the same JSON as `content`, ignoring field order: a
    * full-document write of `content` has already been applied (or someone else wrote the same).
    * A document that isn't JSON doesn't match.
    *
    * `content` must be exactly what the write sends. If that changes (e.g. written directly from
    * Spark's rows rather than via JsonObject), this must change with it, or repeats will be
    * reported as conflicts again.
    */
  def contentMatches(
      coll: Collection,
      id: String,
      content: String,
      timeout: Option[Duration]
  ): Try[Boolean] = {
    var opts = GetOptions().transcoder(RawJsonTranscoder.Instance)
    timeout.foreach(t => opts = opts.timeout(t))
    coll
      .get(id, opts)
      .flatMap(_.contentAs[Array[Byte]])
      .map(stored => Try(sameJson(stored, content)).getOrElse(false))
  }

  /** The result of a write that conflicted (the document exists, or its CAS has changed), given
    * whether a read then found this row already applied.
    *
    * If the read failed in a way that's worth retrying (e.g. it timed out), the read's failure is
    * the result, so that [[KeyValueOptions.RetryAttempts]] repeats the write and checks again.
    * Reporting the conflict instead would often be wrong: it's likely this row's own earlier
    * attempt, which timed out but was applied. Repeating the write is safe, as it conflicts again.
    * The read uses the write's timeout, and its failure feeds the partition's backoff like any
    * other, so a struggling cluster is given time to recover rather than waited on for longer.
    */
  def resolveConflict(conflict: Throwable, alreadyApplied: Try[Boolean]): Try[WriteRetry.Outcome] =
    alreadyApplied match {
      case Success(true)  => Success(WriteRetry.Deduplicated)
      case Success(false) => Failure(conflict)
      case Failure(readErr) if WriteRetry.classify(readErr) != WriteRetry.NotRetryable =>
        readErr.addSuppressed(conflict)
        Failure(readErr)
      case Failure(readErr) =>
        conflict.addSuppressed(readErr)
        Failure(conflict)
    }

  /** How many of the 16 sub-document specs the marker uses. */
  def specsUsed(idempotency: Option[String]): Int = if (idempotency.isDefined) 2 else 0

  /** Whether this failure is the guard spec finding the key already present: the row has already
    * been applied.
    *
    * The SDK reports the index of the failing spec in the list the connector passed it (before the
    * SDK reorders xattr specs first), and the guard is always first.
    */
  def isGuardRejection(err: Throwable): Boolean = err match {
    case e: PathExistsException =>
      e.context() match {
        case ctx: SubDocumentErrorContext => ctx.index() == 0
        case _                            => false
      }
    case _ => false
  }
}

/** Support for [[KeyValueOptions.RetryAttempts]]. */
private[kv] object WriteRetry {

  /** How a failed operation relates to the server's state. */
  sealed trait FailureKind

  /** The server definitely did not apply the operation, so it is always safe to retry. */
  case object NotApplied extends FailureKind

  /** The server may or may not have applied the operation. */
  case object Ambiguous extends FailureKind

  /** Retrying would not help, e.g. the document does not exist. */
  case object NotRetryable extends FailureKind

  def classify(err: Throwable): FailureKind = err match {
    case _: UnambiguousTimeoutException | _: TemporaryFailureException |
        _: ServerOutOfMemoryException | _: DocumentLockedException |
        _: DurableWriteInProgressException | _: DurableWriteReCommitInProgressException =>
      NotApplied
    // The SDK raises AmbiguousTimeoutException for any mutation that times out, even if every
    // attempt was rejected by the server, as it cannot know whether the last attempt was applied.
    case _: AmbiguousTimeoutException | _: DurabilityAmbiguousException |
        _: RequestCanceledException =>
      Ambiguous
    case _ => NotRetryable
  }

  val BaseDelay: FiniteDuration = 100.millis
  val MaxDelay: FiniteDuration  = 5.seconds

  /** The wait before retry number `retry` (from 1): exponential, capped at [[MaxDelay]], with half
    * of it randomised (`random` is in [0, 1)) so that many threads failing together don't all retry
    * at the same moment.
    */
  def backoff(retry: Int, random: Double): FiniteDuration = {
    val exponential = BaseDelay.toMillis * (1L << math.min(math.max(retry - 1, 0), 20))
    val capped      = math.min(exponential, MaxDelay.toMillis)
    (capped / 2 + (capped / 2 * random).toLong).millis
  }

  /** What happened to a row that was written successfully. */
  sealed trait Outcome
  case object Applied      extends Outcome
  case object Deduplicated extends Outcome

  /** Runs `attempt` until it succeeds, fails in a way that should not be retried, or `maxRetries`
    * retries have been made. Returns the last result.
    *
    * @param replaySafe
    *   whether repeating an operation that may already have been applied is safe.
    * @param attempt
    *   is passed the attempt number, from 0.
    * @param decide
    *   decides whether to retry, given the retry number it would be (from 1), the failure, and
    *   whether it would be retried by default. Only called while `maxRetries` allows a retry. May
    *   throw, to fail the row's write.
    * @param onRetry
    *   is called before each retry, with the retry number (from 1) and the failure.
    * @param stopped
    *   when this returns true no more retries are made, and the last failure is returned. Used to
    *   stop retrying once the partition has failed (or the task been killed), so that a failure
    *   isn't held up by other rows working through their retries.
    */
  def run(
      maxRetries: Int,
      replaySafe: Boolean,
      sleep: FiniteDuration => Unit = d => Thread.sleep(d.toMillis),
      random: () => Double = () => java.util.concurrent.ThreadLocalRandom.current().nextDouble(),
      decide: (Int, Throwable, Boolean) => Boolean = (_, _, byDefault) => byDefault,
      onRetry: (Int, Throwable) => Unit = (_, _) => (),
      stopped: () => Boolean = () => false
  )(attempt: Int => Try[Outcome]): Try[Outcome] = {
    var n      = 0
    var result = attempt(0)
    while (
      result.isFailure && n < maxRetries && !stopped() && {
        val err = result.failed.get
        decide(n + 1, err, shouldRetry(err, replaySafe))
      }
    ) {
      sleep(backoff(n + 1, random()))
      // Checked again after waiting, as the wait can be several seconds.
      if (stopped()) return result
      n += 1
      onRetry(n, result.failed.get)
      result = attempt(n)
    }
    result
  }

  private def shouldRetry(err: Throwable, replaySafe: Boolean): Boolean = classify(err) match {
    case NotApplied   => true
    case Ambiguous    => replaySafe
    case NotRetryable => false
  }

  // Retried by the SDK because of other writers to the same document, not the cluster's health.
  private val ContentionReasons: Set[RetryReason] = Set(
    RetryReason.KV_LOCKED,
    RetryReason.KV_SYNC_WRITE_IN_PROGRESS,
    RetryReason.KV_SYNC_WRITE_RE_COMMIT_IN_PROGRESS
  )

  /** Whether this failure suggests the cluster is struggling - overloaded, or nodes unreachable -
    * rather than being about the document, so that writes should slow down.
    *
    * A timeout usually does, but not if the SDK was only retrying because the document was locked,
    * or had a durable write in progress: then it's contention for that one document.
    *
    * Note these checks don't rule out other causes of these errors: in particular we can see timeouts
    * if the cluster is down.  An earlier check does check if the cluster was up before the partition
    * writes started.
    */
  def isPossiblyClusterStrain(err: Throwable): Boolean = err match {
    case e: CouchbaseException
        if e.isInstanceOf[TimeoutException] || e.isInstanceOf[RequestCanceledException] =>
      !onlyRetriedFor(e, ContentionReasons)
    case _: TemporaryFailureException | _: ServerOutOfMemoryException |
        _: DurabilityAmbiguousException =>
      true
    case _ => false
  }

  private def onlyRetriedFor(e: CouchbaseException, reasons: Set[RetryReason]): Boolean = {
    val tried = retryReasons(e)
    tried.nonEmpty && tried.forall(reasons.contains)
  }

  /** Why the SDK retried the request that failed, for a timeout or cancellation. */
  def retryReasons(e: CouchbaseException): Set[RetryReason] = e.context() match {
    case ctx: CancellationErrorContext if ctx.requestContext() != null =>
      Option(ctx.requestContext().retryReasons()).map(_.asScala.toSet).getOrElse(Set.empty)
    case _ => Set.empty
  }
}

/** Slows every writer in a partition while the cluster is struggling, so that the connector
  * reduces its load rather than adding retries to it.
  *
  * Each writer calls [[pause]] before each operation, and [[record]] after it. A failure that
  * suggests the cluster is struggling ([[WriteRetry.isPossiblyClusterStrain]]) starts or doubles a delay,
  * which every writer then waits before its operations, until successes bring it back down. It's
  * shared by the partition's writers, so all of them slow down as soon as any one sees the cluster
  * struggling, rather than each finding out for itself.
  *
  * Each success only reduces the delay by a tenth, so the delay rises if more than about 1 in 8
  * operations are failing, and it takes a few dozen successes to recover from the maximum.
  */
private[kv] class PartitionBackoff(
    stats: PartitionWriteStats,
    stopped: () => Boolean,
    sleep: Long => Unit = ms => Thread.sleep(ms),
    random: () => Double = () => java.util.concurrent.ThreadLocalRandom.current().nextDouble()
) {
  import PartitionBackoff._

  private val delayMillis = new AtomicLong(0)

  def delay: FiniteDuration = delayMillis.get().millis

  def record(result: Try[_]): Unit = result match {
    case Failure(e) if WriteRetry.isPossiblyClusterStrain(e) =>
      stats.recordBackoffDelay(
        delayMillis.updateAndGet(d => if (d == 0) BaseDelay.toMillis else math.min(d * 2, Max))
      )
    case Success(_) if delayMillis.get() != 0 =>
      stats.recordBackoffDelay(delayMillis.updateAndGet { d =>
        val reduced = d - d / 10
        if (reduced < MinDelay.toMillis) 0 else reduced
      })
    case _ =>
  }

  /** Waits for the current delay, randomised, or `atLeast` if that's longer: for a retry, this is
    * the retry's own backoff, so the two waits aren't added together. Returns early once `stopped`.
    */
  def pause(atLeast: FiniteDuration = Duration.Zero): Unit = {
    val d       = delayMillis.get()
    val partial = if (d == 0) 0L else d / 2 + (d / 2 * random()).toLong
    val total   = math.max(partial, atLeast.toMillis)
    if (total > 0) {
      var remaining = total
      while (remaining > 0 && !stopped()) {
        val slice = math.min(remaining, StopCheckInterval.toMillis)
        sleep(slice)
        remaining -= slice
      }
      // Only the time added because of the cluster's health.
      val slept = total - remaining
      if (partial > atLeast.toMillis) {
        stats.recordBackoffPause(math.max(0L, slept - atLeast.toMillis).millis)
      }
    }
  }
}

private[kv] object PartitionBackoff {
  val BaseDelay: FiniteDuration = WriteRetry.BaseDelay
  val MaxDelay: FiniteDuration  = WriteRetry.MaxDelay

  /** Below this the delay is dropped altogether. */
  val MinDelay: FiniteDuration = 25.millis

  private val Max               = MaxDelay.toMillis
  private val StopCheckInterval = 50.millis
}
