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

import com.couchbase.client.core.error.DocumentExistsException
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.util.SparkOperationalSimpleTest
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Records what it is given, and behaves as its options say: `errorHandler.decision` (continue,
  * ignore or fail), `errorHandler.retry` (retry, giveUp or fail) and `errorHandler.throwIn` (open,
  * onFailure or close).
  */
class RecordingFailureHandler extends KeyValueWriteFailureHandler {
  @transient private var context: KeyValueWriteContext = _

  private def recordThread(): Unit =
    RecordingFailureHandler.threads.add(Thread.currentThread().getName)

  override def open(context: KeyValueWriteContext): Unit = {
    recordThread()
    this.context = context
    RecordingFailureHandler.opened.add(context)
    if (context.option("throwIn").contains("open")) throw new IllegalStateException("open failed")
  }

  override def onAttemptFailed(attempt: KeyValueWriteFailedAttempt): KeyValueWriteRetryDecision = {
    recordThread()
    RecordingFailureHandler.attempts.add(attempt)
    context.option("retry").map(_.toLowerCase) match {
      case Some("retry")  => KeyValueWriteRetryDecision.Retry
      case Some("giveup") => KeyValueWriteRetryDecision.GiveUp
      case Some("fail")   => KeyValueWriteRetryDecision.FailTask
      case _              => KeyValueWriteRetryDecision.Default
    }
  }

  override def onFailure(failure: KeyValueWriteFailure): KeyValueWriteFailureDecision = {
    recordThread()
    RecordingFailureHandler.failures.add(failure)
    context.option("delayMs").foreach(ms => Thread.sleep(ms.toLong))
    if (context.option("throwIn").contains("onFailure")) {
      throw new IllegalStateException("onFailure failed")
    }
    context.option("decision") match {
      case Some("ignore") => KeyValueWriteFailureDecision.Ignore
      case Some("fail")   => KeyValueWriteFailureDecision.FailTask
      case _              => KeyValueWriteFailureDecision.Continue
    }
  }

  override def close(error: Option[Throwable]): Unit = {
    recordThread()
    RecordingFailureHandler.closed.add(error)
    if (context.option("throwIn").contains("close")) throw new IllegalStateException("close failed")
  }
}

object RecordingFailureHandler {
  val opened   = new ConcurrentLinkedQueue[KeyValueWriteContext]()
  val attempts = new ConcurrentLinkedQueue[KeyValueWriteFailedAttempt]()
  val failures = new ConcurrentLinkedQueue[KeyValueWriteFailure]()
  val closed   = new ConcurrentLinkedQueue[Option[Throwable]]()
  val threads  = new ConcurrentLinkedQueue[String]()

  def clear(): Unit = {
    opened.clear(); attempts.clear(); failures.clear(); closed.clear(); threads.clear()
  }
}

class WriteFailureHandlerIntegrationTest extends SparkOperationalSimpleTest {
  override def testName: String = "failurehandler"

  @BeforeEach
  def beforeEach(): Unit = RecordingFailureHandler.clear()

  private def newId(): String = UUID.randomUUID().toString

  private def rows(ids: String*): DataFrame = {
    val sp = spark
    import sp.implicits._
    ids.map(id => (id, "value")).toDF("__META_ID", "content")
  }

  private def write(df: DataFrame, mode: SaveMode, options: (String, String)*): Unit = {
    var writer = df.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .mode(mode)
    options.foreach { case (k, v) => writer = writer.option(k, v) }
    writer.save()
  }

  private def withHandler(options: (String, String)*): Seq[(String, String)] =
    (KeyValueOptions.ErrorHandler -> classOf[RecordingFailureHandler].getName) +: options

  private lazy val collection = CouchbaseConnection()
    .cluster(CouchbaseConfig(spark.sparkContext.getConf))
    .bucket(testResources.bucketName)
    .scope(testResources.scopeName)
    .collection(testResources.collectionName)

  private def exists(id: String): Boolean = collection.exists(id).get.exists

  /** Whether the error or any of its causes mentions `text`. */
  private def mentions(t: Throwable, text: String): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).exists(_.toString.contains(text))

  private def failures = RecordingFailureHandler.failures.asScala.toSeq

  @Test
  def `continue: failures are handed over with their details, and the write carries on`(): Unit = {
    val (existing, fresh) = (newId(), newId())
    write(rows(existing), SaveMode.Overwrite)

    // Option names are case-insensitive.
    write(
      rows(existing, fresh),
      SaveMode.ErrorIfExists,
      withHandler("errorHandler.DECISION" -> "continue"): _*
    )

    assertTrue(exists(fresh))
    assertEquals(Seq(existing), failures.map(_.documentId))
    val f = failures.head
    assertEquals(1, f.errors.size)
    assertTrue(f.error.isInstanceOf[DocumentExistsException])
    assertFalse(f.mayHaveBeenApplied)
    assertEquals(testResources.collectionName, f.collection)
    val row = JsonObject.fromJson(f.rowJson)
    assertEquals(existing, row.str("__META_ID"))
    assertEquals("value", row.str("content"))

    val contexts = RecordingFailureHandler.opened.asScala.toSeq
    assertTrue(contexts.nonEmpty)
    assertEquals(contexts.size, RecordingFailureHandler.closed.size)
    assertTrue(RecordingFailureHandler.closed.asScala.forall(_.isEmpty))
    assertEquals(Some("continue"), contexts.head.option("decision"))
    assertEquals(testResources.bucketName, contexts.head.bucket)
    assertEquals(0, contexts.head.taskAttemptNumber)
    assertEquals(None, contexts.head.writeId)

    // Every call on a handler thread, never on the task's thread or a writer's.
    val threads = RecordingFailureHandler.threads.asScala.toSet
    assertTrue(
      threads.forall(_.startsWith("couchbase-kv-error-handler-partition-")),
      threads.toString
    )
    // No retries, so it was not asked about any.
    assertTrue(RecordingFailureHandler.attempts.isEmpty)
  }

  private def attempts = RecordingFailureHandler.attempts.asScala.toSeq

  @Test
  def `onAttemptFailed can retry a failure the connector would not`(): Unit = {
    val id = newId()
    write(rows(id), SaveMode.Overwrite)
    // DocumentExistsException is not retried by default.
    write(
      rows(id),
      SaveMode.ErrorIfExists,
      withHandler(KeyValueOptions.RetryAttempts -> "2", "errorHandler.retry" -> "retry"): _*
    )
    assertEquals(Seq(0, 1), attempts.map(_.attempt))
    assertEquals(Seq(2, 1), attempts.map(_.retriesRemaining))
    assertTrue(attempts.forall(!_.retriedByDefault))
    assertTrue(attempts.forall(_.error.isInstanceOf[DocumentExistsException]))
    assertEquals(3, failures.head.errors.size)
  }

  /** Writes to a locked document, which fail until the lock expires: each attempt times out. */
  private def writeLocked(options: (String, String)*): Unit = {
    val id = newId()
    write(rows(id), SaveMode.Overwrite)
    val locked = collection.getAndLock(id, 15.seconds).get
    try {
      write(
        rows(id),
        SaveMode.Overwrite,
        withHandler(
          (KeyValueOptions.RetryAttempts -> "2") +: (KeyValueOptions.Timeout -> "1s") +: options: _*
        ): _*
      )
    } finally collection.unlock(id, locked.cas)
  }

  @Test
  def `onAttemptFailed can give up on a failure the connector would retry`(): Unit = {
    writeLocked("errorHandler.retry" -> "giveUp")
    assertEquals(1, attempts.size)
    assertTrue(attempts.head.retriedByDefault)
    assertEquals(1, failures.head.errors.size)
  }

  @Test
  def `onAttemptFailed can fail the task, and the row does not reach onFailure`(): Unit = {
    val err = assertThrows(classOf[Exception], () => writeLocked("errorHandler.retry" -> "fail"))
    assertTrue(mentions(err, "TimeoutException"), err.toString)
    assertEquals(1, attempts.size)
    assertTrue(failures.isEmpty)
    assertTrue(RecordingFailureHandler.closed.asScala.exists(_.isDefined))
  }

  @Test
  def `ignore: the write succeeds`(): Unit = {
    val id = newId()
    write(rows(id), SaveMode.Overwrite)
    write(rows(id), SaveMode.ErrorIfExists, withHandler("errorHandler.decision" -> "ignore"): _*)
    assertEquals(1, failures.size)
  }

  @Test
  def `fail: the task fails with the row's error`(): Unit = {
    val id = newId()
    write(rows(id), SaveMode.Overwrite)
    val err = assertThrows(
      classOf[Exception],
      () =>
        write(rows(id), SaveMode.ErrorIfExists, withHandler("errorHandler.decision" -> "fail"): _*)
    )
    // Spark may pass the task's exception back to the driver only as text.
    assertTrue(mentions(err, "DocumentExistsException"), err.toString)
    assertTrue(RecordingFailureHandler.closed.asScala.exists(_.isDefined))
  }

  @Test
  def `fail: rows already being written finish, and their failures are handled first`(): Unit = {
    val writers = 4
    val ids     = (1 to 20).map(_ => newId())
    write(rows(ids: _*), SaveMode.Overwrite)
    val err = assertThrows(
      classOf[Exception],
      () =>
        write(
          rows(ids: _*).coalesce(1),
          SaveMode.ErrorIfExists,
          withHandler(
            "errorHandler.decision"          -> "fail",
            // So every writer has a failure waiting by the time the first is decided.
            "errorHandler.delayMs"           -> "200",
            KeyValueOptions.MaxConcurrentOps -> writers.toString
          ): _*
        )
    )
    assertTrue(mentions(err, "DocumentExistsException"), err.toString)
    // One failure per writer: the one that failed the task, and those already being written.
    // None after that, as no new rows were started.
    println(s"Failures handled: ${failures.size}")
    assertEquals(writers, failures.size)
    assertEquals(failures.size, failures.map(_.documentId).distinct.size)
    // close comes after them all.
    assertEquals(1, RecordingFailureHandler.closed.size)
    assertTrue(RecordingFailureHandler.closed.asScala.head.isDefined)
  }

  @Test
  def `a handler that throws in onFailure fails the task`(): Unit = {
    val id = newId()
    write(rows(id), SaveMode.Overwrite)
    val err = assertThrows(
      classOf[Exception],
      () =>
        write(
          rows(id),
          SaveMode.ErrorIfExists,
          withHandler("errorHandler.throwIn" -> "onFailure"): _*
        )
    )
    assertTrue(mentions(err, "onFailure failed"), err.toString)
  }

  @Test
  def `a handler that throws in open fails the task before anything is written`(): Unit = {
    val id = newId()
    val err = assertThrows(
      classOf[Exception],
      () => write(rows(id), SaveMode.Overwrite, withHandler("errorHandler.throwIn" -> "open"): _*)
    )
    assertTrue(mentions(err, "open failed"), err.toString)
    assertFalse(exists(id))
    // close is still called, with the error.
    assertTrue(RecordingFailureHandler.closed.asScala.exists(_.isDefined))
  }

  @Test
  def `a handler that throws in close fails the task`(): Unit = {
    val id = newId()
    val err = assertThrows(
      classOf[Exception],
      () => write(rows(id), SaveMode.Overwrite, withHandler("errorHandler.throwIn" -> "close"): _*)
    )
    assertTrue(mentions(err, "close failed"), err.toString)
  }

  @Test
  def `the handler is given the error from every attempt`(): Unit = {
    val id = newId()
    write(rows(id), SaveMode.Overwrite)
    // Writes to a locked document fail, and are retried, until the lock expires.
    val locked = collection.getAndLock(id, 15.seconds).get
    try {
      write(
        rows(id),
        SaveMode.Overwrite,
        withHandler(
          KeyValueOptions.RetryAttempts -> "2",
          KeyValueOptions.Timeout       -> "1s"
        ): _*
      )
    } finally collection.unlock(id, locked.cas)

    assertEquals(Seq(id), failures.map(_.documentId))
    val errors = failures.head.errors
    println(s"Errors from each attempt: ${errors.map(_.getClass.getSimpleName)}")
    assertEquals(3, errors.size, errors.toString)
    // Asked about each retry, and left it to the connector.
    assertEquals(2, attempts.size)
    // Timeouts from a locked document are contention, so don't slow the rest of the write.
    errors.foreach(e => assertFalse(WriteRetry.isPossiblyClusterStrain(e), e.toString))
  }

  @Test
  def `a call to the handler that takes too long fails the task`(): Unit = {
    val id = newId()
    write(rows(id), SaveMode.Overwrite)
    val (err, took) = {
      val start = System.nanoTime()
      val e = assertThrows(
        classOf[Exception],
        () =>
          write(
            rows(id),
            SaveMode.ErrorIfExists,
            KeyValueOptions.ErrorHandler        -> classOf[SlowFailureHandler].getName,
            KeyValueOptions.ErrorHandlerTimeout -> "1s"
          )
      )
      (e, (System.nanoTime() - start).nanos)
    }
    assertTrue(mentions(err, "did not return from onFailure"), err.toString)
    assertTrue(took < 20.seconds, took.toString)
  }

  /** Writes a new row with these ErrorBucket options, which should fail before anything is
    * written, rather than each failure retrying its error document until it times out.
    */
  private def errorBucketRejected(options: (String, String)*): Throwable =
    errorBucketRejectedWithin(30.seconds, options: _*)

  private def errorBucketRejectedWithin(
      within: FiniteDuration,
      options: (String, String)*
  ): Throwable = {
    val id    = newId()
    val start = System.nanoTime()
    val err = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        write(
          rows(id),
          SaveMode.Overwrite,
          (KeyValueOptions.ErrorHandlerTimeout -> "10m") +: options: _*
        )
    )
    val took = (System.nanoTime() - start).nanos
    assertTrue(took < within, took.toString)
    assertFalse(exists(id))
    err
  }

  @Test
  def `a missing ErrorBucket collection fails the write before it starts`(): Unit = {
    val missing = s"missing-${newId().take(8)}"
    val err = errorBucketRejected(
      KeyValueOptions.ErrorBucket     -> testResources.bucketName,
      KeyValueOptions.ErrorScope      -> testResources.scopeName,
      KeyValueOptions.ErrorCollection -> missing
    )
    assertTrue(err.getMessage.contains(s"collection '$missing'"), err.getMessage)
    assertTrue(err.getMessage.contains("the scope or collection does not exist"), err.getMessage)
  }

  @Test
  def `a missing ErrorBucket scope fails the write before it starts`(): Unit = {
    val err = errorBucketRejected(
      KeyValueOptions.ErrorBucket -> testResources.bucketName,
      KeyValueOptions.ErrorScope  -> s"missing-${newId().take(8)}"
    )
    assertTrue(err.getMessage.contains("the scope or collection does not exist"), err.getMessage)
  }

  @Test
  def `a missing ErrorBucket bucket fails the write before it starts`(): Unit = {
    // Only after waiting for it to open, for spark.couchbase.waitUntilReadyTimeout (1 minute).
    val err = errorBucketRejectedWithin(
      90.seconds,
      KeyValueOptions.ErrorBucket -> s"missing-${newId().take(8)}"
    )
    assertTrue(err.getMessage.contains("the bucket could not be opened within"), err.getMessage)
    assertTrue(err.getMessage.contains("check that it exists"), err.getMessage)
  }

  @Test
  def `invalid handler classes are rejected before writing, with the reason`(): Unit = {
    val id = newId()
    val err = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        write(
          rows(id),
          SaveMode.Overwrite,
          KeyValueOptions.ErrorHandler -> classOf[ThrowingConstructorHandler].getName
        )
    )
    assertTrue(err.getMessage.contains("constructor failed"), err.getMessage)
    assertTrue(err.getMessage.contains("needs configuring"), err.getMessage)
    assertFalse(exists(id))
  }
}

class SlowFailureHandler extends KeyValueWriteFailureHandler {
  override def onFailure(failure: KeyValueWriteFailure): KeyValueWriteFailureDecision = {
    Thread.sleep(60000)
    KeyValueWriteFailureDecision.Continue
  }
}

class ThrowingConstructorHandler extends KeyValueWriteFailureHandler {
  throw new IllegalStateException("needs configuring")
  override def onFailure(failure: KeyValueWriteFailure): KeyValueWriteFailureDecision =
    KeyValueWriteFailureDecision.Continue
}
