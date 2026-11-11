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

import com.couchbase.client.core.error.{AmbiguousTimeoutException, DocumentExistsException}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

class FailureHandlerRunnerTest {
  import KeyValueWriteFailureDecision._

  private def failure(id: String, errors: Throwable*): WriteFailure =
    WriteFailure(
      id,
      "b",
      "s",
      "c",
      if (errors.isEmpty) Seq(new DocumentExistsException(null)) else errors,
      "{}"
    )

  private class Handler(decide: KeyValueWriteFailure => KeyValueWriteFailureDecision)
      extends KeyValueWriteFailureHandler {
    val threads = new ConcurrentLinkedQueue[String]()
    val closed  = new ConcurrentLinkedQueue[Option[Throwable]]()
    override def onFailure(f: KeyValueWriteFailure): KeyValueWriteFailureDecision = {
      threads.add(Thread.currentThread().getName)
      decide(f)
    }
    override def close(error: Option[Throwable]): Unit = {
      threads.add(Thread.currentThread().getName)
      closed.add(error)
    }
  }

  private def runner(
      handler: KeyValueWriteFailureHandler,
      timeout: FiniteDuration = 1.minute,
      partitionFailed: AtomicBoolean = new AtomicBoolean(false),
      stats: PartitionWriteStats = new PartitionWriteStats("test", "unused"),
      errorBucket: Option[KeyValueErrorDocumentHandler] = None
  ) = new FailureHandlerRunner(
    handler,
    "test",
    "handler-thread",
    partitionFailed,
    stats,
    timeout,
    errorBucket
  )

  private def elapsed[T](f: => T): (T, FiniteDuration) = {
    val start  = System.nanoTime()
    val result = f
    (result, (System.nanoTime() - start).nanos)
  }

  /** Runs `f` on `threads` threads at once, and returns what each returned or threw. */
  private def concurrently[T](threads: Int)(f: Int => T): Seq[Try[T]] = {
    val pool  = Executors.newFixedThreadPool(threads)
    val start = new CountDownLatch(1)
    try {
      val futures = (0 until threads).map(i => pool.submit(() => { start.await(); Try(f(i)) }))
      start.countDown()
      futures.map(_.get(30, TimeUnit.SECONDS))
    } finally pool.shutdownNow()
  }

  @Test
  def `calls from many writers are made one at a time, on the handler's thread`(): Unit = {
    val inCall     = new AtomicInteger()
    val maxInCall  = new AtomicInteger()
    val loaders    = new ConcurrentLinkedQueue[ClassLoader]()
    val handler = new Handler(_ => {
      maxInCall.accumulateAndGet(inCall.incrementAndGet(), math.max)
      loaders.add(Thread.currentThread().getContextClassLoader)
      Thread.sleep(1)
      inCall.decrementAndGet()
      Continue
    })
    val stats = new PartitionWriteStats("test", "unused", hasErrorHandler = true)
    val r     = runner(handler, stats = stats)
    val results = concurrently(16) { i =>
      (1 to 10).map(j => r.onFailure(failure(s"doc-$i-$j")))
    }
    r.close(None)

    assertTrue(results.forall(_.get.forall(_ == Continue)))
    assertEquals(1, maxInCall.get())
    assertEquals(161, handler.threads.size)
    assertEquals(Set("handler-thread"), handler.threads.asScala.toSet)
    // As on the thread that created the runner, the task's.
    assertEquals(Set(Thread.currentThread().getContextClassLoader), loaders.asScala.toSet)
    val report = stats.report(isFinal = true)
    assertTrue(report.contains("error handler calls=161 "), report)
    assertFalse(report.contains("decided"), report)

    stats.recordRetryDecision(KeyValueWriteRetryDecision.Retry)
    stats.recordRetryDecision(KeyValueWriteRetryDecision.Default)
    stats.recordRetryDecision(KeyValueWriteRetryDecision.GiveUp)
    stats.recordRetryDecision(KeyValueWriteRetryDecision.GiveUp)
    val next = stats.report(isFinal = true)
    assertTrue(next.contains("decided retry=1 giveUp=2"), next)
  }

  @Test
  def `a slow handler with many writers waiting does not time out, as each call is quick`()
      : Unit = {
    val r = runner(new Handler(_ => { Thread.sleep(100); Continue }), timeout = 300.millis)
    // Over a second for the last writer, waiting its turn, but no call takes over 300ms.
    val (results, took) = elapsed(concurrently(12)(i => r.onFailure(failure(s"doc$i"))))
    assertTrue(took > 1.second, took.toString)
    assertTrue(results.forall(_.isSuccess), results.toString)
    assertEquals(None, r.failed)
  }

  @Test
  def `a call that does not return fails the partition, frees every writer, and close still runs`()
      : Unit = {
    val release = new CountDownLatch(1)
    val handler = new Handler(_ => {
      // Ignores interrupts, as a badly-behaved handler might.
      while (release.getCount > 0) {
        try release.await()
        catch { case _: InterruptedException => }
      }
      Continue
    })
    val partitionFailed = new AtomicBoolean(false)
    val r               = runner(handler, timeout = 300.millis, partitionFailed = partitionFailed)
    try {
      val (results, took) = elapsed(concurrently(4)(i => r.onFailure(failure(s"doc$i"))))
      assertTrue(took < 3.seconds, took.toString)
      assertTrue(results.forall(_.isFailure), results.toString)
      results.foreach(t => assertTrue(t.failed.get.getMessage.contains("did not return")))
      assertTrue(partitionFailed.get())
      // Only the first got as far as the handler.
      assertEquals(1, handler.threads.size)

      val error = r.failed
      assertTrue(error.get.getMessage.contains("onFailure for document 'doc"), error.toString)
      r.close(error)
      assertEquals(Seq(error), handler.closed.asScala.toSeq)
      assertEquals("handler-thread-close", handler.threads.asScala.last)
    } finally release.countDown()
  }

  @Test
  def `close is waited for even when the task has been killed`(): Unit = {
    val finished = new AtomicBoolean(false)
    val r = runner(new KeyValueWriteFailureHandler {
      override def onFailure(f: KeyValueWriteFailure) = Continue
      override def close(error: Option[Throwable]): Unit = {
        Thread.sleep(200)
        finished.set(true)
      }
    })
    // As writeAll leaves it, once Spark has killed the task.
    Thread.currentThread().interrupt()
    r.close(None)
    assertTrue(finished.get())
    assertTrue(Thread.interrupted(), "the interrupt is kept")
  }

  @Test
  def `close can time out too`(): Unit = {
    val release = new CountDownLatch(1)
    val r = runner(
      new KeyValueWriteFailureHandler {
        override def onFailure(f: KeyValueWriteFailure) = Continue
        override def close(error: Option[Throwable]): Unit = release.await()
      },
      timeout = 300.millis
    )
    val (err, took) = elapsed(assertThrows(classOf[RuntimeException], () => r.close(None)))
    assertTrue(took < 3.seconds, took.toString)
    assertTrue(err.getMessage.contains("did not return from close"), err.getMessage)
    release.countDown()
  }

  @Test
  def `FailTask fails the partition, and failures already waiting are still handled`(): Unit = {
    val partitionFailed = new AtomicBoolean(false)
    val handler = new Handler(f => {
      Thread.sleep(50)
      if (f.documentId == "doc0") FailTask else Continue
    })
    val r = runner(handler, partitionFailed = partitionFailed)
    // doc0 first, so that the rest are waiting behind it.
    val first = new Thread(() => Try(r.onFailure(failure("doc0"))))
    first.start()
    Thread.sleep(10)
    val results = concurrently(4)(i => r.onFailure(failure(s"doc${i + 1}")))
    first.join()

    // Failed, which stops new rows and retries, but every failure was handled.
    assertTrue(partitionFailed.get())
    assertTrue(r.failed.get.isInstanceOf[DocumentExistsException])
    assertTrue(results.forall(_.get == Continue), results.toString)
    assertEquals(5, handler.threads.size)
    // As are those of rows that were still being written.
    assertEquals(Continue, r.onFailure(failure("later")))
    r.close(r.failed)
    assertEquals(Seq(r.failed), handler.closed.asScala.toSeq)
  }

  @Test
  def `a partition failed elsewhere still has its failures handled`(): Unit = {
    val partitionFailed = new AtomicBoolean(true)
    val handler         = new Handler(_ => Continue)
    val r               = runner(handler, partitionFailed = partitionFailed)
    assertEquals(Continue, r.onFailure(failure("d")))
    r.close(None)
    assertEquals(1, handler.closed.size)
  }

  @Test
  def `a handler that throws fails the partition, saying where, and is not called again`()
      : Unit = {
    val handler = new Handler(_ => throw new IllegalStateException("broken"))
    val r       = runner(handler)
    val err     = assertThrows(classOf[RuntimeException], () => r.onFailure(failure("doc1")))
    assertEquals(Some(err), r.failed)
    assertTrue(err.getMessage.contains("onFailure for document 'doc1'"), err.getMessage)
    assertTrue(err.getCause.isInstanceOf[IllegalStateException])
    assertTrue(err.getSuppressed.head.isInstanceOf[DocumentExistsException])

    val skipped = assertThrows(classOf[PartitionStoppedException], () => r.onFailure(failure("d2")))
    assertEquals(err, skipped.getCause)
    assertTrue(skipped.getMessage.contains("Not passed"), skipped.getMessage)
    assertEquals(1, handler.threads.size)
    // Other than to close.
    r.close(Some(err))
    assertEquals(Seq(Some(err)), handler.closed.asScala.toSeq)
  }

  @Test
  def `a handler returning null fails the partition`(): Unit = {
    val r   = runner(new Handler(_ => null))
    val err = assertThrows(classOf[IllegalStateException], () => r.onFailure(failure("doc1")))
    assertTrue(err.getMessage.contains("null"), err.getMessage)
    assertTrue(r.failed.isDefined)
  }

  @Test
  def `retry decisions: FailTask fails the partition, the others don't`(): Unit = {
    val partitionFailed = new AtomicBoolean(false)
    val r = runner(
      new KeyValueWriteFailureHandler {
        override def onFailure(f: KeyValueWriteFailure) = Continue
        override def onAttemptFailed(a: KeyValueWriteFailedAttempt) = a.documentId match {
          case "retry" => KeyValueWriteRetryDecision.Retry
          case "fail"  => KeyValueWriteRetryDecision.FailTask
          case _       => KeyValueWriteRetryDecision.Default
        }
      },
      partitionFailed = partitionFailed
    )
    assertEquals(KeyValueWriteRetryDecision.Retry, r.attemptFailed(failure("retry")))
    assertEquals(KeyValueWriteRetryDecision.Default, r.attemptFailed(failure("other")))
    assertFalse(partitionFailed.get())
    assertEquals(KeyValueWriteRetryDecision.FailTask, r.attemptFailed(failure("fail")))
    assertTrue(partitionFailed.get())
  }

  @Test
  def `an exception from open is the task's error, and close is still called`(): Unit = {
    val closed = new ConcurrentLinkedQueue[Option[Throwable]]()
    val r = runner(new KeyValueWriteFailureHandler {
      override def open(context: KeyValueWriteContext): Unit =
        throw new IllegalArgumentException("not configured")
      override def onFailure(f: KeyValueWriteFailure) = Continue
      override def close(error: Option[Throwable]): Unit = closed.add(error)
    })
    val err = assertThrows(
      classOf[IllegalArgumentException],
      () => r.open(WriteContext(0, 0, 0, 0, "b", "s", "c", None, Map.empty))
    )
    r.close(Some(err))
    assertEquals(Seq(Some(err)), closed.asScala.toSeq)
  }

  @Test
  def `a failure may have been applied if any attempt was ambiguous`(): Unit = {
    val ambiguous = new AmbiguousTimeoutException("ambiguous", null)
    val exists    = new DocumentExistsException(null)
    assertTrue(failure("d", ambiguous, exists).mayHaveBeenApplied)
    assertFalse(failure("d", exists).mayHaveBeenApplied)
    assertEquals(exists, failure("d", ambiguous, exists).error)
    assertEquals(1, failure("d", ambiguous, exists).attempt)
  }

  @Test
  def `handler options are case-insensitive and only those with the prefix`(): Unit = {
    val options = WriteContext.handlerOptions(
      Seq(
        "errorHandler.Directory" -> "s3a://x",
        "errorhandler"           -> "cls",
        "errorHandlerTimeout"    -> "5m",
        "bucket"                 -> "b"
      )
    )
    assertEquals(Map("directory" -> "s3a://x"), options)
    val ctx = WriteContext(0, 0, 0, 0, "b", "s", "c", None, options)
    assertEquals(Some("s3a://x"), ctx.option("DIRECTORY"))
    assertEquals(None, ctx.option("bucket"))
  }

  @Test
  def `handler option values are not logged`(): Unit = {
    val config = KeyValueWriteConfig(
      "b",
      None,
      None,
      "__META_ID",
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      1,
      errorHandlerOptions = Map("secret" -> "hunter2")
    )
    assertFalse(config.toString.contains("hunter2"), config.toString)
    assertTrue(config.toString.contains("secret"), config.toString)
  }


  /** An ErrorBucket that records the documents it's asked to write, after `body`. */
  private class FakeErrorBucket(body: (String, Throwable => Unit) => Unit = (_, _) => ())
      extends KeyValueErrorDocumentHandler(null, null) {
    val written = new ConcurrentLinkedQueue[String]()
    override private[kv] def handleError(
        documentId: String,
        bucket: String,
        scope: String,
        collection: String,
        error: Throwable,
        onRetry: Throwable => Unit
    ): Unit = {
      body(documentId, onRetry)
      written.add(documentId)
    }
  }

  @Test
  def `with ErrorBucket, only failures the handler continues with are written`(): Unit = {
    val errorBucket = new FakeErrorBucket()
    val user = new Handler(f =>
      f.documentId match {
        case "ignored" => Ignore
        case "fatal"   => FailTask
        case _         => Continue
      }
    )
    val r = runner(user, errorBucket = Some(errorBucket))
    Seq("ignored", "fatal", "continued").foreach(id => r.onFailure(failure(id)))
    assertEquals(Seq("continued"), errorBucket.written.asScala.toSeq)

    // With no ErrorHandler, every failure is written.
    val alone = new FakeErrorBucket()
    assertEquals(
      Continue,
      runner(FailureHandlerRunner.ContinueHandler, errorBucket = Some(alone))
        .onFailure(failure("x"))
    )
    assertEquals(Seq("x"), alone.written.asScala.toSeq)
  }

  @Test
  def `the handler and the error document each have the whole timeout`(): Unit = {
    val stats = new PartitionWriteStats("test", "unused", hasErrorHandler = true)
    val errorBucket = new FakeErrorBucket((_, onRetry) => {
      Thread.sleep(400)
      onRetry(new RuntimeException("retried"))
      onRetry(new RuntimeException("retried"))
    })
    val slowUser = new Handler(_ => { Thread.sleep(400); Continue })
    val r =
      runner(slowUser, timeout = 600.millis, stats = stats, errorBucket = Some(errorBucket))
    // 800ms in all, but neither part takes 600ms.
    assertEquals(Continue, r.onFailure(failure("x")))
    assertEquals(Seq("x"), errorBucket.written.asScala.toSeq)
    val report = stats.report(isFinal = true)
    assertTrue(report.contains("error document retries=2 (total 2)"), report)
    assertFalse(
      new PartitionWriteStats("test", "unused", hasErrorHandler = true)
        .report(isFinal = true)
        .contains("error document")
    )
  }

  @Test
  def `an error document that takes too long fails the partition, saying so`(): Unit = {
    val partitionFailed = new AtomicBoolean(false)
    val stuck           = new FakeErrorBucket((_, _) => Thread.sleep(60000))
    val r = runner(
      new Handler(_ => Continue),
      timeout = 300.millis,
      partitionFailed = partitionFailed,
      errorBucket = Some(stuck)
    )
    val err = assertThrows(classOf[RuntimeException], () => r.onFailure(failure("x")))
    assertTrue(
      err.getMessage.startsWith(
        "KeyValueOptions.ErrorBucket did not return from writing the error document for " +
          "document 'x' within 300 milliseconds"
      ),
      err.getMessage
    )
    assertTrue(partitionFailed.get())
  }

  @Test
  def `an error document that can't be written fails the partition as it is`(): Unit = {
    val notWritten = new ErrorDocumentNotWrittenException("Could not write", null)
    val failing    = new FakeErrorBucket((_, _) => throw notWritten)
    val user       = new Handler(_ => Continue)
    val r          = runner(user, errorBucket = Some(failing))
    // Not wrapped as an error from the ErrorHandler, which did nothing wrong.
    val err =
      assertThrows(classOf[ErrorDocumentNotWrittenException], () => r.onFailure(failure("x")))
    assert(err eq notWritten)
    assertEquals(1, err.getSuppressed.length)
    assertEquals(Some(notWritten), r.failed)
    // Nothing more is written, but the ErrorHandler is still closed.
    assertThrows(classOf[PartitionStoppedException], () => r.onFailure(failure("y")))
    r.close(r.failed)
    assertEquals(Seq(r.failed), user.closed.asScala.toSeq)
  }

  @Test
  def `only a V2 handler is asked about retries`(): Unit = {
    val v1 = new LegacyErrorHandlerAdapter(new KeyValueWriteErrorHandler {
      override def onError(info: KeyValueWriteErrorInfo): Unit = ()
    })
    assertTrue(FailureHandlerRunner.decidesRetries(Some(new Handler(_ => Continue))))
    assertFalse(FailureHandlerRunner.decidesRetries(Some(v1)))
    assertFalse(FailureHandlerRunner.decidesRetries(None))
  }
}
