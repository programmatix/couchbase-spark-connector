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

import com.couchbase.client.core.error.context.SubDocumentErrorContext
import com.couchbase.client.core.error.subdoc.PathExistsException
import com.couchbase.client.core.error.{
  AmbiguousTimeoutException,
  DocumentExistsException,
  DocumentNotFoundException,
  TemporaryFailureException,
  UnambiguousTimeoutException
}
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus
import com.couchbase.client.scala.json.JsonObject
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class KeyValueWriteRetryTest {
  private def ambiguous    = new AmbiguousTimeoutException("ambiguous", null)
  private def notApplied   = new UnambiguousTimeoutException("unambiguous", null)
  private def notRetryable = new DocumentNotFoundException(null)

  private def pathExists(index: Int) = new PathExistsException(
    new SubDocumentErrorContext(
      null,
      index,
      "p",
      SubDocumentOpResponseStatus.PATH_EXISTS,
      null,
      null
    )
  )

  // --- IdempotentWrites

  @Test
  def `row key is stable, compact and safe as a path component`(): Unit = {
    val row = """{"__META_ID":"doc1","arrayappend:items":"a"}"""
    val key = IdempotentWrites.rowKey(row)
    assertEquals(key, IdempotentWrites.rowKey(row))
    assertEquals(22, key.length)
    assertTrue(key.matches("[A-Za-z0-9_-]+"), key)
  }

  @Test
  def `row key differs for different content or document`(): Unit = {
    val a = IdempotentWrites.rowKey("""{"__META_ID":"doc1","arrayappend:items":"a"}""")
    val b = IdempotentWrites.rowKey("""{"__META_ID":"doc1","arrayappend:items":"b"}""")
    val c = IdempotentWrites.rowKey("""{"__META_ID":"doc2","arrayappend:items":"a"}""")
    assertEquals(3, Set(a, b, c).size)
  }

  @Test
  def `write ids are validated`(): Unit = {
    assertEquals("run-2026_11_06", IdempotentWrites.validateWriteId("run-2026_11_06"))
    IdempotentWrites.validateWriteId(IdempotentWrites.generateWriteId())
    Seq("", "has.dot", "has space", "back`tick", "x" * 65).foreach { bad =>
      assertThrows(classOf[IllegalArgumentException], () => IdempotentWrites.validateWriteId(bad))
    }
  }

  @Test
  def `marker specs`(): Unit = {
    assertEquals(2, IdempotentWrites.markerSpecs("w", "k", 1L).size)
    assertEquals(2, IdempotentWrites.specsUsed(Some(KeyValueOptions.IdempotencyEnabled)))
    assertEquals(0, IdempotentWrites.specsUsed(None))
    assertEquals("cbspark.w.k", IdempotentWrites.markerPath("w", "k"))
  }

  @Test
  def `only a path-exists failure on the first spec is the guard`(): Unit = {
    assertTrue(IdempotentWrites.isGuardRejection(pathExists(0)))
    assertFalse(IdempotentWrites.isGuardRejection(pathExists(2)))
    assertFalse(IdempotentWrites.isGuardRejection(new DocumentExistsException(null)))
    assertFalse(IdempotentWrites.isGuardRejection(ambiguous))
  }

  @Test
  def `same JSON ignores field order but nothing else`(): Unit = {
    def same(stored: String, written: String) =
      IdempotentWrites.sameJson(stored.getBytes("UTF-8"), written)
    assertTrue(same("""{"a":1,"b":{"c":[1,2],"d":"x"}}""", """{"b":{"d":"x","c":[1,2]},"a":1}"""))
    assertFalse(same("""{"a":1}""", """{"a":2}"""))
    assertFalse(same("""{"a":1}""", """{"a":"1"}"""))
    assertFalse(same("""{"a":[1,2]}""", """{"a":[2,1]}"""))
    assertFalse(same("""{"a":1}""", """{"a":1,"b":null}"""))
  }

  @Test
  def `a conflict is deduplicated if already applied, else reported`(): Unit = {
    val conflict = new DocumentExistsException(null)
    assertEquals(
      Success(WriteRetry.Deduplicated),
      IdempotentWrites.resolveConflict(conflict, Success(true))
    )
    assertEquals(Failure(conflict), IdempotentWrites.resolveConflict(conflict, Success(false)))
  }

  @Test
  def `a retryable read failure is the result, so the write is retried and checked again`(): Unit = {
    val conflict = new DocumentExistsException(null)
    val readErr  = notApplied
    val result   = IdempotentWrites.resolveConflict(conflict, Failure(readErr))
    assertEquals(Failure(readErr), result)
    assertEquals(WriteRetry.NotApplied, WriteRetry.classify(result.failed.get))
    assertEquals(Seq(conflict), readErr.getSuppressed.toSeq)
  }

  @Test
  def `a non-retryable read failure reports the conflict`(): Unit = {
    val conflict = new DocumentExistsException(null)
    val readErr  = notRetryable
    assertEquals(Failure(conflict), IdempotentWrites.resolveConflict(conflict, Failure(readErr)))
    assertEquals(Seq(readErr), conflict.getSuppressed.toSeq)
  }

  // --- WriteRetry.classify and backoff

  @Test
  def `classifies failures`(): Unit = {
    assertEquals(WriteRetry.Ambiguous, WriteRetry.classify(ambiguous))
    assertEquals(WriteRetry.NotApplied, WriteRetry.classify(notApplied))
    assertEquals(WriteRetry.NotApplied, WriteRetry.classify(new TemporaryFailureException(null)))
    assertEquals(WriteRetry.NotRetryable, WriteRetry.classify(notRetryable))
    assertEquals(WriteRetry.NotRetryable, WriteRetry.classify(new RuntimeException("x")))
  }

  @Test
  def `backoff is exponential, capped and half randomised`(): Unit = {
    assertEquals(50.millis, WriteRetry.backoff(1, 0.0))
    assertEquals(99.millis, WriteRetry.backoff(1, 0.999))
    assertEquals(100.millis, WriteRetry.backoff(2, 0.0))
    assertEquals(200.millis, WriteRetry.backoff(3, 0.0))
    assertEquals(2500.millis, WriteRetry.backoff(10, 0.0))
    assertEquals(2500.millis, WriteRetry.backoff(1000, 0.0))
    assertTrue(WriteRetry.backoff(1000, 0.999) < WriteRetry.MaxDelay)
  }

  // --- WriteRetry.run

  /** Runs with attempts returning `results` in turn, and returns the outcome, the attempts made and
    * the retries reported.
    */
  private def runWith(
      maxRetries: Int,
      replaySafe: Boolean,
      results: Try[WriteRetry.Outcome]*
  ): (Try[WriteRetry.Outcome], Seq[Int], Seq[Int]) = {
    val attempts = mutable.ArrayBuffer.empty[Int]
    val retries  = mutable.ArrayBuffer.empty[Int]
    val outcome = WriteRetry.run(
      maxRetries,
      replaySafe,
      sleep = _ => (),
      random = () => 0.5,
      onRetry = (n, _) => retries += n
    ) { n =>
      attempts += n
      results(n)
    }
    (outcome, attempts.toSeq, retries.toSeq)
  }

  @Test
  def `a write whose conflict check timed out is retried and then deduplicated`(): Unit = {
    // The write times out ambiguously, the retry conflicts and the check times out, then the next
    // retry conflicts and the check finds the row already applied.
    val checks = Iterator(Failure(notApplied), Success(true))
    val (outcome, attempts, _) = runWith(
      3,
      replaySafe = true,
      Failure(ambiguous),
      IdempotentWrites.resolveConflict(new DocumentExistsException(null), checks.next()),
      IdempotentWrites.resolveConflict(new DocumentExistsException(null), checks.next())
    )
    assertEquals(Success(WriteRetry.Deduplicated), outcome)
    assertEquals(Seq(0, 1, 2), attempts)
  }

  @Test
  def `no retry on success`(): Unit = {
    val (outcome, attempts, retries) = runWith(3, replaySafe = false, Success(WriteRetry.Applied))
    assertEquals(Success(WriteRetry.Applied), outcome)
    assertEquals(Seq(0), attempts)
    assertEquals(Seq(), retries)
  }

  @Test
  def `not-applied failures are retried until success`(): Unit = {
    val (outcome, attempts, retries) =
      runWith(
        3,
        replaySafe = false,
        Failure(notApplied),
        Failure(notApplied),
        Success(WriteRetry.Applied)
      )
    assertEquals(Success(WriteRetry.Applied), outcome)
    assertEquals(Seq(0, 1, 2), attempts)
    assertEquals(Seq(1, 2), retries)
  }

  @Test
  def `gives up after the maximum retries, returning the last failure`(): Unit = {
    val last = notApplied
    val (outcome, attempts, _) =
      runWith(2, replaySafe = true, Failure(notApplied), Failure(notApplied), Failure(last))
    assertEquals(Failure(last), outcome)
    assertEquals(Seq(0, 1, 2), attempts)
  }

  @Test
  def `zero retries is the old behaviour`(): Unit = {
    val (outcome, attempts, _) = runWith(0, replaySafe = true, Failure(notApplied))
    assertTrue(outcome.isFailure)
    assertEquals(Seq(0), attempts)
  }

  @Test
  def `ambiguous failures are only retried when replay is safe`(): Unit = {
    val (unsafe, unsafeAttempts, _) =
      runWith(3, replaySafe = false, Failure(ambiguous), Success(WriteRetry.Applied))
    assertTrue(unsafe.isFailure)
    assertEquals(Seq(0), unsafeAttempts)

    val (safe, safeAttempts, _) =
      runWith(3, replaySafe = true, Failure(ambiguous), Success(WriteRetry.Deduplicated))
    assertEquals(Success(WriteRetry.Deduplicated), safe)
    assertEquals(Seq(0, 1), safeAttempts)
  }

  @Test
  def `non-retryable failures are not retried`(): Unit = {
    val (outcome, attempts, _) =
      runWith(3, replaySafe = true, Failure(notRetryable), Success(WriteRetry.Applied))
    assertTrue(outcome.isFailure)
    assertEquals(Seq(0), attempts)
  }

  @Test
  def `waits with backoff between retries`(): Unit = {
    val waits = mutable.ArrayBuffer.empty[FiniteDuration]
    WriteRetry.run(3, replaySafe = false, sleep = d => waits += d, random = () => 0.0) { _ =>
      Failure(notApplied)
    }
    assertEquals(Seq(50.millis, 100.millis, 200.millis), waits.toSeq)
  }

  @Test
  def `no retries once stopped`(): Unit = {
    var attempts = 0
    val outcome = WriteRetry.run(3, replaySafe = true, sleep = _ => (), stopped = () => true) { _ =>
      attempts += 1
      Failure(notApplied)
    }
    assertTrue(outcome.isFailure)
    assertEquals(1, attempts)
  }

  @Test
  def `stopping during the backoff wait prevents the retry`(): Unit = {
    var stopped  = false
    var attempts = 0
    val outcome = WriteRetry.run(
      3,
      replaySafe = true,
      sleep = _ => stopped = true, // e.g. another row failed the partition while this one waited
      stopped = () => stopped
    ) { _ =>
      attempts += 1
      Failure(notApplied)
    }
    assertTrue(outcome.isFailure)
    assertEquals(1, attempts)
  }

  @Test
  def `writeAll flags the partition as failed so in-progress rows stop retrying`(): Unit = {
    val failed = new java.util.concurrent.atomic.AtomicBoolean(false)
    val stats  = new PartitionWriteStats("test", "write-retry-test-stats")
    // So that the slow row is already being processed when the other fails the partition.
    val slowStarted         = new java.util.concurrent.CountDownLatch(1)
    @volatile var sawFailed = false
    assertThrows(
      classOf[RuntimeException],
      () =>
        RelationPartitionWriter.writeAll(Iterator("fail", "slow"), 2, i => s"t$i", stats, failed) {
          case "fail" =>
            slowStarted.await(5, java.util.concurrent.TimeUnit.SECONDS)
            throw new RuntimeException("boom")
          case _ =>
            // Like a row in the middle of its retries: polls the flag the retry loop checks.
            slowStarted.countDown()
            val deadline = System.nanoTime() + 5.seconds.toNanos
            while (!failed.get() && System.nanoTime() < deadline) Thread.sleep(1)
            sawFailed = failed.get()
        }
    )
    assertTrue(failed.get())
    assertTrue(sawFailed)
  }

  // --- WriteRetry.run with decide, as from KeyValueWriteFailureHandler.onAttemptFailed

  /** Like runWith, deciding with `decision`, and also returning what decide was given. */
  private def runDeciding(
      maxRetries: Int,
      decision: (Int, Throwable, Boolean) => Boolean,
      results: Try[WriteRetry.Outcome]*
  ): (Try[WriteRetry.Outcome], Seq[Int], Seq[(Int, Boolean)]) = {
    val attempts = mutable.ArrayBuffer.empty[Int]
    val asked    = mutable.ArrayBuffer.empty[(Int, Boolean)]
    val outcome = WriteRetry.run(
      maxRetries,
      replaySafe = false,
      sleep = _ => (),
      decide = (n, err, byDefault) => { asked += (n -> byDefault); decision(n, err, byDefault) }
    ) { n =>
      attempts += n
      results(n)
    }
    (outcome, attempts.toSeq, asked.toSeq)
  }

  @Test
  def `decide can retry what would not be retried`(): Unit = {
    // Neither is retried by default: replay isn't safe for the ambiguous one.
    val (outcome, attempts, asked) = runDeciding(
      3,
      (_, _, _) => true,
      Failure(notRetryable),
      Failure(ambiguous),
      Success(WriteRetry.Applied)
    )
    assertEquals(Success(WriteRetry.Applied), outcome)
    assertEquals(Seq(0, 1, 2), attempts)
    assertEquals(Seq(1 -> false, 2 -> false), asked)
  }

  @Test
  def `decide can give up on what would be retried`(): Unit = {
    val (outcome, attempts, asked) =
      runDeciding(3, (_, _, _) => false, Failure(notApplied), Success(WriteRetry.Applied))
    assertTrue(outcome.isFailure)
    assertEquals(Seq(0), attempts)
    assertEquals(Seq(1 -> true), asked)
  }

  @Test
  def `decide is only asked while a retry is allowed`(): Unit = {
    val (outcome, attempts, asked) =
      runDeciding(1, (_, _, _) => true, Failure(notApplied), Failure(notApplied))
    assertTrue(outcome.isFailure)
    assertEquals(Seq(0, 1), attempts)
    assertEquals(Seq(1 -> true), asked)

    val (_, _, none) = runDeciding(0, (_, _, _) => true, Failure(notApplied))
    assertEquals(Seq(), none)
  }

  @Test
  def `decide throwing fails the row`(): Unit = {
    val err = new RuntimeException("fail the task")
    val thrown = assertThrows(
      classOf[RuntimeException],
      () => runDeciding(3, (_, _, _) => throw err, Failure(notApplied))
    )
    assertEquals(err, thrown)
  }

  // --- PartitionBackoff

  @Test
  def `timeouts and overload are cluster strain, document problems are not`(): Unit = {
    assertTrue(WriteRetry.isPossiblyClusterStrain(ambiguous))
    assertTrue(WriteRetry.isPossiblyClusterStrain(notApplied))
    assertTrue(WriteRetry.isPossiblyClusterStrain(new TemporaryFailureException(null)))
    assertFalse(WriteRetry.isPossiblyClusterStrain(notRetryable))
    assertFalse(WriteRetry.isPossiblyClusterStrain(new DocumentExistsException(null)))
    assertFalse(WriteRetry.isPossiblyClusterStrain(new RuntimeException("x")))
    // A timeout from retrying only for a locked document is checked against a real cluster, in
    // WriteFailureHandlerIntegrationTest.
  }

  private class Backoff(random: Double = 0.0, stopAfter: Int = Int.MaxValue) {
    val stats  = new PartitionWriteStats("test", "unused")
    val sleeps = mutable.ArrayBuffer.empty[Long]
    val backoff = new PartitionBackoff(
      stats,
      stopped = () => sleeps.size >= stopAfter,
      sleep = ms => sleeps += ms,
      random = () => random
    )
    def slept: Long = sleeps.sum
  }

  @Test
  def `strain doubles the delay up to the maximum, other failures leave it`(): Unit = {
    val b = new Backoff().backoff
    assertEquals(Duration.Zero, b.delay)
    b.record(Failure(notRetryable))
    assertEquals(Duration.Zero, b.delay)
    b.record(Failure(notApplied))
    assertEquals(PartitionBackoff.BaseDelay, b.delay)
    b.record(Failure(ambiguous))
    assertEquals(PartitionBackoff.BaseDelay * 2, b.delay)
    (1 to 20).foreach(_ => b.record(Failure(ambiguous)))
    assertEquals(PartitionBackoff.MaxDelay, b.delay)
    b.record(Failure(new DocumentExistsException(null)))
    assertEquals(PartitionBackoff.MaxDelay, b.delay)
  }

  @Test
  def `each success reduces the delay by a tenth, until it is dropped`(): Unit = {
    val b = new Backoff().backoff
    (1 to 20).foreach(_ => b.record(Failure(ambiguous)))
    b.record(Success(WriteRetry.Applied))
    assertEquals(4500.millis, b.delay)
    b.record(Success(WriteRetry.Deduplicated))
    assertEquals(4050.millis, b.delay)
    var successes = 2
    while (b.delay > Duration.Zero) {
      b.record(Success(WriteRetry.Applied))
      successes += 1
    }
    // A few dozen successes to recover from the maximum.
    assertTrue(successes > 30 && successes < 60, successes.toString)
  }

  @Test
  def `pause waits for the randomised delay, or longer if asked`(): Unit = {
    val none = new Backoff()
    none.backoff.pause()
    assertEquals(Seq(), none.sleeps.toSeq)

    val b = new Backoff(random = 0.0)
    (1 to 4).foreach(_ => b.backoff.record(Failure(ambiguous))) // 800ms
    b.backoff.pause()
    assertEquals(400L, b.slept) // half of it, as random is 0
    assertTrue(b.sleeps.forall(_ <= 50), b.sleeps.toString) // in slices, to notice stopping
    val report = b.stats.report(isFinal = false)
    assertTrue(report.contains("backoff delay=800ms (max 800ms) paused=0.4s"), report)

    // A retry's own backoff, when it's longer, instead of adding the two.
    b.sleeps.clear()
    b.backoff.pause(atLeast = 2.seconds)
    assertEquals(2000L, b.slept)
    // Not time added by the partition's backoff.
    assertTrue(b.stats.report(isFinal = false).contains("paused=0.0s"))
  }

  @Test
  def `pause stops early once the partition has failed`(): Unit = {
    val b = new Backoff(stopAfter = 2)
    (1 to 20).foreach(_ => b.backoff.record(Failure(ambiguous)))
    b.backoff.pause()
    assertEquals(2, b.sleeps.size)
  }

  // --- SubdocUtil support

  @Test
  def `only array insertions and counters are unsafe to repeat`(): Unit = {
    assertTrue(
      SubdocUtil.isReplaySafe(
        JsonObject(
          "upsert:a"         -> 1,
          "replace:b"        -> 2,
          "c"                -> 3,
          "insert:d"         -> 4,
          "remove:e"         -> 5,
          "arrayaddunique:f" -> 6
        )
      )
    )
    Seq(
      "arrayappend:a",
      "arrayprepend:a",
      "arrayinsert:a[0]",
      "increment:a",
      "decrement:a"
    ).foreach { col =>
      assertFalse(SubdocUtil.isReplaySafe(JsonObject("upsert:x" -> 1, col -> 1)), col)
    }
  }

  @Test
  def `idempotency reserves specs`(): Unit = {
    val schema = StructType(
      (1 to 15).map(i => StructField(s"upsert:path$i", StringType)) :+
        StructField("__META_ID", StringType)
    )
    SubdocUtil.validate(schema, "__META_ID", None, reservedSpecs = 1)
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => SubdocUtil.validate(schema, "__META_ID", None, reservedSpecs = 2)
    )
    assertTrue(ex.getMessage.contains("maximum 14 specs"), ex.getMessage)
    assertTrue(ex.getMessage.contains("Idempotency"), ex.getMessage)
  }
}
