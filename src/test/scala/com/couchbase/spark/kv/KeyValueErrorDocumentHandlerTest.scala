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

import com.couchbase.client.core.error.{
  CouchbaseException,
  DocumentNotFoundException,
  InvalidArgumentException,
  TemporaryFailureException,
  UnambiguousTimeoutException
}
import org.junit.jupiter.api.Assertions.{
  assertEquals,
  assertNotEquals,
  assertSame,
  assertThrows,
  assertTrue
}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class KeyValueErrorDocumentHandlerTest {

  /** A fake clock for [[KeyValueErrorDocumentHandler.writeWithRetry]]: sleeping and each attempt
    * advance it, and it records the timeout each attempt was given.
    */
  private class Clock(attemptTakes: FiniteDuration) {
    var now: Long = 0L
    val timeouts  = ArrayBuffer[FiniteDuration]()
    val sleeps    = ArrayBuffer[FiniteDuration]()
    val retried   = ArrayBuffer[Throwable]()

    def run(deadline: FiniteDuration)(result: Int => Try[Unit]): Unit =
      KeyValueErrorDocumentHandler.writeWithRetry(
        "doc1",
        deadline.toNanos,
        nanoTime = () => now,
        sleep = d => { sleeps += d; now += d.toNanos },
        random = () => 0.0,
        onRetry = e => retried += e
      ) { timeout =>
        timeouts += timeout
        now += attemptTakes.min(timeout).toNanos
        result(timeouts.size)
      }
  }

  private def timeout = new UnambiguousTimeoutException("timed out", null)

  @Test
  def `retries until written`(): Unit = {
    val clock = new Clock(10.millis)
    clock.run(1.minute)(n =>
      if (n < 3) Failure(new TemporaryFailureException(null)) else Success(())
    )
    assertEquals(3, clock.timeouts.size)
    assertEquals(2, clock.retried.size)
    // WriteRetry.backoff, with no randomness.
    assertEquals(Seq(50.millis, 100.millis), clock.sleeps.toSeq)
  }

  @Test
  def `retries until the deadline, then fails with the last error`(): Unit = {
    // As when the cluster is down: every attempt takes its full 2.5s timeout.
    val clock  = new Clock(2500.millis)
    val errors = ArrayBuffer[Throwable]()
    val err = assertThrows(
      classOf[RuntimeException],
      () => clock.run(2.minutes)(_ => { val e = timeout; errors += e; Failure(e) })
    )
    assertSame(errors.last, err.getCause)
    assertTrue(err.getMessage.contains(s"in ${errors.size} attempts"), err.getMessage)
    assertTrue(err.getMessage.contains("KeyValueOptions.ErrorHandlerTimeout"), err.getMessage)
    // It kept trying for nearly all of the time, and not past it.
    assertTrue(clock.now <= 2.minutes.toNanos, clock.now.toString)
    assertTrue(clock.now > 2.minutes.toNanos - 5.seconds.toNanos, clock.now.toString)
    assertTrue(errors.size > 20, errors.size.toString)
    assertEquals(errors.size - 1, clock.retried.size)
  }

  @Test
  def `each attempt is timed out by the time left`(): Unit = {
    val clock = new Clock(1.hour)
    assertThrows(
      classOf[RuntimeException],
      () => clock.run(3.seconds)(_ => Failure(timeout))
    )
    // The attempt is passed all the time left, which then runs out.
    assertEquals(Seq(3.seconds), clock.timeouts.toSeq)
    assertEquals(3.seconds.toNanos, clock.now)
  }

  @Test
  def `doesn't wait if there would be no time to try again`(): Unit = {
    val clock = new Clock(0.millis)
    // The first wait is 50ms.
    assertThrows(classOf[RuntimeException], () => clock.run(40.millis)(_ => Failure(timeout)))
    assertEquals(1, clock.timeouts.size)
    assertTrue(clock.sleeps.isEmpty)
  }

  @Test
  def `only errors from Couchbase are retried`(): Unit = {
    val e     = new RuntimeException("not from Couchbase")
    val clock = new Clock(0.millis)
    val err   = assertThrows(classOf[RuntimeException], () => clock.run(1.minute)(_ => Failure(e)))
    assertSame(e, err.getCause)
    assertTrue(err.getMessage.contains("in 1 attempt over"), err.getMessage)
    assertTrue(err.getMessage.contains("retrying would not help"), err.getMessage)
    assertEquals(1, clock.timeouts.size)
    assertTrue(KeyValueErrorDocumentHandler.isRetryable(timeout))
    val invalid = new InvalidArgumentException("x", null, null)
    assertTrue(KeyValueErrorDocumentHandler.isRetryable(invalid))
  }

  @Test
  def `the check before the write: only a missing document means it can be written to`(): Unit = {
    import KeyValueErrorDocumentHandler._
    assertEquals(Writable, checkResult(Failure(new DocumentNotFoundException(null))))
    assertEquals(Writable, checkResult(Success(())))
    // Such as no permission to write.
    val denied = new CouchbaseException("NO_ACCESS")
    assertEquals(NotWritable(denied.toString, denied), checkResult(Failure(denied)))
    // A timeout that the SDK can't explain (missing collections and buckets are tested against a
    // cluster): the cluster may be struggling, so the write isn't failed for it.
    assertEquals(Unknown(timeout).getClass, checkResult(Failure(timeout)).getClass)
    val other = new RuntimeException("x")
    assertEquals(Unknown(other), checkResult(Failure(other)))
  }

  @Test
  def `retries stop a little before the handler's call would time out`(): Unit = {
    assertEquals(119.seconds, KeyValueErrorDocumentHandler.retryFor(2.minutes))
    assertEquals(900.millis, KeyValueErrorDocumentHandler.retryFor(1.second))
  }
  private def id(bucket: String, scope: String, collection: String, documentId: String, ts: Long = 1000L) =
    KeyValueErrorDocumentHandler.errorDocumentId(bucket, scope, collection, documentId, ts)

  @Test
  def format(): Unit = {
    assertTrue(id("b", "s", "c", "doc1").matches("error_[0-9a-f]{32}_1000"))
  }

  @Test
  def maxLengthDocumentIdStaysWithinKeyLimit(): Unit = {
    // A 250-byte original ID, which would previously have produced an invalid error document key.
    val errorId = id("a-bucket-name", "a-scope", "a-collection", "x" * 250)
    assertTrue(errorId.getBytes(StandardCharsets.UTF_8).length < 250)
  }

  @Test
  def multiByteDocumentId(): Unit = {
    assertTrue(id("b", "s", "c", "ü" * 125).matches("error_[0-9a-f]{32}_1000"))
  }

  @Test
  def sameDocumentSameHash(): Unit = {
    val first  = id("b", "s", "c", "doc1", 1000L)
    val second = id("b", "s", "c", "doc1", 2000L)
    assertEquals(first.stripSuffix("_1000"), second.stripSuffix("_2000"))
    assertNotEquals(first, second)
  }

  @Test
  def locationIsPartOfTheHash(): Unit = {
    assertNotEquals(id("b", "s", "c", "doc1"), id("b", "s", "c2", "doc1"))
    assertNotEquals(id("b", "s", "c", "doc1"), id("b2", "s", "c", "doc1"))
    // Bucket names can contain '.', which the old dotted format could not distinguish.
    assertNotEquals(id("b.s", "c", "d", "doc1"), id("b", "s.c", "d", "doc1"))
  }
}
