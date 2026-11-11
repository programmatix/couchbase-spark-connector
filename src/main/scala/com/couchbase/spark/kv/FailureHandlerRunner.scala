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

import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{
  ExecutionException,
  FutureTask,
  LinkedBlockingQueue,
  RejectedExecutionException,
  ThreadPoolExecutor,
  TimeUnit,
  TimeoutException
}
import scala.concurrent.duration._

/** Calls one partition's [[KeyValueWriteFailureHandler]].
  *
  * Every call is made on the handler's own thread, one at a time, so the handler need not be
  * thread-safe, while the calling thread waits for the result. A writer thread with a failure
  * waiting for the handler is deliberate: error handling then never adds concurrent work, and
  * slows the write rather than adding to the load on a cluster whose writes are failing.
  *
  * Calls are made on another thread so that they can be timed out: if one takes longer than
  * `callTimeout`, the partition fails, waiting threads are released, and the handler's thread is
  * interrupted and abandoned. The timeout is for the call itself, not for a caller's wait, which
  * includes waiting for its turn.
  *
  * Once the partition has failed, for whatever reason, calls are still made: the failures of rows
  * that were already being written are handled, while the writers stop starting new ones. Only a
  * handler that is itself broken - it threw, returned null or did not return - is not called again
  * (other than to close), and those calls fail with [[PartitionStoppedException]] instead. How many
  * is logged.
  *
  * @param handler
  *   the user's handler, or [[FailureHandlerRunner.ContinueHandler]] if there's only an ErrorBucket.
  * @param partitionFailed
  *   set when the handler fails the partition, which stops new rows and retries.
  * @param errorBucket
  *   writes the error document for each failure the handler continues with. This is done on the
  *   handler's thread, as part of the same call, but has a timeout of its own.
  */
private[kv] class FailureHandlerRunner(
    handler: KeyValueWriteFailureHandler,
    partition: String,
    threadName: String,
    partitionFailed: AtomicBoolean,
    stats: PartitionWriteStats,
    callTimeout: FiniteDuration = FailureHandlerRunner.DefaultCallTimeout,
    errorBucket: Option[KeyValueErrorDocumentHandler] = None
) extends Logging {
  import FailureHandlerRunner._

  // So that the handler can load classes as it would on the task's thread.
  private val classLoader = Thread.currentThread().getContextClassLoader
  private val failure     = new AtomicReference[Throwable]()

  /** @param who what is being waited for, for the error if it times out. */
  private case class Running(
      what: String,
      startedNanos: Long,
      who: String = "KeyValueOptions.ErrorHandler"
  )

  /** A thread for the handler, and the call it is making. Replaced if a call times out, so that
    * close can still be called.
    */
  private final class Worker(name: String) {
    @volatile var thread: Thread              = _
    @volatile var timedOut: Option[Throwable] = None
    val current                               = new AtomicReference[Running]()
    val executor = new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](),
      (r: Runnable) => {
        val t = new Thread(r, name)
        t.setDaemon(true)
        t.setContextClassLoader(classLoader)
        thread = t
        t
      }
    )
  }

  @volatile private var worker = new Worker(threadName)

  // Set once the handler has thrown, returned null or not returned, so is not called again.
  @volatile private var broken: Option[Throwable] = None
  // Calls not made, or not completed, because the handler was broken: each a row's failure that
  // was not handled.
  private val skipped = new AtomicInteger()

  /** The error the handler failed the partition with, if it has. */
  def failed: Option[Throwable] = Option(failure.get())

  private def fail(err: Throwable): Unit =
    if (failure.compareAndSet(null, err)) {
      partitionFailed.set(true)
      logError(s"Partition $partition: the write will fail: ${PartitionWriteStats.describe(err)}")
    }

  private def breakWith(err: Throwable): Unit = {
    if (broken.isEmpty) broken = Some(err)
    fail(err)
  }

  /** For a call that is not made because the handler is broken. */
  private def skip(always: Boolean): PartitionStoppedException = {
    if (!always) skipped.incrementAndGet()
    new PartitionStoppedException(broken)
  }

  def open(context: KeyValueWriteContext): Unit =
    call("open", always = false)(handler.open(context))

  /** @return the handler's decision. If that is FailTask, the partition has failed. */
  def attemptFailed(attempt: KeyValueWriteFailedAttempt): KeyValueWriteRetryDecision =
    call(s"onAttemptFailed for document '${attempt.documentId}'", always = false) {
      val decision = handling(attempt, "onAttemptFailed")(handler.onAttemptFailed(attempt))
      if (decision == KeyValueWriteRetryDecision.FailTask) fail(attempt.error)
      decision
    }

  /** @return the handler's decision. If that is FailTask, the partition has failed. */
  def onFailure(f: KeyValueWriteFailure): KeyValueWriteFailureDecision =
    call(s"onFailure for document '${f.documentId}'", always = false) {
      val decision = handling(f, "onFailure")(handler.onFailure(f))
      // Set here, on the handler's thread, so that new rows and retries stop at once. The failures
      // of rows already being written are still handled.
      if (decision == KeyValueWriteFailureDecision.FailTask) fail(f.error)
      if (decision == KeyValueWriteFailureDecision.Continue) {
        errorBucket.foreach(writeErrorDocument(f, _))
      }
      decision
    }

  /** Writes the failure's error document. Called on the handler's thread, after the handler has
    * decided, and given a timeout of its own by restarting the call's clock.
    */
  private def writeErrorDocument(f: KeyValueWriteFailure, docs: KeyValueErrorDocumentHandler) = {
    worker.current.set(
      Running(
        s"writing the error document for document '${f.documentId}'",
        System.nanoTime(),
        "KeyValueOptions.ErrorBucket"
      )
    )
    try {
      docs.handleError(
        f.documentId,
        f.bucket,
        f.scope,
        f.collection,
        f.error,
        _ => stats.recordErrorDocumentRetry()
      )
    } catch {
      case e: InterruptedException => throw e
      case e: Throwable =>
        e.addSuppressed(f.error)
        breakWith(e)
        throw e
    }
  }

  /** Calls the handler's close, even if the partition has failed or the task has been killed, and
    * waits for it. Must be the last call.
    */
  def close(error: Option[Throwable]): Unit = {
    val notHandled = skipped.get()
    if (notHandled > 0) {
      logWarning(
        s"Partition $partition: $notHandled write failures were not passed to " +
          "KeyValueOptions.ErrorHandler or ErrorBucket, as handling an earlier one had failed: " +
          broken.fold("")(PartitionWriteStats.describe)
      )
    }
    val w = worker
    if (w.timedOut.isDefined) {
      logWarning(
        s"Partition $partition: calling KeyValueOptions.ErrorHandler's close on a new thread, as " +
          s"its thread has not returned from ${Option(w.current.get()).fold("a call")(_.what)}"
      )
      worker = new Worker(s"$threadName-close")
    }
    // A killed task's thread is already interrupted (writeAll restores the flag), which would
    // otherwise stop it waiting at all. Restored after.
    val wasInterrupted = Thread.interrupted()
    try call("close", always = true)(handler.close(error))
    finally {
      worker.executor.shutdown()
      if (wasInterrupted) Thread.currentThread().interrupt()
    }
  }

  /** Wraps an exception from the handler, so the log and the task's error say where it came from.
    */
  private def handling[T](f: KeyValueWriteFailure, method: String)(body: => T): T = {
    val result =
      try body
      catch {
        case e: InterruptedException => throw e
        case e: Throwable =>
          val err = new RuntimeException(
            s"KeyValueOptions.ErrorHandler failed in $method for document '${f.documentId}': $e",
            e
          )
          err.addSuppressed(f.error)
          breakWith(err)
          throw err
      }
    if (result == null) {
      val err = new IllegalStateException(
        s"KeyValueOptions.ErrorHandler returned null from $method for document '${f.documentId}'"
      )
      breakWith(err)
      throw err
    }
    result
  }

  /** Runs `body` on the handler's thread, and waits for it.
    *
    * @param always
    *   whether to run it even if the handler is broken: for close.
    */
  private def call[T](what: String, always: Boolean)(body: => T): T = {
    if (!always && broken.isDefined) throw skip(always)
    val w         = worker
    val submitted = System.nanoTime()
    val task = new FutureTask[T](() => {
      // The handler may have broken while this was waiting its turn.
      if (!always && broken.isDefined) throw skip(always)
      val started = System.nanoTime()
      stats.recordHandlerWait(started - submitted)
      w.current.set(Running(what, started))
      try body
      finally {
        w.current.set(null)
        stats.recordHandlerCall(System.nanoTime() - started)
      }
    })

    try w.executor.execute(task)
    catch { case _: RejectedExecutionException => throw skip(always) }

    var interrupted = false
    try {
      while (true) {
        try return task.get(PollInterval.toMillis, TimeUnit.MILLISECONDS)
        catch {
          case e: ExecutionException => throw e.getCause
          case _: TimeoutException =>
            checkTimeout(w)
            // Whether it's this call that did not return, or one queued behind it.
            w.timedOut.foreach { err =>
              if (!always) skipped.incrementAndGet()
              throw err
            }
          // When Spark kills the task, it interrupts the task's thread. close is still waited
          // for (within the timeout), as it would be if it ran on the task's thread.
          case e: InterruptedException => if (always) interrupted = true else throw e
        }
      }
      throw new IllegalStateException("unreachable")
    } finally if (interrupted) Thread.currentThread().interrupt()
  }

  private def checkTimeout(w: Worker): Unit = {
    val running = w.current.get()
    if (running != null && System.nanoTime() - running.startedNanos > callTimeout.toNanos) {
      w.synchronized {
        if (w.timedOut.isEmpty) {
          val err = new RuntimeException(
            s"${running.who} did not return from ${running.what} within " +
              s"$callTimeout (KeyValueOptions.ErrorHandlerTimeout), so the task is failing"
          )
          Option(w.thread).foreach { t =>
            // Where the handler is stuck is more use than where this was noticed.
            err.setStackTrace(t.getStackTrace)
            logError(
              s"Partition $partition: KeyValueOptions.ErrorHandler's thread ${t.getName} is " +
                s"stuck in ${running.what}, at:\n  ${t.getStackTrace.take(20).mkString("\n  ")}"
            )
          }
          breakWith(err)
          w.timedOut = Some(err)
          // Interrupts the call, and drops those queued behind it, whose callers are released by
          // `timedOut`.
          w.executor.shutdownNow()
        }
      }
    }
  }
}

private[kv] object FailureHandlerRunner {
  val DefaultCallTimeout: FiniteDuration = 2.minutes
  private val PollInterval               = 50.millis

  /** The handler when there's only an ErrorBucket: every failure has its error document written,
    * and the write carries on.
    */
  object ContinueHandler extends KeyValueWriteFailureHandler {
    override def onFailure(f: KeyValueWriteFailure): KeyValueWriteFailureDecision =
      KeyValueWriteFailureDecision.Continue
  }

  /** Whether the user's handler can make retry decisions other than the default. If not, the
    * connector doesn't call `onAttemptFailed`, which saves each retry waiting its turn for the
    * handler.
    */
  def decidesRetries(user: Option[KeyValueWriteFailureHandler]): Boolean =
    user.exists(!_.isInstanceOf[LegacyErrorHandlerAdapter])
}

/** Thrown when a handler call is not made because the handler is broken. Its cause is how. */
private[kv] class PartitionStoppedException(cause: Option[Throwable])
    extends RuntimeException(
      "Not passed to KeyValueOptions.ErrorHandler or ErrorBucket, as " +
        cause.fold("the task is ending")(c => s"handling an earlier failure failed: $c"),
      cause.orNull
    )
