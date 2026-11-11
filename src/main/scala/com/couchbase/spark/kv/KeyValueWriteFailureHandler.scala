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

import java.util.Locale

/** Handles rows that a Key-Value DataFrame write could not write, and decides what happens next.
  *
  * Configure it with [[KeyValueOptions.ErrorHandler]], giving the class name. This replaces
  * [[KeyValueWriteErrorHandler]], which is still supported.
  *
  * A row only reaches [[onFailure]] once the connector's own retries
  * ([[KeyValueOptions.RetryAttempts]]) are used up, or if its failure is not one that is retried.
  * The handler can also make those retry decisions itself, with [[onAttemptFailed]].
  *
  * ==Lifecycle==
  * The handler is created on the Spark driver when the write starts, using its no-argument
  * constructor, so it must have one, and must be Serializable. Each Spark task attempt (one per
  * partition, plus any retries) then gets its own copy on a Spark executor:
  *
  *   1. [[open]] is called once before any row is written.
  *   1. [[onAttemptFailed]] is called each time an attempt to write a row fails and
  *      [[KeyValueOptions.RetryAttempts]] allows another.
  *   1. [[onFailure]] is called for each row that could not be written.
  *   1. [[close]] is called once the write has finished. It is always called, including when the
  *      task is failing, and even if [[open]] threw, so it must cope with state that [[open]] did
  *      not set up.
  *
  * Since this runs on the executors, not the driver, anything the handler records in memory is not
  * visible to the application. To get results back, write them somewhere durable, such as a file
  * system or a database.
  *
  * ==Threads and timing==
  * Each task attempt has a thread for its handler, and every method is called on that thread, one
  * call at a time. So the handler does not need to be thread-safe. Note that Spark's
  * `TaskContext.get()` is not available on it: use [[KeyValueWriteContext]] instead.
  *
  * The connector's writers wait for each call to return before continuing, so a slow handler slows
  * writes. This is deliberate: when writes are failing, the cluster may be struggling, and error
  * handling should not add to its load.
  *
  * No failure is dropped, including when the task is failing: whether because of the handler (see
  * below), or something else, such as an error reading the input or Spark killing the task. The
  * write then stops starting new rows, and stops retrying, but rows already being written finish,
  * and any that fail are passed to [[onFailure]]. Rows not yet started are not passed to the
  * handler, as they were not written. Only then is [[close]] called, with the task's error, and the
  * task fails. Spark's next attempt at the task writes every row of the partition again, including
  * those not started, so a failure handled in an attempt that failed may be handled again, and what
  * such an attempt recorded should be treated as incomplete.
  *
  * The one exception is a handler that throws, returns null, or does not return: it is not called
  * again, other than [[close]], and how many failures it was not given is logged.
  *
  * If a call has not returned after [[KeyValueOptions.ErrorHandlerTimeout]] (2 minutes by
  * default), the task fails, and the handler's thread is interrupted. [[close]] is then called on
  * a new thread, while the call that did not return may still be running on the old one.
  *
  * ==Failing the task==
  * The Spark task fails if [[onFailure]] returns [[KeyValueWriteFailureDecision.FailTask]],
  * [[onAttemptFailed]] returns [[KeyValueWriteRetryDecision.FailTask]], or any of these methods
  * throws or times out. Spark then retries the task, up to `spark.task.maxFailures` attempts in
  * total, and each retry writes every row of the partition again. See
  * [[KeyValueOptions.Idempotency]] for making that safe for all writes.
  */
trait KeyValueWriteFailureHandler extends Serializable {

  /** Called before any row of this task attempt is written. Throwing fails the task, so this is a
    * good place to check the handler's configuration.
    */
  def open(context: KeyValueWriteContext): Unit = ()

  /** Called when an attempt to write a row has failed and [[KeyValueOptions.RetryAttempts]] allows
    * another, to decide whether to retry. Not called when RetryAttempts is 0 (the default), or once
    * the retries are used up.
    *
    * The default is [[KeyValueWriteRetryDecision.Default]]: retry if the connector would.
    */
  def onAttemptFailed(attempt: KeyValueWriteFailedAttempt): KeyValueWriteRetryDecision =
    KeyValueWriteRetryDecision.Default

  /** Called for each row that could not be written. */
  def onFailure(failure: KeyValueWriteFailure): KeyValueWriteFailureDecision

  /** Called once this task attempt's write has finished and every failure has been handled.
    *
    * @param error
    *   the error the task is failing with, or None if it is succeeding. Throwing when it is None
    *   fails the task.
    */
  def close(error: Option[Throwable]): Unit = ()
}

/** What happens after a [[KeyValueWriteFailureHandler]] has handled a failure.
  *
  * If [[KeyValueOptions.ErrorBucket]] is also set, an error document is written only for failures
  * the handler decides to [[KeyValueWriteFailureDecision.Continue]] with.
  */
sealed trait KeyValueWriteFailureDecision

object KeyValueWriteFailureDecision {

  /** The failure was expected, and is not an error: it is counted as ignored rather than failed.
    * The write carries on.
    */
  case object Ignore extends KeyValueWriteFailureDecision

  /** The failure has been handled, for example recorded: it is counted as failed. The write
    * carries on.
    */
  case object Continue extends KeyValueWriteFailureDecision

  /** Fail the Spark task, with this row's error. No new rows are started, but rows already being
    * written finish, and any that fail are still passed to
    * [[KeyValueWriteFailureHandler.onFailure]], before the task fails.
    */
  case object FailTask extends KeyValueWriteFailureDecision
}

/** Whether to retry a failed attempt, from [[KeyValueWriteFailureHandler.onAttemptFailed]]. */
sealed trait KeyValueWriteRetryDecision

object KeyValueWriteRetryDecision {

  /** Retry if the connector would, as described in [[KeyValueOptions.RetryAttempts]]:
    * [[KeyValueWriteFailedAttempt.retriedByDefault]].
    */
  case object Default extends KeyValueWriteRetryDecision

  /** Retry, even if the connector would not. This includes a failure where the write may already
    * have been applied, and repeating it would apply its array or counter specs twice: the handler
    * takes responsibility for that being safe.
    */
  case object Retry extends KeyValueWriteRetryDecision

  /** Do not retry. The row has failed, and is passed to
    * [[KeyValueWriteFailureHandler.onFailure]].
    */
  case object GiveUp extends KeyValueWriteRetryDecision

  /** Fail the Spark task now, with this attempt's error. The row is not passed to
    * [[KeyValueWriteFailureHandler.onFailure]].
    */
  case object FailTask extends KeyValueWriteRetryDecision
}

/** A row that a Key-Value DataFrame write could not write.
  *
  * Methods may be added in future versions.
  */
trait KeyValueWriteFailure {
  def documentId: String
  def bucket: String
  def scope: String
  def collection: String

  /** The error from each attempt to write the row, in order. Never empty: there is one more than
    * the number of retries made.
    */
  def errors: Seq[Throwable]

  /** The error from the last attempt, which is why the row failed. */
  def error: Throwable = errors.last

  /** Whether any attempt failed in a way where it is unknown whether the server applied the write,
    * such as an AmbiguousTimeoutException. If so, the document may have been written even though
    * the row failed: for example, an applied attempt followed by a retry that found the CAS
    * changed.
    */
  def mayHaveBeenApplied: Boolean

  /** The row as JSON, as it was given to the write, including the document id column and any CAS
    * column. Note that this holds the row's data, which may be sensitive.
    */
  def rowJson: String
}

/** An attempt to write a row that failed, given to
  * [[KeyValueWriteFailureHandler.onAttemptFailed]]. [[error]] is this attempt's error.
  *
  * Methods may be added in future versions.
  */
trait KeyValueWriteFailedAttempt extends KeyValueWriteFailure {

  /** Which attempt this was: 0 for the first. */
  def attempt: Int = errors.size - 1

  /** How many more retries [[KeyValueOptions.RetryAttempts]] allows: at least 1. */
  def retriesRemaining: Int

  /** Whether the connector would retry, i.e. what [[KeyValueWriteRetryDecision.Default]] does. */
  def retriedByDefault: Boolean
}

/** About the Spark task attempt a [[KeyValueWriteFailureHandler]] is running in, and the write.
  *
  * Methods may be added in future versions.
  */
trait KeyValueWriteContext {
  def partitionId: Int
  def stageId: Int
  def stageAttemptNumber: Int

  /** How many times this task has been attempted before: 0 for the first attempt. */
  def taskAttemptNumber: Int

  def bucket: String
  def scope: String
  def collection: String

  /** The write's [[KeyValueOptions.WriteId]], if it has one: set for sub-document writes with
    * [[KeyValueOptions.Idempotency]] enabled.
    */
  def writeId: Option[String]

  /** An option given to the write for the handler, as `errorHandler.<name>`. Names are
    * case-insensitive, like all Spark write options. For example, the value of
    * `.option("errorHandler.directory", "s3a://bucket/errors")` is `option("directory")`.
    */
  def option(name: String): Option[String]
}

/** Both a row's failure, and a failed attempt: `retriesRemaining` and `retriedByDefault` only mean
  * anything for the latter.
  */
private[kv] case class WriteFailure(
    documentId: String,
    bucket: String,
    scope: String,
    collection: String,
    errors: Seq[Throwable],
    rowJson: String,
    retriesRemaining: Int = 0,
    retriedByDefault: Boolean = false
) extends KeyValueWriteFailedAttempt {
  require(errors.nonEmpty)

  override def mayHaveBeenApplied: Boolean =
    errors.exists(e => WriteRetry.classify(e) == WriteRetry.Ambiguous)
}

private[kv] case class WriteContext(
    partitionId: Int,
    stageId: Int,
    stageAttemptNumber: Int,
    taskAttemptNumber: Int,
    bucket: String,
    scope: String,
    collection: String,
    writeId: Option[String],
    // Keys lower-cased, without the "errorHandler." prefix.
    options: Map[String, String]
) extends KeyValueWriteContext {
  override def option(name: String): Option[String] = options.get(name.toLowerCase(Locale.ROOT))
}

private[kv] object WriteContext {
  val OptionPrefix = "errorhandler."

  /** The options meant for the handler, from all the write's options. */
  def handlerOptions(options: Iterable[(String, String)]): Map[String, String] =
    options.flatMap { case (k, v) =>
      val key = k.toLowerCase(Locale.ROOT)
      if (key.startsWith(OptionPrefix) && key.length > OptionPrefix.length) {
        Some(key.stripPrefix(OptionPrefix) -> v)
      } else None
    }.toMap
}

/** Adapts a [[KeyValueWriteErrorHandler]] to the newer interface, keeping its behaviour: an
  * exception it throws is logged, but does not fail the task.
  */
private[kv] class LegacyErrorHandlerAdapter(handler: KeyValueWriteErrorHandler)
    extends KeyValueWriteFailureHandler
    with org.apache.spark.internal.Logging {

  override def onFailure(f: KeyValueWriteFailure): KeyValueWriteFailureDecision = {
    try {
      handler.onError(
        KeyValueWriteErrorInfo(f.bucket, f.scope, f.collection, f.documentId, Some(f.error))
      )
    } catch {
      case scala.util.control.NonFatal(e) =>
        logWarning(s"Custom error handler failed for document ${f.documentId}: ${e.getMessage}")
    }
    KeyValueWriteFailureDecision.Continue
  }
}
