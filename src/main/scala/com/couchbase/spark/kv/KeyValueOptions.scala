/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import com.couchbase.spark.DefaultConstants

/** Helper object to provide type-safe keys and values for Spark SQL query options.
  */
object KeyValueOptions {

  /** Option Key: Instead of using the "default" connection, allows to connect to a different
    * cluster.
    */
  val ConnectionIdentifier = "connectionIdentifier"

  /** Option Key: The name of the bucket, which overrides the implicit bucket configured.
    */
  val Bucket = "bucket"

  /** Option Key: The name of the scope, which overrides the implicit scope (if configured at all).
    *
    * Note: only works against Couchbase Server 7.0 or later.
    */
  val Scope = "scope"

  /** Option Key: The name of the collection, which overrides the implicit collection (if configured
    * at all).
    *
    * Note: only works against Couchbase Server 7.0 or later.
    */
  val Collection = "collection"

  /** Option Key: The name of the collections (comma separated) that will be used for Spark
    * Streaming.
    *
    * The [[Collection]] arg can also be used if only one needs to be specified.
    */
  val Collections = "collections"

  /** Option Key: The field name of the document ID, used to override the default.
    *
    * The default can be located in [[DefaultConstants.DefaultIdFieldName]]
    */
  val IdFieldName = "idFieldName"

  /** Option Key: The field name of the document CAS, used to override the default.
    *
    * This both sets the column name and enables CAS-based operations, where that is supported
    *
    * CAS is supported for:
    * - Replace operations ([[WriteModeReplace]])
    *
    * CAS is not supported for other operations (SaveMode.Overwrite, SaveMode.ErrorIfExists, SaveMode.Ignore).
    */
  val CasFieldName = "casFieldName"

  /** Option key: The durability level of write operations, used to override the default.
    *
    * The default is "none", so no durability applied on write operations.
    */
  val Durability = "durability"

  /** Option Key: The timeout to use which overrides the default configured.
    *
    * The value is a string and must be parsable from a scala Duration.
    */
  val Timeout = "timeout"

  /** Option Key: The number of partitions to use when using Spark Streaming.
    *
    * If not provided, defaults to the defaultParallelism of the spark context.
    */
  val NumPartitions = "numPartitions"

  /** Option Key: Controls from where the connector starts streaming from.
    *
    * If not provided, defaults to
    */
  val StreamFrom = "streamFrom"

  /** Option Key: The type of streaming metadata that should be sent to a downstream consumer per
    * row.
    *
    * Choose either [[StreamMetaDataNone]], [[StreamMetaDataBasic]] or [[StreamMetaDataFull]] as the
    * values, where Basic is the default value if none provided.
    */
  val StreamMetaData = "streamMetaData"

  /** Option Key: If the content of a document should also be streamed, or just the id (Spark
    * Streaming only).
    *
    * The value is a "true" or "false", where "true" is the default.
    */
  val StreamContent = "streamContent"

  /** Option Key: The flow control buffer size to use for a DCP stream (spark streaming only).
    *
    * The value is a buffer size in bytes and defaults to 10MB (1024 * 1024 * 10).
    */
  val StreamFlowControlBufferSize = "streamFlowControlBufferSize"

  /** Option Key: the polling interval to mitigate rollbacks.
    *
    * The value is a string and must be parsable from a scala Duration (defaults to 100ms). If set
    * to 0, the polling interval is disabled.
    */
  val StreamPersistencePollingInterval = "streamPersistencePollingInterval"

  /** Option Key: Enables writing failed documents to a specified Couchbase collection.
   *
   * This handler creates error documents with IDs of the pattern "error_{hash}_{timestamp}", where the hash is
   * of the original document's bucket, scope, collection and ID.
   * 
   * One will be created for each failing underlying KeyValue operation (e.g. an individual upsert,
   * insert or similar).
   *
   * The error document currently contains JSON with the following fields:
   *   - "timestamp": ISO 8601 timestamp when the error occurred
   *   - "documentId": The original document ID that failed
   *   - "bucket": The original bucket name
   *   - "scope": The original scope name
   *   - "collection": The original collection name
   *   - "error": Nested object containing:
   *     - "class": Full class name of the exception
   *     - "simpleClass": Simple class name of the exception
   *     - "context": Couchbase context map (for Couchbase errors)
   *
   * NOTE: these fields should be regarded as somewhat volatile. While the fields above are
   * unlikely to be removed, additional fields may be added in future versions, and not all fields
   * may be available on all every operation. Applications should handle missing fields gracefully.
   * In particular the contents of the "context" field should not be relied upon.
   *
   * The bucket, scope, and collection must already exist, and the user must be able to write to
   * them. This is checked before the write starts, and the write fails if not, before writing
   * anything. A bucket that can't be opened is waited for first, for up to
   * `spark.couchbase.waitUntilReadyTimeout` (1 minute by default).
   *
   * IMPORTANT: When ErrorBucket is specified, individual write operation failures will no longer
   * cause the entire Spark job to fail.
   *
   * Some validation failures that occur before the write operations commence will intentionally continue to fast-fail
   * the Spark job, and will not trigger this error handler.
   *
   * Both ErrorBucket and ErrorHandler may be used together. A [[KeyValueWriteFailureHandler]] then
   * decides first, and error documents are only written for failures it continues with.
   *
   * Error documents are written one at a time per Spark task, and the write waits for each, so if
   * they can't be written as fast as failures occur, the write slows down to match, rather than
   * discarding failures.
   *
   * Error documents are written while writes are failing, which is quite likely because the
   * cluster is struggling, so writing one is retried, with a backoff between attempts that grows
   * to 5 seconds, until it succeeds or [[ErrorHandlerTimeout]] has nearly passed. Only then does
   * the Spark task fail, rather than the failure being lost. Each attempt writes the same document,
   * so an attempt that timed out but was in fact applied does not leave a duplicate.
   */
  val ErrorBucket = "errorBucket"

  /** Option Key: The scope name where error documents should be stored.
   *
   * The scope must already exist. If not specified, uses default scope.
   */
  val ErrorScope = "errorScope"

  /** Option Key: The collection name where error documents should be stored.
   *
   * The collection must already exist. If not specified, uses default collection.
   */
  val ErrorCollection = "errorCollection"

  /** Option Key: an error handler class name for handling write operation errors.
    *
    * The value should be a fully qualified class name that implements
    * [[KeyValueWriteFailureHandler]], or the older [[KeyValueWriteErrorHandler]]. The class code
    * must be present on the classpath of what the Spark executor is given to run.
    *
    * When ErrorHandler is specified, an individual write operation failure no longer fails the
    * Spark job by default: the handler decides. A [[KeyValueWriteFailureHandler]] can ignore it,
    * carry on, or fail the Spark task. A [[KeyValueWriteErrorHandler]] always carries on.
    *
    * Options for the handler can be given to the write as `errorHandler.<name>`, and are available
    * to a [[KeyValueWriteFailureHandler]] from [[KeyValueWriteContext.option]].
    *
    * Some validation failures that occur before the write operations commence will intentionally continue to fast-fail
    * the Spark job, and will not trigger this error handler.
    *
    * Note the ErrorHandler will be run on the Spark executor - NOT the main application.  See
    * [[KeyValueWriteFailureHandler]] for why this is important.
    *
    * The handler is called one failure at a time per Spark task, on a thread of its own, and the
    * write waits for each call. So a slow handler intentionally slows the write down. A handler
    * that prefers to do slow work asynchronously can pass failures to a thread of its own.
    *
    * A handler call that takes longer than [[ErrorHandlerTimeout]] fails the Spark task.
    */
  val ErrorHandler = "errorHandler"

  /** Option Key: how long a single call to the [[ErrorHandler]], or a write of an [[ErrorBucket]]
    * document, may take before the Spark task fails.
    *
    * An [[ErrorBucket]] document is retried for up to this long, less a moment, so that the error
    * reported is that the document could not be written. When there's also an [[ErrorHandler]],
    * the error document gets this long on top of the time the handler took to decide.
    *
    * Defaults to 2 minutes. The value is a string and must be parsable as a scala Duration, such as
    * "5m".
    *
    * This applies to each call separately, including the handler's `close`. A handler that does
    * slow work there, such as uploading a file of the failures it recorded, may need longer.
    */
  val ErrorHandlerTimeout = "errorHandlerTimeout"

  /** Option Key: How many KV write operations each Spark partition keeps in-flight at once.
    *
    * Each partition uses this many threads to write, each performing one blocking KV operation at a
    * time.
    *
    * The value must be a positive integer, and defaults to 16.
    *
    * Note the total concurrency against the cluster is this value multiplied by the number of Spark tasks
    * running concurrently, which is bounded by the executor cores available.
    * The value should be selected carefully, based on the number of Spark cores and the expected capacity of the
    * Couchbase cluster.
    * To give a worked example with the default value of 16: 8 cores will be performing a total of 128 operations in
    * parallel (assuming 8 partitions are available to process), and if each operation takes 1ms, this will create
    * 128,000 operations per second.
    */
  val MaxConcurrentOps = "maxConcurrentOps"

  /** Option Key: how many times the connector retries a failed KV write operation before giving up
    * on it.
    *
    * Defaults to 0: no retries, which is the behaviour of earlier versions.
    *
    * Note the SDK already retries many transient failures internally (such as the server
    * temporarily rejecting writes), until the operation's [[Timeout]]. This option adds retries on
    * top of that, after the operation has failed or timed out. Each retry waits with exponential
    * backoff (100ms doubling up to 5s, with jitter).
    *
    * Only failures that are safe to retry are retried:
    *   - Failures where the server definitely did not apply the operation, such as an
    *     UnambiguousTimeoutException or TemporaryFailureException, are always retryable.
    *   - Failures where the operation may or may not have been applied, such as an
    *     AmbiguousTimeoutException, are retried unless repeating an applied operation would change
    *     the document a second time. That is only the case for sub-document writes containing
    *     arrayAppend, arrayPrepend, arrayInsert, increment or decrement specs, without CAS, not
    *     using [[WriteModeSubdocInsert]], and without [[Idempotency]] enabled. These are not
    *     retried after an ambiguous failure.  To have these retried also, enable [[Idempotency]].
    *
    *     All other writes are safe to repeat: either a repeat leaves the document the same (upserts,
    *     replaces), or it fails without changing anything (a CAS write finds the CAS has moved, an
    *     insert finds the document exists, a sub-document insert or remove finds the path already
    *     inserted or removed).
    *   - Other failures, such as DocumentNotFoundException, are not retried.
    *
    * A [[KeyValueWriteFailureHandler]] set with [[ErrorHandler]] can override this for each failed
    * attempt, with its `onAttemptFailed` method.
    *
    * When a write fails in a way that suggests the cluster is struggling (a timeout, or the server
    * being temporarily unable to take writes), every writer in the Spark task also waits before
    * its next operation, whether that's a retry or not, until writes start succeeding again. This
    * reduces the load the write puts on the cluster, rather than adding retries to it.
    *
    * Once an operation has failed and will not be retried, it is handled exactly as it would be
    * without this option: passed to the [[ErrorHandler]] / [[ErrorBucket]] if configured, and
    * otherwise failing the Spark task.  With current Spark defaults, Spark will then retry the
    * task up to 3 times (`spark.task.maxFailures` defaults to 4 attempts in total; note that in
    * local mode it defaults to 1, so the task is not retried).
    *
    * The value must be a non-negative integer.
    */
  val RetryAttempts = "retryAttempts"

  /** Option Key: makes more write operations idempotent - safe to repeat - than they are by
    * default, so that a write repeated after an ambiguous failure (by [[RetryAttempts]]), or because
    * Spark retried a task or stage, is not applied twice.
    *
    * Defaults to [[IdempotencyNone]].
    *
    * Sub-document writes ([[WriteModeSubdocUpsert]], [[WriteModeSubdocReplace]] and
    * [[WriteModeSubdocInsert]]): this matters most for sub-document operations that are not
    * naturally idempotent, such as arrayAppend, arrayPrepend, arrayInsert, increment and decrement.
    * The connector generates an idempotency key for each row, and stores it alongside the
    * document, so that it can tell whether a write was actually applied if it needs to be repeated.
    * This works across Spark's task and stage retries. It uses a small amount of metadata per
    * document (under 100 bytes, in an extended attribute named `cbspark`), and uses 2 of the 16
    * specs a sub-document write allows, so the DataFrame can have at most 14 sub-document columns.
    *
    * Full-document inserts (SaveMode.ErrorIfExists and SaveMode.Ignore) and replaces using CAS
    * ([[CasFieldName]]): repeating one that was already applied fails, as the document now exists
    * or its CAS has changed. With this option, the connector then reads the document, and if its
    * content is already exactly what the row would write, the row is treated as written rather
    * than failed. Nothing extra is stored, and the read is only made on such a failure. A document
    * that someone else changed to the same content is also treated as written. If the read fails
    * (e.g. it times out), the write is retried like any other failure, per [[RetryAttempts]].
    *
    * Other writes (upserts, and replaces without CAS) are already safe to repeat, and are
    * unaffected.
    */
  val Idempotency = "idempotency"

  /** Option value: no idempotency - to be used with [[Idempotency]] as the key. The default. */
  val IdempotencyNone = "none"

  /** Option value: idempotency enabled - to be used with [[Idempotency]] as the key. */
  val IdempotencyEnabled = "enabled"

  /** Option Key: an advanced option for a somewhat niche case: re-running a Spark job whose write failed
    * part-way through, without applying the rows it had already written a second time. Only
    * relevant for sub-document writes with [[Idempotency]] enabled, and most users will not need
    * it. (Full-document writes with [[Idempotency]] do not need it: they compare content instead.)
    *
    * Each write is identified by a WriteId, which is part of every row's idempotency key. If not
    * specified, a random one is generated for each write, and logged by the Spark driver. All of
    * Spark's own task and stage retries within that job share it, so they need nothing extra.
    *
    * A job that is re-submitted after failing is a new write, with a new WriteId, so rows the
    * failed job had already applied would be applied again. To avoid that, re-run the job with this
    * option set to the WriteId logged by the failed job, and with the same input data.
    *
    * Note this does not resume the job from where it failed: every row is written again. For each
    * row, the idempotency key is used to detect whether the failed job had already applied it, and if
    * so it is skipped rather than applied a second time.
    *
    * If error handling is enabled ([[ErrorHandler]] or [[ErrorBucket]]), individual write failures
    * do not fail the job unless the handler decides to, so jobs failing part-way through are not
    * expected and this option should not be needed except to support extreme cases such as Sparker
    * worker failure.
    *
    * Must be 1 to 64 characters from A-Z, a-z, 0-9, '-' and '_'. Only valid when [[Idempotency]] is
    * enabled.
    */
  val WriteId = "writeId"

  /** Option value: Majority Durability - to be used with [[Durability]] as the key.
    */
  val MajorityDurability = "majority"

  /** Option value: Majority And Persist To Active Durability - to be used with [[Durability]] as
    * the key.
    */
  val MajorityAndPersistToActiveDurability = "majorityAndPersistToActive"

  /** Option value: Persist To Majority Durability - to be used with [[Durability]] as the key.
    */
  val PersistToMajorityDurability = "PersistToMajority"

  /** Option value: Do not stream any additional metadata for a spark stream.
    */
  val StreamMetaDataNone = "none"

  /** Option value: Only stream basic metadata information.
    */
  val StreamMetaDataBasic = "basic"

  /** Option value: Stream all mutation metadata that is available.
    */
  val StreamMetaDataFull = "full"

  /** Option value: If an offset has been saved start from there, otherwise start the Structured
    * stream from "now" (no mutations are streamed from before this point).
    */
  val StreamFromNow = "fromNow"

  /** Option value: If an offset has been saved start from there, otherwise start the Structured
    * stream from "beginning" (previous mutations are streamed from before this point).
    */
  val StreamFromBeginning = "fromBeginning"

  /** Option Key: Specifies the write mode for operations.
   *
   * Can be used with values like [[WriteModeReplace]] to specify the operation type.
   * This is used when Spark's SaveMode is not sufficient for the required operation.
   * When WriteMode is set, Spark's SaveMode will be ignored.
   */
  val WriteMode = "writeMode"

  /** Option value: Use replace operation for writes - to be used with [[WriteMode]] as the key.
    *
    * Replace operations require that documents already exist in the target collection.
    * If any document does not exist, the operation will fail with DocumentNotFoundException.
    *
    * CAS support:
    * Couchbase's optimistic locking, CAS, is supported.
    * This mode will use CAS values if the [[CasFieldName]] option is specified and if such a CAS column
    * is present in the DataFrame with valid values. If a custom CAS field name is specified
    * and the column is missing or contains invalid values, the job will fast-fail.
    *
    * If the replace of the document fails because the document has changed since the read (e.g. the CAS has changed),
    * that operation will fail with a CasMismatchException.
    *
    * Any operation failure, such as DocumentNotFoundException, CasMismatchException or other, will generally result in
    * job failure unless error handling is configured using either:
    * - [[ErrorHandler]]: A custom error handler class to handle failures
    * - [[ErrorBucket]]: Write failed operations to Couchbase for later analysis
    *
    * When WriteModeReplace is used, Spark's SaveMode is ignored and replace semantics are applied.
    */
  val WriteModeReplace = "replace"

  /** Option value: Use sub-document insert semantics for writes - to be used with [[WriteMode]] as the key.
    *
    * Sub-document operations
    * -----------------------
    * Sub-document writes are a powerful Couchbase feature that allows specific parts of a document to be
    * updated without having to replace the entire document.
    * In the context of a Spark DataFrame:
    * Each row will become a single write to a particular document, as usual.
    * Each column will become a sub-document operation for that write.
    * The operation is specified by the heading of the column.
    * The heading of the column is a string that looks like this:
    * "upsert:foo"
    * The operation would be "upsert" and the path would be "foo".
    * The value would be the content of the DataFrame at that cell.
    *
    * More complex operations are supported:
    * "arrayAppend:reviews[0].ratings.Rooms"
    * The operation would be "arrayAppend" and the path would be "reviews[0].ratings.Rooms".
    * The value would be the content of the DataFrame at that cell.
    *
    * The path is passed verbatim to the SDK.  See this documentation for more details on the path syntax
    * and the supported operations:
    * https://docs.couchbase.com/scala-sdk/current/howtos/subdocument-operations.html
    * All operations are supported: upsert, insert, replace, remove, arrayAppend, arrayPrepend,
    * arrayInsert, arrayAddUnique, increment, decrement.
    *
    * Failures
    * --------
    * Sub-document insert operations require that documents do not already exist in the target collection.
    * If any document already exists, the operation will fail with DocumentExistsException.
    *
    * Any operation failure will generally result in job failure unless error handling is configured
    * using either ErrorHandler or ErrorBucket.
    */
  val WriteModeSubdocInsert = "subdocInsert"

  /** Option value: Use sub-document replace semantics for writes - to be used with [[WriteMode]] as the key.
    *
    * See the [[WriteModeSubdocInsert]] documentation for more details.
    *
    * The key difference is that operations will fail if the doc does not already exist.  Every document
    * written is required to exist.
    *
    * CAS support
    * -----------
    * Couchbase's optimistic locking, CAS, is supported for subdocReplace operations.
    * This mode will use CAS values if the [[CasFieldName]] option is specified and if such a CAS column
    * is present in the DataFrame with valid values. If a custom CAS field name is specified
    * and the column is missing or contains invalid values, the job will fast-fail.
    * If the operation fails because the document has changed since the read (e.g. the CAS has changed),
    * that operation will fail with a CasMismatchException.
    */
  val WriteModeSubdocReplace = "subdocReplace"

  /** Option value: Use sub-document upsert semantics for writes - to be used with [[WriteMode]] as the key.
    *
    * See the [[WriteModeSubdocInsert]] documentation for more details.
    *
    * The key difference from replace and insert mode, is that upsert will create the document if it does not already
    * exist.
    */
  val WriteModeSubdocUpsert = "subdocUpsert"
}
