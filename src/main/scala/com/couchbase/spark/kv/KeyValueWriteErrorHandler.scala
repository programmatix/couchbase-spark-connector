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

/** Case class containing error information for KeyValue write operations.
  *
  * @param bucket The bucket name where the error occurred
  * @param scope The scope name where the error occurred
  * @param collection The collection name where the error occurred
  * @param documentId The document ID that failed to write
  * @param throwable The original exception that occurred, or None if the exception could not be returned (which may be because it's not serializable)
  */
case class KeyValueWriteErrorInfo(
    bucket: String,
    scope: String,
    collection: String,
    documentId: String,
    throwable: Option[Throwable]
)

/** Trait for handling errors during Couchbase KeyValue write operations.
  *
  * This allows applications to define custom error handling behavior for all write operations
  * instead of having operations fail the entire job.
  *
  * Blocking operations are permitted. The handler is called on a thread of its own, one error at a
  * time, and the write waits for each call, so a slow handler slows the write down. A call that
  * takes longer than [[KeyValueOptions.ErrorHandlerTimeout]] fails the Spark task.
  *
  * See ExampleErrorHandler.
  *
  * [[KeyValueWriteFailureHandler]] is the newer interface, and is preferred: it can also decide to
  * ignore a failure or fail the Spark task, and is given more about each failure.
  */
@deprecated("KeyValueWriteFailureHandler is the newer and preferred interface")
trait KeyValueWriteErrorHandler extends Serializable {

  /** Called when an error occurs during a write operation.
    *
    * @param errorInfo
    *   Case class containing error details
    */
  def onError(errorInfo: KeyValueWriteErrorInfo): Unit
}
