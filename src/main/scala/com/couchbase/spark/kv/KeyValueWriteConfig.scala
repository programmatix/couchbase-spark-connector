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

import com.couchbase.spark.util.Redaction

import scala.concurrent.duration.FiniteDuration
import scala.runtime.ScalaRunTime

case class KeyValueWriteConfig(
    bucket: String,
    scope: Option[String],
    collection: Option[String],
    idFieldName: String,
    casFieldName: Option[String],
    durability: Option[String],
    timeout: Option[String],
    connectionIdentifier: Option[String],
    errorHandler: Option[String],
    errorBucket: Option[String],
    errorScope: Option[String],
    errorCollection: Option[String],
    writeMode: Option[String],
    maxConcurrentOps: Int,
    retryAttempts: Int = 0,
    // KeyValueOptions.IdempotencyEnabled when idempotency is enabled.
    idempotency: Option[String] = None,
    // Set when idempotency is enabled for a sub-document write mode, the only ones that use it.
    writeId: Option[String] = None,
    // The "errorHandler.*" options, for the error handler: keys lower-cased, without the prefix.
    errorHandlerOptions: Map[String, String] = Map.empty,
    errorHandlerTimeout: FiniteDuration = FailureHandlerRunner.DefaultCallTimeout
) {
  // The error handler's option values may be credentials, so only their names are logged.
  override def toString: String = ScalaRunTime._toString(
    copy(
      connectionIdentifier = Redaction.connectionIdentifier(connectionIdentifier),
      errorHandlerOptions = errorHandlerOptions.map { case (k, _) => k -> "<redacted>" }
    )
  )
}
