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

import com.couchbase.client.core.error.{
  CasMismatchException,
  CouchbaseException,
  DocumentExistsException
}
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.codec.RawJsonTranscoder
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.json.{JsonObject}
import com.couchbase.client.scala.kv.{
  InsertOptions,
  MutateInOptions,
  ReplaceOptions,
  StoreSemantics,
  UpsertOptions
}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.TaskContext
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.{
  BinaryType,
  BooleanType,
  IntegerType,
  LongType,
  MapType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._
import java.util
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}

class KeyValueTableProvider
    extends Logging
    with TableProvider
    with DataSourceRegister
    with CreatableRelationProvider {

  override def shortName(): String = "couchbase.kv"

  override def createRelation(
      ctx: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    // Validate before writing anything: rejecting the save mode after the write would leave every
    // document mutated while the job reports failure.
    if (mode == SaveMode.Append) {
      throw new IllegalArgumentException(
        "SaveMode.Append is not supported - please use the other SaveMode types."
      )
    }

    val couchbaseConfig = CouchbaseConfig(
      ctx.sparkContext.getConf,
      parameters.get(KeyValueOptions.ConnectionIdentifier)
    )

    val writeModeOpt = Option(parameters.get(KeyValueOptions.WriteMode))
    val isSubdocMode = writeModeOpt.flatten.exists(m =>
      m == KeyValueOptions.WriteModeSubdocInsert ||
        m == KeyValueOptions.WriteModeSubdocReplace ||
        m == KeyValueOptions.WriteModeSubdocUpsert
    )
    // Parsed here on the driver, so that a generated WriteId is the same for every task.
    val config = writeConfig(parameters.asJava, couchbaseConfig)

    if (isSubdocMode) {
      val idFieldName =
        parameters.getOrElse(KeyValueOptions.IdFieldName, DefaultConstants.DefaultIdFieldName)
      val casFieldOpt = parameters.get(KeyValueOptions.CasFieldName)
      SubdocUtil.validate(
        data.schema,
        idFieldName,
        casFieldOpt,
        IdempotentWrites.specsUsed(config.idempotency)
      )
    }

    config.writeId.foreach { id =>
      logInfo(
        s"KV dataframe write using KeyValueOptions.WriteId '$id'.  If this write fails, it can be " +
          s"safely re-run on the same input by setting KeyValueOptions.WriteId to '$id': rows that " +
          "were already applied will be skipped."
      )
    }

    data.toJSON.foreachPartition(
      new RelationPartitionWriter(
        config,
        couchbaseConfig,
        mode
      )
    )

    new BaseRelation {
      override def sqlContext: SQLContext = ctx
      override def schema: StructType     = data.schema
    }
  }

  def writeConfig(
      properties: util.Map[String, String],
      conf: CouchbaseConfig
  ): KeyValueWriteConfig = {
    val writeMode = Option(properties.get(KeyValueOptions.WriteMode)) match {
      case None                                   => None
      case Some(KeyValueOptions.WriteModeReplace) => Some(KeyValueOptions.WriteModeReplace)
      case Some(KeyValueOptions.WriteModeSubdocInsert) =>
        Some(KeyValueOptions.WriteModeSubdocInsert)
      case Some(KeyValueOptions.WriteModeSubdocReplace) =>
        Some(KeyValueOptions.WriteModeSubdocReplace)
      case Some(KeyValueOptions.WriteModeSubdocUpsert) =>
        Some(KeyValueOptions.WriteModeSubdocUpsert)
      case Some(v) =>
        throw new IllegalArgumentException("Unknown KeyValueOptions.WriteMode option: " + v)
    }

    val maxConcurrentOps = Option(properties.get(KeyValueOptions.MaxConcurrentOps)) match {
      case None => RelationPartitionWriter.DefaultMaxConcurrentOps
      case Some(v) =>
        v.trim.toIntOption.filter(_ > 0).getOrElse(
          throw new IllegalArgumentException(
            s"KeyValueOptions.MaxConcurrentOps must be a positive integer, but was: $v"
          )
        )
    }

    val retryAttempts = Option(properties.get(KeyValueOptions.RetryAttempts)) match {
      case None => 0
      case Some(v) =>
        v.trim.toIntOption.filter(_ >= 0).getOrElse(
          throw new IllegalArgumentException(
            s"KeyValueOptions.RetryAttempts must be a non-negative integer, but was: $v"
          )
        )
    }

    val errorHandlerTimeout = Option(properties.get(KeyValueOptions.ErrorHandlerTimeout)) match {
      case None => FailureHandlerRunner.DefaultCallTimeout
      case Some(v) =>
        Try(Duration(v.trim)).toOption
          .collect { case d: FiniteDuration if d > Duration.Zero => d }
          .getOrElse(
            throw new IllegalArgumentException(
              s"KeyValueOptions.ErrorHandlerTimeout must be a positive duration such as '5m', " +
                s"but was: $v"
            )
          )
    }

    val isSubdoc = writeMode.exists(m =>
      m == KeyValueOptions.WriteModeSubdocInsert ||
        m == KeyValueOptions.WriteModeSubdocReplace ||
        m == KeyValueOptions.WriteModeSubdocUpsert
    )

    val idempotency = Option(properties.get(KeyValueOptions.Idempotency)) match {
      case None | Some(KeyValueOptions.IdempotencyNone) => None
      case Some(v @ KeyValueOptions.IdempotencyEnabled) => Some(v)
      case Some(v) =>
        throw new IllegalArgumentException("Unknown KeyValueOptions.Idempotency option: " + v)
    }

    // Only sub-document writes store an idempotency key, so only they need a WriteId.
    // Full-document writes compare the document's content instead.
    val usesWriteId = idempotency.isDefined && isSubdoc
    val writeId = Option(properties.get(KeyValueOptions.WriteId)) match {
      case Some(_) if idempotency.isEmpty =>
        throw new IllegalArgumentException(
          "KeyValueOptions.WriteId can only be used when KeyValueOptions.Idempotency is enabled"
        )
      case Some(id) => Some(IdempotentWrites.validateWriteId(id)).filter(_ => usesWriteId)
      case None     => if (usesWriteId) Some(IdempotentWrites.generateWriteId()) else None
    }

    KeyValueWriteConfig(
      conf.implicitBucketNameOr(properties.get(KeyValueOptions.Bucket)),
      conf.implicitScopeNameOr(properties.get(KeyValueOptions.Scope)),
      conf.implicitCollectionName(properties.get(KeyValueOptions.Collection)),
      Option(properties.get(KeyValueOptions.IdFieldName))
        .getOrElse(DefaultConstants.DefaultIdFieldName),
      Option(properties.get(KeyValueOptions.CasFieldName)),
      Option(properties.get(KeyValueOptions.Durability)),
      Option(properties.get(KeyValueOptions.Timeout)),
      Option(properties.get(KeyValueOptions.ConnectionIdentifier)),
      Option(properties.get(KeyValueOptions.ErrorHandler)),
      Option(properties.get(KeyValueOptions.ErrorBucket)),
      Option(properties.get(KeyValueOptions.ErrorScope)),
      Option(properties.get(KeyValueOptions.ErrorCollection)),
      writeMode,
      maxConcurrentOps,
      retryAttempts,
      idempotency,
      writeId,
      WriteContext.handlerOptions(properties.asScala),
      errorHandlerTimeout
    )
  }

  def streamConfig(properties: util.Map[String, String]): KeyValueStreamConfig = {
    val sparkContext = SparkSession.active.sparkContext

    val ci = Option(properties.get(KeyValueOptions.ConnectionIdentifier))
    val conf = CouchbaseConfig(
      sparkContext.getConf,
      ci
    )

    val defaultNumPartitions = sparkContext.defaultParallelism

    var collections: Seq[String] = Seq()

    val userCollection = conf.implicitCollectionName(properties.get(KeyValueOptions.Collection))
    if (userCollection.isDefined) {
      collections = collections :+ userCollection.get
    }
    val userCollections = conf.implicitCollectionName(properties.get(KeyValueOptions.Collections))
    if (userCollections.isDefined) {
      collections = collections ++ userCollections.get.split(",")
    }

    val meta         = properties.get(KeyValueOptions.StreamMetaData)
    val streamXattrs = meta != null && meta.equals(KeyValueOptions.StreamMetaDataFull)

    val streamFrom = properties.get(KeyValueOptions.StreamFrom) match {
      case null | KeyValueOptions.StreamFromNow => StreamFromVariants.FromNow
      case KeyValueOptions.StreamFromBeginning  => StreamFromVariants.FromBeginning
      case v =>
        throw new IllegalArgumentException("Unknown KeyValueOptions.StreamFrom option: " + v)
    }

    val c = KeyValueStreamConfig(
      streamFrom,
      Option(properties.get(KeyValueOptions.NumPartitions))
        .getOrElse(defaultNumPartitions.toString)
        .toInt,
      conf.implicitBucketNameOr(properties.get(KeyValueOptions.Bucket)),
      conf.implicitScopeNameOr(properties.get(KeyValueOptions.Scope)),
      collections,
      Option(properties.get(KeyValueOptions.StreamContent)).getOrElse("true").toBoolean,
      streamXattrs,
      Option(properties.get(KeyValueOptions.StreamFlowControlBufferSize)).map(_.toInt),
      Option(properties.get(KeyValueOptions.StreamPersistencePollingInterval)),
      ci
    )
    logDebug(s"Using KeyValueStreamConfig of $c")
    c
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val always =
      if (
        options.containsKey(KeyValueOptions.StreamContent) && !options
          .get(KeyValueOptions.StreamContent)
          .toBoolean
      ) {
        Seq(
          StructField("id", StringType, nullable = false),
          StructField("deletion", BooleanType, nullable = false)
        )
      } else {
        Seq(
          StructField("id", StringType, nullable = false),
          StructField("content", BinaryType, nullable = true),
          StructField("deletion", BooleanType, nullable = false)
        )
      }

    val basic = Seq(
      StructField("cas", LongType, nullable = false),
      StructField("scope", StringType, nullable = true),
      StructField("collection", StringType, nullable = true)
    )
    val full = Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("vbucket", IntegerType, nullable = false),
      StructField("xattrs", MapType(StringType, StringType), nullable = true)
    )

    val fields = options.get(KeyValueOptions.StreamMetaData) match {
      case KeyValueOptions.StreamMetaDataNone         => always
      case null | KeyValueOptions.StreamMetaDataBasic => always ++ basic
      case KeyValueOptions.StreamMetaDataFull         => always ++ basic ++ full
    }

    logDebug(s"Streaming the following fields $fields")

    StructType(fields)
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    new KeyValueTable(schema, streamConfig(properties))
  }

}

// Each Spark task deserializes its own copy of this class.
class RelationPartitionWriter(
    writeConfig: KeyValueWriteConfig,
    couchbaseConfig: CouchbaseConfig,
    mode: SaveMode
) extends ForeachPartitionFunction[String]
    with Logging {

  // Created here on the driver, so that a handler class that is missing or can't be created fails
  // the write before anything is written.
  private val userHandler: Option[KeyValueWriteFailureHandler] =
    writeConfig.errorHandler.map(validateAndInstantiateCustomErrorHandler)

  // Checked here too, so that a missing collection fails the write at once, rather than each
  // failure retrying its error document until it times out.
  private val errorBucket: Option[KeyValueErrorDocumentHandler] = writeConfig.errorBucket.map {
    _ =>
      val handler = new KeyValueErrorDocumentHandler(writeConfig, couchbaseConfig)
      handler.checkCanWrite()
      handler
  }

  private def hasFailureHandling = userHandler.isDefined || errorBucket.isDefined

  private val loggedSubdocSpecs = new AtomicBoolean(false)

  private def validateAndInstantiateCustomErrorHandler(
      className: String
  ): KeyValueWriteFailureHandler = {
    try {
      val clazz = Class.forName(className)
      clazz.getDeclaredConstructor().newInstance() match {
        case h: KeyValueWriteFailureHandler => h
        case h: KeyValueWriteErrorHandler   => new LegacyErrorHandlerAdapter(h)
        case _ =>
          throw new IllegalArgumentException(
            s"Error handler class $className does not implement KeyValueWriteErrorHandler or " +
              "KeyValueWriteFailureHandler"
          )
      }
    } catch {
      case _: ClassNotFoundException =>
        throw new IllegalArgumentException(s"Error handler class not found: $className")
      case _: InstantiationException =>
        throw new IllegalArgumentException(
          s"Error handler class $className cannot be instantiated (no default constructor?)"
        )
      case _: IllegalAccessException =>
        throw new IllegalArgumentException(
          s"Error handler class $className constructor is not accessible"
        )
      case e: java.lang.reflect.InvocationTargetException =>
        throw new IllegalArgumentException(
          s"Error handler class $className constructor failed: ${e.getCause}",
          e.getCause
        )
      case e: IllegalArgumentException =>
        throw e // Re-throw our custom messages
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Failed to validate error handler class $className: ${e.getMessage}",
          e
        )
    }
  }

  override def call(t: util.Iterator[String]): Unit = {
    val ctx            = TaskContext.get()
    val partitionId    = Option(ctx).map(_.partitionId()).getOrElse(-1)
    val partition      = RelationPartitionWriter.partitionLabel(ctx)
    val scopeName      = writeConfig.scope.getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = writeConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName)
    logInfo(s"Partition ${partition}: KV dataframe write starting")

    val stats = new PartitionWriteStats(
      partition,
      s"couchbase-kv-stats-partition-$partitionId",
      hasErrorHandler = hasFailureHandling
    )
    val partitionFailed = new AtomicBoolean(false)
    val backoff         = new PartitionBackoff(stats, () => partitionFailed.get())

    val failures =
      if (!hasFailureHandling) None
      else {
        Some(
          RelationPartitionWriter.Failures(
            new FailureHandlerRunner(
              userHandler.getOrElse(FailureHandlerRunner.ContinueHandler),
              partition,
              s"couchbase-kv-error-handler-partition-$partitionId",
              partitionFailed,
              stats,
              writeConfig.errorHandlerTimeout,
              errorBucket
            ),
            decidesRetries = FailureHandlerRunner.decidesRetries(userHandler)
          )
        )
      }

    var error: Option[Throwable] = None
    try {
      failures.foreach(_.runner.open(writeContext(ctx, scopeName, collectionName)))

      val coll = CouchbaseConnection(writeConfig.connectionIdentifier)
        .cluster(couchbaseConfig)
        .bucket(writeConfig.bucket)
        .scope(scopeName)
        .collection(collectionName)

      val durability = writeConfig.durability match {
        case Some(v) if v == KeyValueOptions.MajorityDurability => Durability.Majority
        case Some(v) if v == KeyValueOptions.MajorityAndPersistToActiveDurability =>
          Durability.MajorityAndPersistToActive
        case Some(v) if v == KeyValueOptions.PersistToMajorityDurability =>
          Durability.PersistToMajority
        case None => Durability.Disabled
        case d =>
          throw new IllegalArgumentException("Unknown/Unsupported durability provided: " + d)
      }

      logInfo(s"Partition ${partition}: KV dataframe write using config ${writeConfig} saveMode ${mode}")

      stats.start()

      val count = RelationPartitionWriter.writeAll(
        t.asScala,
        writeConfig.maxConcurrentOps,
        i => s"couchbase-kv-writer-partition-$partitionId-$i",
        stats,
        partitionFailed
      ) { encoded =>
        write(coll, durability, encoded, stats, () => partitionFailed.get(), failures, backoff)
      }

      logInfo(s"Partition ${partition}: KV dataframe write completed processing $count documents")
    } catch {
      case err: Throwable => error = Some(err)
    }

    // If the handler failed the partition, that is the real reason, rather than a writer thread
    // finding it had stopped.
    failures.foreach(_.runner.failed.foreach { handlerError =>
      error = error match {
        case None | Some(_: PartitionStoppedException) => Some(handlerError)
        case Some(e) =>
          if (e ne handlerError) e.addSuppressed(handlerError)
          Some(e)
      }
    })

    failures.foreach { f =>
      try f.runner.close(error)
      catch {
        case e: Throwable =>
          error match {
            case None           => error = Some(e)
            case Some(existing) => if (existing ne e) existing.addSuppressed(e)
          }
      }
    }

    logInfo(s"Partition ${partition}: KV dataframe write finishing")
    stats.close()

    error.foreach { err =>
      logError(
        s"Partition ${partition}: KV dataframe write failed due to " +
          PartitionWriteStats.describe(err)
      )
      // Must be rethrown, so that Spark fails (and retries) the task.
      throw err
    }
  }

  private def writeContext(
      ctx: TaskContext,
      scope: String,
      collection: String
  ): KeyValueWriteContext = {
    def get(f: TaskContext => Int): Int = Option(ctx).map(f).getOrElse(-1)
    WriteContext(
      partitionId = get(_.partitionId()),
      stageId = get(_.stageId()),
      stageAttemptNumber = get(_.stageAttemptNumber()),
      taskAttemptNumber = get(_.attemptNumber()),
      bucket = writeConfig.bucket,
      scope = scope,
      collection = collection,
      writeId = writeConfig.writeId,
      options = writeConfig.errorHandlerOptions
    )
  }

  private def toKeyValue(encoded: String): (String, JsonObject, Option[Long]) = {
    val decoded = JsonObject.fromJson(encoded)
    val id      = decoded.str(writeConfig.idFieldName)
    decoded.remove(writeConfig.idFieldName)

    val cas: Option[Long] = writeConfig.casFieldName match {
      case Some(casFieldName) =>
        decoded.safe.numLong(casFieldName) match {
          case Failure(_) =>
            // Fail the job rather than invoking the error handler.  Usually if CAS is not present it's not
            // going to be present for any doc, and we don't want to invoke the error handler pointlessly N
            // times.  Note that documents earlier in the partition may already have been written.
            throw new IllegalArgumentException(
              s"CAS field '${casFieldName}' was specified but no valid CAS value found in that column for document '$id'"
            )
          case Success(cas) =>
            decoded.safe.remove(casFieldName)
            Some(cas)
        }
      case _ => None
    }

    // A query read can now easily add this CAS field.
    // We'll assume the user doesn't want to write a column __META_CAS to their KV docs, regardless of whether
    // they have explicitly requested CAS or not.
    decoded.safe.remove(DefaultConstants.DefaultCasFieldName)

    (id, decoded, cas)
  }

  private def write(
      coll: Collection,
      durability: Durability,
      encoded: String,
      stats: PartitionWriteStats,
      partitionFailed: () => Boolean,
      failures: Option[RelationPartitionWriter.Failures],
      backoff: PartitionBackoff
  ): Unit = {
    val conversionStarted  = System.nanoTime()
    val (id, decoded, cas) = toKeyValue(encoded)
    // For the reads that check whether a conflicting write was this row's own.
    val timeout = writeConfig.timeout.map(Duration(_))

    // `replaySafe`: whether repeating the operation, when it may already have been applied, is safe.
    val (op, replaySafe): (() => Try[WriteRetry.Outcome], Boolean) = writeConfig.writeMode match {
      case Some(KeyValueOptions.WriteModeSubdocInsert) |
          Some(KeyValueOptions.WriteModeSubdocReplace) |
          Some(KeyValueOptions.WriteModeSubdocUpsert) => {
        val (specs, debugInfo) = SubdocUtil.buildSubdocSpecs(decoded)
        val storeSemantics = writeConfig.writeMode.get match {
          case KeyValueOptions.WriteModeSubdocInsert  => StoreSemantics.Insert
          case KeyValueOptions.WriteModeSubdocReplace => StoreSemantics.Replace
          case _                                      => StoreSemantics.Upsert
        }

        // Log only the first to avoid spam.  The specs come from the column names so are usually the same for every document.
        if (loggedSubdocSpecs.compareAndSet(false, true)) {
          logInfo(
            s"Partition ${stats.partition}: performing subdoc mutations with ${storeSemantics}, " +
              s"specs for the first document: ${debugInfo}" +
              writeConfig.idempotency.fold("")(m =>
                s", plus idempotency '$m' with writeId '${writeConfig.writeId.get}'"
              )
          )
        }

        val opts = buildMutateInOptions(durability, storeSemantics, cas)

        writeConfig.idempotency match {
          case Some(_) =>
            val writeId = writeConfig.writeId.get
            val rowKey  = IdempotentWrites.rowKey(encoded)
            val op = () => {
              val allSpecs =
                IdempotentWrites.markerSpecs(writeId, rowKey, System.currentTimeMillis()) ++ specs
              val result: Try[WriteRetry.Outcome] =
                coll.mutateIn(id, allSpecs, opts).map(_ => WriteRetry.Applied)
              result.recoverWith {
                // The key is already in the document: this row has been applied before.
                case e if IdempotentWrites.isGuardRejection(e) => Success(WriteRetry.Deduplicated)
                // With insert semantics or CAS, a repeat fails before the guard is evaluated.  Only
                // now is it worth reading the document, to see whether it was this row that applied.
                case e @ (_: DocumentExistsException | _: CasMismatchException) =>
                  IdempotentWrites.resolveConflict(
                    e,
                    IdempotentWrites.markerPresent(
                      coll,
                      id,
                      IdempotentWrites.markerPath(writeId, rowKey),
                      timeout
                    )
                  )
              }
            }
            (op, true)

          case None =>
            // With CAS or insert semantics a repeat of an applied write fails (the CAS has moved,
            // or the document exists) rather than applying the specs again, so any specs are safe.
            val safe = cas.isDefined || storeSemantics == StoreSemantics.Insert ||
              SubdocUtil.isReplaySafe(decoded)
            (() => coll.mutateIn(id, specs, opts).map(_ => WriteRetry.Applied), safe)
        }
      }

      case _ =>
        val content = decoded.toString

        // With Idempotency, a conflict may just mean this row was already applied: by an earlier
        // attempt that failed ambiguously, an earlier Spark task attempt, or a re-run.  There is no
        // idempotency key to check, but the document then holds exactly this row's content.
        def checkingContent(op: () => Try[WriteRetry.Outcome]): () => Try[WriteRetry.Outcome] =
          if (writeConfig.idempotency.isEmpty) op
          else
            () =>
              op().recoverWith {
                case e @ (_: DocumentExistsException | _: CasMismatchException) =>
                  IdempotentWrites.resolveConflict(
                    e,
                    IdempotentWrites.contentMatches(coll, id, content, timeout)
                  )
              }

        (writeConfig.writeMode, mode) match {
          case (Some(KeyValueOptions.WriteModeReplace), _) =>
            val opts = buildReplaceOptions(durability, cas)
            // Without CAS a repeat writes the same content; with CAS it fails, as the CAS has moved.
            val op = () => coll.replace(id, content, opts).map(_ => WriteRetry.Applied)
            (if (cas.isDefined) checkingContent(op) else op, true)
          case (_, SaveMode.Overwrite) =>
            val opts = buildUpsertOptions(durability)
            (() => coll.upsert(id, content, opts).map(_ => WriteRetry.Applied), true)
          case (_, SaveMode.ErrorIfExists | SaveMode.Ignore) =>
            val opts = buildInsertOptions(durability)
            // A repeat fails with DocumentExistsException (which SaveMode.Ignore ignores).
            val op   = () => coll.insert(id, content, opts).map(_ => WriteRetry.Applied)
            (checkingContent(op), true)
          case _ =>
            throw new RuntimeException("Unexpected save mode " + mode)
        }
    }

    val opStarted = System.nanoTime()
    stats.recordConversion(opStarted - conversionStarted)

    def failure(errors: Seq[Throwable]) = WriteFailure(
      documentId = id,
      bucket = writeConfig.bucket,
      scope = writeConfig.scope.getOrElse(DefaultConstants.DefaultScopeName),
      collection = writeConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName),
      errors = errors,
      rowJson = encoded
    )

    // Execute the KV op here, with any retries.
    var attemptStarted = opStarted
    var retries        = 0
    val errors         = mutable.ArrayBuffer.empty[Throwable]
    val result = WriteRetry.run(
      writeConfig.retryAttempts,
      replaySafe,
      // The retry's own backoff, or the partition's if that is longer.
      sleep = d => backoff.pause(d),
      decide = failures.filter(_.decidesRetries) match {
        case None => (_, _, byDefault) => byDefault
        case Some(f) =>
          (retry, err, byDefault) => {
            val attempt = failure(errors.toSeq :+ err).copy(
              retriesRemaining = writeConfig.retryAttempts - retry + 1,
              retriedByDefault = byDefault
            )
            val decision = f.runner.attemptFailed(attempt)
            stats.recordRetryDecision(decision)
            decision match {
              case KeyValueWriteRetryDecision.Default => byDefault
              case KeyValueWriteRetryDecision.Retry   => true
              case KeyValueWriteRetryDecision.GiveUp  => false
              case KeyValueWriteRetryDecision.FailTask =>
                stats.recordFailure(id, err, " (the error handler is failing the task)")
                throw err
            }
          }
      },
      onRetry = (n, err) => {
        retries = n
        errors += err
        stats.recordRetry()
        logInfo(
          s"Partition ${stats.partition}: retrying write of document '$id' (retry $n of " +
            s"${writeConfig.retryAttempts}) after ${PartitionWriteStats.describe(err)}"
        )
      },
      // Once the partition has failed there's no point retrying, and doing so would delay the
      // failure by up to RetryAttempts operation timeouts. The row's failure so far is still
      // passed to the error handler.
      stopped = partitionFailed
    ) { n =>
      // Retries have already waited, in `sleep`.
      if (n == 0) backoff.pause()
      attemptStarted = System.nanoTime()
      val r = op()
      backoff.record(r)
      r
    }

    result match {
      case Success(WriteRetry.Applied)      => stats.recordSuccess(System.nanoTime() - attemptStarted)
      case Success(WriteRetry.Deduplicated) => stats.recordDeduplicated()
      case Failure(t) =>
        val note =
          if (retries > 0) s" (after $retries retries)"
          else if (
            writeConfig.retryAttempts > 0 && !replaySafe &&
            WriteRetry.classify(t) == WriteRetry.Ambiguous
          ) {
            " (not retried, as it may have been applied and its array or counter specs would " +
              "then be applied twice: see KeyValueOptions.Idempotency)"
          } else ""
        errors += t
        handleWriteError(failure(errors.toSeq), mode, stats, note, failures)
    }
  }

  private def handleWriteError(
      failure: WriteFailure,
      saveMode: SaveMode,
      stats: PartitionWriteStats,
      note: String,
      failures: Option[RelationPartitionWriter.Failures]
  ): Unit = {
    val documentId = failure.documentId
    val throwable  = failure.error
    // Special case for SaveMode.Ignore: silently ignore DocumentExistsException
    if (saveMode == SaveMode.Ignore && throwable.isInstanceOf[DocumentExistsException]) {
      stats.recordIgnored()
      logInfo(s"Document $documentId already exists, ignoring as per SaveMode.Ignore")
    } else {
      failures match {
        case Some(f) =>
          // On this writer thread, which waits for the handler: see FailureHandlerRunner.
          val decision =
            try f.runner.onFailure(failure)
            catch {
              case e: PartitionStoppedException =>
                // The handler is broken, so this failure is only logged and counted.
                stats.recordFailure(documentId, throwable, s"$note (not handled: ${e.getMessage})")
                throw e
            }
          decision match {
            case KeyValueWriteFailureDecision.Ignore => stats.recordIgnored()
            case KeyValueWriteFailureDecision.Continue =>
              stats.recordFailure(documentId, throwable, note)
            case KeyValueWriteFailureDecision.FailTask =>
              stats.recordFailure(
                documentId,
                throwable,
                s"$note (the error handler is failing the task)"
              )
              throw throwable
          }
        case None =>
          stats.recordFailure(documentId, throwable, note)
          throw throwable
      }
    }
  }

  def buildInsertOptions(durability: Durability): InsertOptions = {
    var opts = InsertOptions().transcoder(RawJsonTranscoder.Instance).durability(durability)
    writeConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

  def buildUpsertOptions(durability: Durability): UpsertOptions = {
    var opts = UpsertOptions().transcoder(RawJsonTranscoder.Instance).durability(durability)
    writeConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

  def buildReplaceOptions(durability: Durability, cas: Option[Long] = None): ReplaceOptions = {
    var opts = ReplaceOptions().transcoder(RawJsonTranscoder.Instance).durability(durability)
    cas.foreach(casValue => opts = opts.cas(casValue))
    writeConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

  def buildMutateInOptions(
      durability: Durability,
      storeSemantics: StoreSemantics,
      cas: Option[Long]
  ): MutateInOptions = {
    var opts = MutateInOptions().durability(durability).document(storeSemantics)
    cas.foreach(casValue => opts = opts.cas(casValue))
    writeConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

}

object RelationPartitionWriter extends Logging {

  /** The default for how many KV operations each partition - and so each Spark task - keeps
    * in-flight at once. Can be overridden with [[KeyValueOptions.MaxConcurrentOps]].
    */
  val DefaultMaxConcurrentOps: Int = 16

  /** A partition's error handling.
    *
    * @param decidesRetries
    *   whether the handler is called for retry decisions: see
    *   [[FailureHandlerRunner.decidesRetries]].
    */
  private[kv] case class Failures(runner: FailureHandlerRunner, decidesRetries: Boolean)

  /** Identifies which Spark task a log line came from, as "3 of 128".
    *
    * The count is taken from the TaskContext rather than passed down from the driver, as under AQE
    * the number of partitions actually executed can differ from the number the driver planned.
    */
  private[kv] def partitionLabel(ctx: TaskContext): String =
    if (ctx == null) "unknown" else s"${ctx.partitionId()} of ${ctx.numPartitions()}"

  /** SPARKC-240: Read and convert documents while we stream
    * Runs `process` on each item using `numThreads` threads, and returns how many items were
    * processed.
    *
    * `items` is only ever advanced on the calling thread - the Spark task thread. Spark's iterator
    * may drive upstream work (reading files, fetching shuffle blocks, ...) that expects the task's
    * thread-local TaskContext, so it must not be advanced from any other thread. Items are handed
    * to the threads through a small bounded queue, so only around `2 * numThreads` items are held
    * in memory at once.
    *
    * The first exception thrown by `process` (or by `items`) fails the whole write. No further
    * items are processed, those already being processed are allowed to finish, and then the
    * exception is rethrown.
    *
    * `failed` is set as soon as the write has failed (including if the task is killed), so that
    * items already being processed can stop early, e.g. rather than working through their retries.
    */
  private[kv] def writeAll[T <: AnyRef](
      items: Iterator[T],
      numThreads: Int,
      threadName: Int => String,
      stats: PartitionWriteStats,
      failed: AtomicBoolean = new AtomicBoolean(false)
  )(process: T => Unit): Long = {
    val partition  = stats.partition
    val queue      = new ArrayBlockingQueue[T](numThreads)
    val firstError = new AtomicReference[Throwable]()

    @volatile var producerDone = false

    def fail(e: Throwable, context: String): Unit =
      if (firstError.compareAndSet(null, e)) {
        failed.set(true)
        logError(
          s"Partition $partition: $context, so the write will fail and remaining items will be " +
            s"skipped: ${PartitionWriteStats.describe(e)}"
        )
      }

    val threads = mutable.ArrayBuffer.empty[Thread]
    var count   = 0L
    try {
      for (i <- 0 until numThreads) {
        val thread = new Thread(
          () => {
            var processed = 0L
            // A worker exits when no more work can arrive, which is triggered by producerDone (the incoming items
            // stream has been fully consumed), and the work queue being empty.
            while (!(producerDone && queue.isEmpty)) {
              val item = queue.poll(100, TimeUnit.MILLISECONDS)
              // After a failure keep draining the queue, so the producer cannot block on it,
              // but skip the work.
              if (item != null && firstError.get() == null) {
                try process(item)
                // The first thrown error triggers task failure as is the normal Spark model.  Spark by default will
                // retry the full task (partition) 4 times.  It is now recommended that users apply the failure handling to have better handling
                // of e.g. occasional KV timeouts:
                // https://docs.couchbase.com/spark-connector/current/spark-sql.html#failure-handling-during-persistence
                catch { case e: Throwable => fail(e, s"worker ${Thread.currentThread().getName} failed") }
                processed += 1
              }
            }
            logInfo(
              s"Partition $partition: worker ${Thread.currentThread().getName} finished after " +
                s"processing $processed items"
            )
          },
          threadName(i)
        )
        thread.setDaemon(true)
        thread.start()
        threads += thread
      }

      // hasNext() as well as next() is timed, as Spark iterators often do the upstream work in hasNext().
      var readStarted = System.nanoTime()
      while (firstError.get() == null && items.hasNext) {
        // Each call to .next() here is executing the upstream Spark code, e.g. it may block as it reads from file etc.
        // The aim is to keep the queue ahead of the workers by ~numThreads items, which should ensure a smooth flow.
        val item = items.next()
        stats.recordUpstreamRead(System.nanoTime() - readStarted, queue.size())
        queue.put(item)
        count += 1
        readStarted = System.nanoTime()
      }
    } catch {
      case e: Throwable => fail(e, "reading the next item from Spark failed")
    } finally {
      if (firstError.get() == null) {
        logInfo(
          s"Partition $partition: finished reading all $count items from Spark, " +
            s"${queue.size()} are still queued.  Waiting for ${threads.size} worker threads to finish"
        )
      } else {
        logInfo(
          s"Partition $partition: stopped reading from Spark after $count items due to an error.  " +
            s"Waiting for ${threads.size} worker threads to finish their in-flight items"
        )
      }

      producerDone = true
      val joinStarted    = System.nanoTime()
      var lastWaitingLog = joinStarted
      var interrupted    = false
      threads.foreach { thread =>
        while (thread.isAlive) {
          try thread.join(1000)
          // If Spark kills the task, it interrupts the task thread.  We handle that here and let the worker threads
          // finish cleanly first (recording the interrupt as the error makes them skip any remaining queued items).
          // We will re-interrupt after all workers are done. The while loop also ensures that we still join the
          // interrupted worker.
          catch {
            case e: InterruptedException =>
              interrupted = true
              fail(e, "interrupted while waiting for worker threads (likely the task was killed) - will continue")
          }

          val now = System.nanoTime()
          if (now - lastWaitingLog >= PartitionWriteStats.DefaultInterval.toNanos) {
            lastWaitingLog = now
            val alive = threads.filter(_.isAlive)
            if (alive.nonEmpty) {
              logInfo(
                s"Partition $partition: still waiting after ${(now - joinStarted) / 1000000}ms " +
                  s"for ${alive.size} worker threads: ${alive.map(_.getName).mkString(", ")}"
              )
            }
          }
        }
      }
      logInfo(
        s"Partition $partition: all ${threads.size} worker threads finished, after waiting " +
          s"${(System.nanoTime() - joinStarted) / 1000000}ms"
      )
      if (interrupted) Thread.currentThread().interrupt()
    }

    val e = firstError.get()
    if (e != null) throw e
    count
  }
}
