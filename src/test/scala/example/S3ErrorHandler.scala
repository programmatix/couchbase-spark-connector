package example

import com.couchbase.client.scala.json.JsonObject
import com.couchbase.spark.kv.{
  KeyValueWriteContext,
  KeyValueWriteFailure,
  KeyValueWriteFailureDecision,
  KeyValueWriteFailureHandler
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.io.{BufferedWriter, File}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import scala.util.control.NonFatal

/** A KeyValueWriteFailureHandler that records the rows that could not be written in a directory on
  * any Hadoop filesystem (S3, HDFS...), so the driver can check them once the write has finished.
  *
  * Note this is an example handler and users are encouraged to copy it into their application and
  * adapt to their needs.
  *
  * Usage, on the driver:
  * {{{
  * val errorDir = s"s3a://my-bucket/couchbase-errors/$runId" // a new directory for each write
  * df.write
  *   .format("couchbase.kv")
  *   // ... other options
  *   .option(KeyValueOptions.RetryAttempts, "3")
  *   .option(KeyValueOptions.ErrorHandler, "example.S3ErrorHandler")
  *   .option("errorHandler.directory", errorDir)
  *   // The upload is made in close(), so allow for a large file.
  *   .option(KeyValueOptions.ErrorHandlerTimeout, "10m")
  *   .save()
  *
  * val failed = S3ErrorHandler.notWritten(spark, errorDir)
  * if (!failed.isEmpty) throw new RuntimeException(s"${failed.count()} rows not written, see $errorDir")
  * }}}
  *
  * How it works:
  *   - Each Spark task attempt gets its own copy of the handler. Failures are appended to a local
  *     temporary file on the executor (this is faster than sending rows immediately to the remote
  *     destination). This is synchronous: the connector's writer waits while each one is appended.
  *   - At the end of the task attempt, the file is uploaded to the directory, under
  *     `outcome=succeeded/` or `outcome=failed/` depending on whether the attempt succeeded. When
  *     the directory is read, this is the `outcome` column.
  *   - If the upload fails for an attempt that succeeded, the task fails, so Spark retries it
  *     rather than the failures being lost. For an attempt that failed, the upload is best effort:
  *     a failure to upload is logged, so as not to hide the task's own error. An attempt that does
  *     not reach the end, such as when its executor is lost, uploads nothing.
  *   - The directory is only created if there were failures.
  *
  * Reading the directory:
  *   - [[S3ErrorHandler.read]] gives every recorded failure, from every attempt. A failed attempt's
  *     failures show what was going wrong, including the row that failed the task if the handler
  *     was the cause. But they are incomplete, as rows not yet started when the attempt failed are
  *     not tried, and out of date: Spark runs the task again, writing every row of the partition
  *     again.
  *   - [[S3ErrorHandler.notWritten]] gives the rows to write again: the failures from attempts
  *     that succeeded, one per document. It can include rows that were in fact written: where
  *     `mayHaveBeenWritten` is set (e.g. timeouts), or where Spark ran a partition twice, because
  *     its task failed after the upload or with speculative execution (`spark.speculation`).
  *
  * Each record has the row as JSON, so failed rows can be re-written from it. Note that this copies
  * the row's data to the directory.
  *
  * S3 settings are taken from `spark.hadoop.*` configuration. Changes made to
  * `sparkContext.hadoopConfiguration` in the application are not seen on the executors.
  */
class S3ErrorHandler extends KeyValueWriteFailureHandler {
  import S3ErrorHandler._

  // Set on the executor, in open().
  @transient private var context: KeyValueWriteContext = _
  @transient private var dir: String                   = _
  @transient private var file: File                    = _
  @transient private var out: BufferedWriter           = _
  @transient private var count: Int                    = 0

  override def open(context: KeyValueWriteContext): Unit = {
    this.context = context
    // Throwing here fails the task before anything is written.
    dir = context
      .option("directory")
      .getOrElse(
        throw new IllegalArgumentException("""Set .option("errorHandler.directory", ...)""")
      )
  }

  override def onFailure(f: KeyValueWriteFailure): KeyValueWriteFailureDecision = {
    if (out == null) {
      file = Files.createTempFile("couchbase-errors-", ".jsonl").toFile
      out = Files.newBufferedWriter(file.toPath, StandardCharsets.UTF_8)
    }
    out.write(
      JsonObject(
        "documentId"         -> f.documentId,
        "keyspace"           -> s"${f.bucket}.${f.scope}.${f.collection}",
        "error"              -> f.error.getClass.getName,
        "message"            -> String.valueOf(f.error.getMessage).take(2000),
        "attempts"           -> f.errors.size,
        "mayHaveBeenWritten" -> f.mayHaveBeenApplied,
        "row"                -> f.rowJson,
        "partition"          -> context.partitionId,
        "stageAttempt"       -> context.stageAttemptNumber,
        "taskAttempt"        -> context.taskAttemptNumber,
        "time"               -> Instant.now().toString
      ).toString
    )
    out.newLine()
    count += 1
    KeyValueWriteFailureDecision.Continue
  }

  override def close(error: Option[Throwable]): Unit = {
    if (out != null) {
      try {
        out.close()
        val outcome = if (error.isDefined) Failed else Succeeded
        val dest = new Path(
          dir,
          s"outcome=$outcome/stage-${context.stageId}-partition-${context.partitionId}" +
            s"-attempt-${context.stageAttemptNumber}-${context.taskAttemptNumber}.jsonl"
        )
        def upload(): Unit =
          dest.getFileSystem(hadoopConf).copyFromLocalFile(false, true, new Path(file.toURI), dest)
        if (error.isEmpty) {
          // Throws if it can't be uploaded, which fails the task.
          upload()
          log.warn(s"$count rows could not be written to Couchbase: recorded in $dest")
        } else {
          try {
            upload()
            log.warn(s"Task attempt failed, its $count write failures are recorded in $dest")
          } catch {
            // The task is failing anyway: don't hide its error.
            case NonFatal(e) =>
              log.error(s"Task attempt failed, and its $count write failures could not be " +
                s"recorded in $dest: $e")
          }
        }
      } finally file.delete()
    }
  }
}

object S3ErrorHandler {
  private val log = LoggerFactory.getLogger(classOf[S3ErrorHandler])

  val Schema: StructType = StructType(
    Seq(
      StructField("documentId", StringType),
      StructField("keyspace", StringType),
      StructField("error", StringType),
      StructField("message", StringType),
      StructField("attempts", IntegerType),
      StructField("mayHaveBeenWritten", BooleanType),
      StructField("row", StringType),
      StructField("partition", IntegerType),
      StructField("stageAttempt", IntegerType),
      StructField("taskAttempt", IntegerType),
      StructField("time", StringType),
      // From the directory each file is in.
      StructField("outcome", StringType)
    )
  )

  /** The `outcome` of a task attempt that succeeded. */
  val Succeeded = "succeeded"

  /** The `outcome` of a task attempt that failed. */
  val Failed = "failed"

  /** Every recorded failure, from every task attempt, on the driver. Empty if there were none. */
  def read(spark: SparkSession, dir: String): DataFrame = {
    val path = new Path(dir)
    if (path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path)) {
      spark.read.schema(Schema).json(dir)
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schema)
    }
  }

  /** The rows to write again, on the driver: the failures from task attempts that succeeded, one
    * per document. Empty if there were none. See the class docs for when this includes rows that
    * were in fact written.
    */
  def notWritten(spark: SparkSession, dir: String): DataFrame =
    read(spark, dir)
      .filter(col("outcome") === Succeeded)
      .dropDuplicates("keyspace", "documentId")

  /** On the executor: the `spark.hadoop.*` settings, as Spark itself uses for S3 and HDFS. */
  private lazy val hadoopConf: Configuration = {
    val conf = new Configuration()
    SparkEnv.get.conf.getAll.foreach { case (k, v) =>
      if (k.startsWith("spark.hadoop.")) conf.set(k.stripPrefix("spark.hadoop."), v)
    }
    conf
  }
}
