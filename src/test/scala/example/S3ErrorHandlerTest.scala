package example

import com.couchbase.client.scala.json.JsonObject
import com.couchbase.spark.kv.KeyValueOptions
import com.couchbase.spark.util.SparkOperationalSimpleTest
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.Files
import java.util.UUID

/** Exercises S3ErrorHandler, using a local directory in place of S3. */
class S3ErrorHandlerTest extends SparkOperationalSimpleTest {
  override def testName: String = "s3errorhandler"

  private lazy val base = Files.createTempDirectory(testName).toString

  private def write(df: DataFrame, dir: Option[String]): Unit = {
    var writer = df.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeSubdocReplace)
      .option(KeyValueOptions.RetryAttempts, "2")
      .option(KeyValueOptions.ErrorHandler, classOf[S3ErrorHandler].getName)
      .mode(SaveMode.Overwrite)
    dir.foreach(d => writer = writer.option("errorHandler.directory", d))
    writer.save()
  }

  private def seed(ids: Seq[String]): Unit = {
    val sp = spark
    import sp.implicits._
    ids
      .map(id => (id, "old"))
      .toDF("__META_ID", "name")
      .write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def replaceRows(ids: Seq[String]): DataFrame = {
    val sp = spark
    import sp.implicits._
    ids.map(id => (id, "new")).toDF("__META_ID", "replace:name")
  }

  @Test
  def `records only the rows that failed`(): Unit = {
    val sp = spark
    import sp.implicits._
    val existing = (1 to 3).map(_ => UUID.randomUUID().toString)
    val missing  = (1 to 50).map(_ => UUID.randomUUID().toString)
    seed(existing)
    val dir = s"file://$base/${UUID.randomUUID()}"
    // subdocReplace of a missing document fails with DocumentNotFoundException, not retried.
    write(replaceRows(existing ++ missing).repartition(4), Some(dir))

    val all = S3ErrorHandler.read(spark, dir)
    all.show(false)
    assertEquals(Set(S3ErrorHandler.Succeeded), all.select("outcome").as[String].collect().toSet)
    assertEquals(missing.size, all.count())

    val failed = S3ErrorHandler.notWritten(spark, dir)
    assertEquals(missing.toSet, failed.select("documentId").as[String].collect().toSet)
    assertTrue(failed.filter("mayHaveBeenWritten").isEmpty)
    assertTrue(failed.select("error").as[String].collect().forall(_.contains("DocumentNotFound")))
    val row = JsonObject.fromJson(failed.select("row").as[String].collect().head)
    assertEquals("new", row.str("replace:name"))
  }

  @Test
  def `no failures, no directory`(): Unit = {
    val existing = (1 to 3).map(_ => UUID.randomUUID().toString)
    seed(existing)
    val dir = s"$base/${UUID.randomUUID()}"
    write(replaceRows(existing), Some(s"file://$dir"))
    assertFalse(new java.io.File(dir).exists())
    assertTrue(S3ErrorHandler.read(spark, s"file://$dir").isEmpty)
    assertTrue(S3ErrorHandler.notWritten(spark, s"file://$dir").isEmpty)
  }

  @Test
  def `a failed task attempt's failures are recorded, as failed`(): Unit = {
    val sp = spark
    import sp.implicits._
    val missing = (1 to 20).map(_ => UUID.randomUUID().toString)
    val poison  = UUID.randomUUID().toString
    val dir     = s"file://$base/${UUID.randomUUID()}"
    // One partition, in order. Reading the last row fails the task. The rows before it that have
    // been started finish, and fail, as the documents don't exist. Those read but not yet started
    // are not written.
    val df = spark.sparkContext
      .parallelize(missing :+ poison, 1)
      .map { id =>
        if (id == poison) throw new RuntimeException("Failing the task")
        (id, "new")
      }
      .toDF("__META_ID", "replace:name")
    assertThrows(classOf[Exception], () => write(df, Some(dir)))

    val all = S3ErrorHandler.read(spark, dir)
    all.show(false)
    assertEquals(Set(S3ErrorHandler.Failed), all.select("outcome").as[String].collect().toSet)
    val ids = all.select("documentId").as[String].collect()
    assertTrue(ids.nonEmpty)
    assertTrue(ids.toSet.subsetOf(missing.toSet))
    assertEquals(ids.length, ids.distinct.length)
    // local[*] runs each task once.
    assertEquals(Set(0), all.select("taskAttempt").as[Int].collect().toSet)
    assertTrue(S3ErrorHandler.notWritten(spark, dir).isEmpty)
  }

  @Test
  def `missing directory option fails the write before anything is written`(): Unit = {
    val id = UUID.randomUUID().toString
    seed(Seq(id))
    assertThrows(classOf[Exception], () => write(replaceRows(Seq(id)), None))
    import com.couchbase.spark._
    import com.couchbase.spark.kv.Get
    val doc = spark.sparkContext
      .couchbaseGet(
        Seq(Get(id)),
        com.couchbase.spark.Keyspace(
          Some(testResources.bucketName),
          Some(testResources.scopeName),
          Some(testResources.collectionName)
        )
      )
      .collect()
      .head
      .contentAs[JsonObject]
      .get
    assertEquals("old", doc.str("name"))
  }

  @Test
  def `failing to record the failures fails the task`(): Unit = {
    assertThrows(
      classOf[Exception],
      () => write(replaceRows(Seq(UUID.randomUUID().toString)), Some("file:///proc/not-writable"))
    )
  }
}
