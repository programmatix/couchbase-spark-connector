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

import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.LookupInSpec
import com.couchbase.spark.Keyspace
import com.couchbase.spark.util.SparkOperationalSimpleTest
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeAll, Test}

import java.util.UUID

/** KeyValueOptions.Idempotency and RetryAttempts against a real cluster. */
class KeyValueIdempotencyIntegrationTest extends SparkOperationalSimpleTest {
  private val TestCollection = "idempotency_test"

  override def testName: String = "idempotency"

  @BeforeAll
  def setUpCollection(): Unit = {
    testResourceCreator.createCollection(
      testResources.bucketName,
      testResources.scopeName,
      Some(TestCollection)
    )
  }

  private def keyspace = Keyspace(
    bucket = Some(testResources.bucketName),
    scope = Some(testResources.scopeName),
    collection = Some(TestCollection)
  )

  /** Unique per test, as all tests share the collection. */
  private def newId(): String = UUID.randomUUID().toString

  private def write(df: DataFrame, mode: SaveMode, options: (String, String)*): Unit = {
    var writer = df.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, TestCollection)
      .mode(mode)
    options.foreach { case (k, v) => writer = writer.option(k, v) }
    writer.save()
  }

  /** Creates the docs, each with an empty `items` array. */
  private def seed(ids: String*): Unit = {
    val sp = spark
    import sp.implicits._
    write(ids.map(id => (id, Seq.empty[String])).toDF("__META_ID", "items"), SaveMode.Overwrite)
  }

  /** Appends `value` to the `items` array of each document. */
  private def appendRows(rows: (String, String)*): DataFrame = {
    val sp = spark
    import sp.implicits._
    rows.toDF("__META_ID", "arrayappend:items")
  }

  private def appendIdempotently(df: DataFrame, writeId: String): Unit =
    write(
      df,
      SaveMode.Overwrite,
      KeyValueOptions.WriteMode   -> KeyValueOptions.WriteModeSubdocUpsert,
      KeyValueOptions.Idempotency -> KeyValueOptions.IdempotencyEnabled,
      KeyValueOptions.WriteId     -> writeId
    )

  private def doc(id: String): JsonObject = {
    import com.couchbase.spark._
    spark.sparkContext
      .couchbaseGet(Seq(Get(id)), keyspace)
      .collect()
      .head
      .contentAs[JsonObject]
      .get
  }

  private def items(id: String): Seq[String] =
    doc(id).arr("items").toSeq.map(_.toString)

  private def marker(id: String): JsonObject = {
    import com.couchbase.spark._
    spark.sparkContext
      .couchbaseLookupIn(Seq(LookupIn(id, Seq(LookupInSpec.get("cbspark").xattr))), keyspace)
      .collect()
      .head
      .contentAs[JsonObject](0)
      .get
  }

  @Test
  def `repeating a write with the same writeId does not re-apply it`(): Unit = {
    val (doc1, doc2) = (newId(), newId())
    seed(doc1, doc2)
    val df = appendRows(doc1 -> "a", doc2 -> "b")

    appendIdempotently(df, "w1")
    assertEquals(Seq("a"), items(doc1))
    assertEquals(Seq("b"), items(doc2))

    // As Spark retrying the task would: every row has already been applied, so none are again.
    appendIdempotently(df, "w1")
    assertEquals(Seq("a"), items(doc1))
    assertEquals(Seq("b"), items(doc2))

    // A different write is applied.
    appendIdempotently(df, "w2")
    assertEquals(Seq("a", "a"), items(doc1))
    assertEquals(Seq("b", "b"), items(doc2))
  }

  @Test
  def `without idempotency, repeating a write re-applies it`(): Unit = {
    val id = newId()
    seed(id)
    val df      = appendRows(id -> "a")
    val options = Seq(KeyValueOptions.WriteMode -> KeyValueOptions.WriteModeSubdocUpsert)
    write(df, SaveMode.Overwrite, options: _*)
    write(df, SaveMode.Overwrite, options: _*)
    assertEquals(Seq("a", "a"), items(id))
  }

  @Test
  def `only the latest key is kept, outside the document body`(): Unit = {
    val id = newId()
    seed(id)
    appendIdempotently(appendRows(id -> "a"), "w1")
    appendIdempotently(appendRows(id -> "a"), "w2")

    val m = marker(id)
    assertEquals(Set("w2"), m.names)
    assertEquals(1, m.obj("w2").names.size)
    assertFalse(doc(id).containsKey("cbspark"))
  }

  private def cas(id: String): Long = {
    import com.couchbase.spark._
    spark.sparkContext.couchbaseGet(Seq(Get(id)), keyspace).collect().head.cas
  }

  /** A row with a mix of types, so that matching content also checks the stored document compares
    * equal to what was written.
    */
  private def person(id: String, name: String): DataFrame = {
    val sp = spark
    import sp.implicits._
    Seq((id, name, 42, 1.5, Seq("a", "b"), true))
      .toDF("__META_ID", "name", "age", "score", "tags", "active")
  }

  private val idempotent = KeyValueOptions.Idempotency -> KeyValueOptions.IdempotencyEnabled

  @Test
  def `CAS replace repeated after it was applied: deduplicated if the content matches`(): Unit = {
    val id = newId()
    write(person(id, "old"), SaveMode.Overwrite)
    val replace =
      person(id, "new").withColumn("__META_CAS", org.apache.spark.sql.functions.lit(cas(id)))
    val options = Seq(
      KeyValueOptions.WriteMode    -> KeyValueOptions.WriteModeReplace,
      KeyValueOptions.CasFieldName -> "__META_CAS"
    )
    write(replace, SaveMode.Overwrite, options :+ idempotent: _*)
    assertEquals("new", doc(id).str("name"))

    // As a retry would, after the first attempt was applied (or a Spark task retry): the CAS is
    // now stale.
    assertThrows(classOf[Exception], () => write(replace, SaveMode.Overwrite, options: _*))
    write(replace, SaveMode.Overwrite, options :+ idempotent: _*)

    // Someone else has changed it since: a genuine conflict.
    write(person(id, "other"), SaveMode.Overwrite)
    assertThrows(
      classOf[Exception],
      () => write(replace, SaveMode.Overwrite, options :+ idempotent: _*)
    )
    assertEquals("other", doc(id).str("name"))
  }

  @Test
  def `insert repeated after it was applied: deduplicated if the content matches`(): Unit = {
    val id = newId()
    write(person(id, "Alice"), SaveMode.ErrorIfExists, idempotent)

    assertThrows(classOf[Exception], () => write(person(id, "Alice"), SaveMode.ErrorIfExists))
    write(person(id, "Alice"), SaveMode.ErrorIfExists, idempotent)

    assertThrows(
      classOf[Exception],
      () => write(person(id, "Bob"), SaveMode.ErrorIfExists, idempotent)
    )
    assertEquals("Alice", doc(id).str("name"))
  }

  @Test
  def `full-document writes store nothing extra`(): Unit = {
    val sp = spark
    import sp.implicits._
    val id = newId()
    write(
      Seq((id, "Alice")).toDF("__META_ID", "name"),
      SaveMode.Overwrite,
      KeyValueOptions.Idempotency   -> KeyValueOptions.IdempotencyEnabled,
      KeyValueOptions.RetryAttempts -> "2"
    )
    assertEquals("Alice", doc(id).str("name"))
    import com.couchbase.spark._
    val hasXattr = spark.sparkContext
      .couchbaseLookupIn(Seq(LookupIn(id, Seq(LookupInSpec.exists("cbspark").xattr))), keyspace)
      .map(_.exists(0)) // on the executor, as a result with a missing path isn't serializable
      .collect()
      .head
    assertFalse(hasXattr)
  }

  @Test
  def `subdocInsert: repeating with the same writeId is deduplicated, not an error`(): Unit = {
    val sp = spark
    import sp.implicits._
    val id = newId()
    val df = Seq((id, "Alice")).toDF("__META_ID", "upsert:name")
    val options = Seq(
      KeyValueOptions.WriteMode   -> KeyValueOptions.WriteModeSubdocInsert,
      KeyValueOptions.Idempotency -> KeyValueOptions.IdempotencyEnabled,
      KeyValueOptions.WriteId     -> "w1"
    )
    write(df, SaveMode.Overwrite, options: _*)
    // The document now exists, so this fails before the guard; the connector checks the key.
    write(df, SaveMode.Overwrite, options: _*)
    assertEquals("Alice", doc(id).str("name"))
  }

  @Test
  def `a user insert spec that fails is still reported, not mistaken for a repeat`(): Unit = {
    val sp = spark
    import sp.implicits._
    val id = newId()
    write(Seq((id, "original")).toDF("__META_ID", "name"), SaveMode.Overwrite)

    // The user's insert spec fails with PathExistsException, like the guard would - but it is not
    // the guard, so it's a genuine failure.
    val df = Seq((id, "new")).toDF("__META_ID", "insert:name")
    assertThrows(
      classOf[Exception],
      () =>
        write(
          df,
          SaveMode.Overwrite,
          KeyValueOptions.WriteMode     -> KeyValueOptions.WriteModeSubdocUpsert,
          KeyValueOptions.Idempotency   -> KeyValueOptions.IdempotencyEnabled,
          KeyValueOptions.RetryAttempts -> "2"
        )
    )
    assertEquals("original", doc(id).str("name"))
  }

  @Test
  def `retries do not change a successful write`(): Unit = {
    val id = newId()
    seed(id)
    write(
      appendRows(id -> "a"),
      SaveMode.Overwrite,
      KeyValueOptions.WriteMode     -> KeyValueOptions.WriteModeSubdocUpsert,
      KeyValueOptions.RetryAttempts -> "3"
    )
    assertEquals(Seq("a"), items(id))
  }

  @Test
  def `invalid options are rejected before writing`(): Unit = {
    val id = newId()
    val df = appendRows(id -> "a")
    def rejected(options: (String, String)*): Unit = {
      val ex = assertThrows(
        classOf[IllegalArgumentException],
        () => write(df, SaveMode.Overwrite, options: _*)
      )
      assertTrue(ex.getMessage.contains("KeyValueOptions"), ex.getMessage)
    }

    rejected(
      KeyValueOptions.WriteMode   -> KeyValueOptions.WriteModeSubdocUpsert,
      KeyValueOptions.Idempotency -> "bogus"
    )
    rejected(
      KeyValueOptions.WriteMode -> KeyValueOptions.WriteModeSubdocUpsert,
      KeyValueOptions.WriteId   -> "w1"
    )
    rejected(
      KeyValueOptions.WriteMode   -> KeyValueOptions.WriteModeSubdocUpsert,
      KeyValueOptions.Idempotency -> KeyValueOptions.IdempotencyEnabled,
      KeyValueOptions.WriteId     -> "not.valid"
    )
    rejected(KeyValueOptions.RetryAttempts -> "-1")

    // Nothing was written.
    import com.couchbase.spark._
    val found = scala.util.Try(spark.sparkContext.couchbaseGet(Seq(Get(id)), keyspace).collect())
    assertTrue(found.isFailure || found.get.isEmpty, found.toString)
  }
}
