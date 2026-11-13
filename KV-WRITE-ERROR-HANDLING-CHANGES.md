# KV DataFrame writes: retries, idempotency and error handling

> **This is a throwaway file.** It's only here to explain the changes on this branch to
> reviewers, and should be dropped before merging. The scaladoc in `KeyValueOptions` and
> `KeyValueWriteFailureHandler` is the real documentation.

This covers what changes for someone writing a DataFrame to Couchbase with `format("couchbase.kv")`.
Everything new is opt-in: with none of the new options set, a write behaves as before, except
where noted under [Changes to existing behaviour](#changes-to-existing-behaviour).

## In short

- Failed writes can now be **retried** (`retryAttempts`), but only where that's safe.
- Writes can be made **idempotent** (`idempotency=enabled`), so a retry, or Spark re-running a
  task, doesn't apply a row twice. This matters for sub-document array and counter operations.
- A new error handler interface, **`KeyValueWriteFailureHandler`**, decides what happens to each
  failed row: ignore it, record it and carry on, or fail the Spark task. It can also decide
  whether each failed attempt is retried.
- **Error handling no longer drops failures.** Previously the error handler and the error bucket
  ran behind a bounded background queue that discarded failures once it was full, with only a
  warning in the executor log to show for it. Now every failure is handled, and if the handling
  can't keep up, the write slows down to match.

## Example

A sub-document write that retries failures, is safe to repeat, and records every row it couldn't
write to S3:

```scala
import com.couchbase.spark.kv.KeyValueOptions
import org.apache.spark.sql.SaveMode
import com.example.S3ErrorHandler // your copy of the example handler: see below

val errorDir = s"s3a://my-bucket/couchbase-errors/$runId" // a new directory for each write

df.write
  .format("couchbase.kv")
  .option(KeyValueOptions.Bucket, "my-bucket")
  .option(KeyValueOptions.Scope, "my-scope")
  .option(KeyValueOptions.Collection, "my-collection")
  .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeSubdocUpsert)
  .mode(SaveMode.Overwrite)
  // Retry failed writes up to 3 times, where that's safe.
  .option(KeyValueOptions.RetryAttempts, "3")
  // Make writes safe to repeat, so that a retry, or Spark re-running a task, doesn't apply a
  // row twice.
  .option(KeyValueOptions.Idempotency, KeyValueOptions.IdempotencyEnabled)
  // A custom error handler, which decides what happens to each row that still fails.
  .option(KeyValueOptions.ErrorHandler, classOf[S3ErrorHandler].getName)
  .option("errorHandler.directory", errorDir)
  // The handler uploads its file in close(), so allow longer than the default 2 minutes.
  .option(KeyValueOptions.ErrorHandlerTimeout, "10m")
  .save()

// Back on the driver: the rows that weren't written, with each row's data as JSON.
val failed = S3ErrorHandler.notWritten(spark, errorDir)
if (!failed.isEmpty) {
  throw new RuntimeException(s"${failed.count()} rows not written, see $errorDir")
}
```

**The S3 handler is example code, not part of the connector.** It isn't in the connector's jar.
To use it, copy
[`src/test/scala/example/S3ErrorHandler.scala`](src/test/scala/example/S3ErrorHandler.scala)
into your own application, put it in your own package, and adapt it as needed. Your application's
jar must be on the executors' classpath, as it is for `spark-submit`. The handler:

- Works with S3, HDFS or any Hadoop filesystem. S3 settings come from the `spark.hadoop.*`
  configuration, and `s3a://` needs the `hadoop-aws` jar.
- Writes one file per task attempt, under `outcome=succeeded/` or `outcome=failed/`.
- Provides `S3ErrorHandler.read`, which returns everything recorded, including failed attempts,
  and `S3ErrorHandler.notWritten`, which returns the rows to write again. `notWritten` can
  over-report but doesn't miss rows. Its scaladoc explains when.

## New options

| Option | Default | What it does |
|---|---|---|
| `retryAttempts` | `0` | How many times to retry a failed write, on top of the SDK's own retries. Waits between attempts, from 100ms doubling to 5s. |
| `idempotency` | `none` | `enabled` makes writes that aren't naturally safe to repeat safe to repeat. See below. |
| `writeId` | random | Advanced. Re-runs a failed job without re-applying sub-document writes it already made. Needs `idempotency=enabled`. |
| `errorHandlerTimeout` | `2m` | How long a single call to the error handler, or a write of an error document, may take before the Spark task fails. |
| `errorHandler.<name>` | | Any option for your error handler, e.g. `errorHandler.directory`. Your handler reads it with `context.option("directory")`. |

### `retryAttempts`

Only failures that are safe to retry are retried:

- **The server definitely didn't apply the write** (e.g. an unambiguous timeout, or a temporary
  failure): always retried.
- **The write may or may not have been applied** (e.g. an ambiguous timeout): retried unless
  repeating it would change the document twice. Only sub-document writes with `arrayAppend`,
  `arrayPrepend`, `arrayInsert`, `increment` or `decrement`, and without CAS, fall into this
  case. Turn on `idempotency` to have those retried too.
- **Anything else** (e.g. document not found): not retried.

A row that still fails is handled exactly as it would be without retries: it goes to the error
handler or error bucket if you configured one, and otherwise fails the Spark task.

### `idempotency=enabled`

- **Sub-document writes:** each row gets an idempotency key, stored in a small extended attribute
  (`cbspark`, under 100 bytes) on the document. A repeat of a write that was already applied is
  detected and skipped, across retries and across Spark re-running tasks and stages. It uses 2
  of the 16 specs a sub-document write allows, so at most 14 sub-document columns are allowed.
- **Full-document inserts, and replaces with CAS:** if repeating the write fails because the
  document exists or its CAS changed, the connector reads the document. If it already holds
  exactly what the row would write, the row counts as written, not failed. Nothing extra is
  stored.
- **Upserts, and replaces without CAS:** already safe to repeat, so nothing changes.

### `writeId`

Every write has an ID, which is logged on the driver. Spark's own task and stage retries share
it, so they need nothing extra. If a whole job fails and you re-submit it with the same input,
set `writeId` to the ID the failed job logged. Sub-document rows the failed job already applied
are then skipped, not applied twice. Every row is still written again: this doesn't resume from
where the job stopped.

Most users won't need this. With an error handler, individual write failures don't fail the job
anyway.

## The new error handler: `KeyValueWriteFailureHandler`

Set it with `.option(KeyValueOptions.ErrorHandler, "com.example.MyHandler")`. The older
`KeyValueWriteErrorHandler` still works, and always carries on after a failure.

```scala
class MyHandler extends KeyValueWriteFailureHandler {
  override def open(context: KeyValueWriteContext): Unit = ()  // once per task attempt

  // Optional: called after each failed attempt, while retryAttempts allows another.
  override def onAttemptFailed(a: KeyValueWriteFailedAttempt): KeyValueWriteRetryDecision =
    KeyValueWriteRetryDecision.Default  // or Retry, GiveUp, FailTask

  // Called for each row that could not be written.
  override def onFailure(f: KeyValueWriteFailure): KeyValueWriteFailureDecision =
    KeyValueWriteFailureDecision.Continue  // or Ignore, FailTask

  override def close(error: Option[Throwable]): Unit = ()  // always called, once
}
```

**What a failure includes:** the document ID and keyspace, the error from every attempt,
whether the write may have been applied anyway (`mayHaveBeenApplied`), and the original row as
JSON (`rowJson`), so the row can be written again later.

**What the handler can decide**

| Decision | Effect |
|---|---|
| `Ignore` | Expected, not an error. Counted as ignored. |
| `Continue` | Handled, e.g. recorded somewhere. Counted as failed, and the write carries on. If an error bucket is also set, an error document is written. |
| `FailTask` | Fails the Spark task with this row's error. |

From `onAttemptFailed`:

| Decision | Effect |
|---|---|
| `Default` | Retry if the connector would. |
| `Retry` | Retry even if the connector wouldn't. |
| `GiveUp` | Stop retrying and pass the row to `onFailure`. |
| `FailTask` | Fail the Spark task now. |

**How it runs**

- **One copy per task attempt.** Each Spark task attempt (one per partition, plus any retries)
  gets its own copy of the handler on an executor. What it records in memory isn't visible on
  the driver, so write results somewhere durable.
- **One call at a time.** The handler is called one call at a time, on a thread of its own, so it
  doesn't need to be thread-safe. `TaskContext.get()` isn't available there: use the
  `KeyValueWriteContext` passed to `open` instead.
- **Writes wait for each call.** A slow handler slows the write down. That's deliberate: when
  writes are failing the cluster may be struggling, and error handling shouldn't add to the load.
- **Calls are timed out.** A call that takes longer than `errorHandlerTimeout` fails the task.
  This includes `close`, so allow for anything slow you do there, such as uploading a file.
- **A handler that throws, returns null or times out** fails the task, and isn't called again
  apart from `close`.

**When the task is failing**, whether because the handler said `FailTask` or for some other
reason such as bad input:

- No new rows are started, and retries stop.
- Rows already being written finish, and any that fail still go to `onFailure`.
- Rows that hadn't started aren't passed to the handler, since they weren't written.
- Then `close` is called with the error, and the task fails.

Spark's next attempt at the task writes every row of the partition again. So treat what a failed
attempt recorded as incomplete and possibly out of date.

For a full handler to copy, see the S3 handler in the [Example](#example) section.

## Changes to existing behaviour

- **`ErrorBucket` and the older `KeyValueWriteErrorHandler` are now synchronous.** Previously they
  ran behind a background queue that dropped failures when it filled up, logging a warning on
  the executor. Now the write waits for
  each one, so a slow handler or error bucket slows the write instead of losing failures. Each
  call is subject to `errorHandlerTimeout`.
- **`TaskContext.get()` returns null inside error handlers**, since they now run on their own
  thread.
- **`ErrorBucket` is checked before the write starts.** If the bucket, scope or collection is
  missing, or the user can't write to it, the write fails before writing anything. A bucket that
  can't be opened is waited for first, for up to `spark.couchbase.waitUntilReadyTimeout`.
- **Error documents are retried.** An error document that can't be written is retried, backing
  off up to 5s, until `errorHandlerTimeout` has nearly passed. Only then does the task fail.
  Previously such a failure was lost.
- **Writes slow down when the cluster is struggling.** When retries or error handling are on,
  failures that suggest the cluster is under strain (timeouts, temporary failures) make every
  writer in the task pause before its next write, until writes succeed again. This applies
  whether or not the next write is a retry. Without retries or error handling, the first failure
  fails the task anyway, as before.
- **The per-partition stats log line** now also shows error handler call times, any retry
  decisions the handler made, and time spent paused by the slowdown above.

## Status

- **Tested** against Couchbase Server 8.0, with the KV write regression tests and the new unit
  and integration tests.
- **Not yet tested**: Server 7.6, other Spark versions, real S3, and running as a user with only
  the Data Writer role.
