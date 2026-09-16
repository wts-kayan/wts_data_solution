# bug_projection_001 — `NoSuchElementException: None.get` at the run-history write

**Module:** `str_projection_engine` (7.8.2-RELEASE)
**Entry point:** `com.bnpp.itg.fresh.str.runners.impl.RunProjection.main`
**Failing call:** `RunHistorizer.write` → `DataFrameWriter.saveAsTable`
**Status:** Proximate cause established, `RunHistorizer` reviewed — root cause pending `RunProjection.java` and the driver log (§9)
**Date:** 2026-09-14 — updated 2026-09-15 (`RunHistorizer.java` review, bytecode checks)
**Evidence:**
- [`stacktrace.txt`](stacktrace.txt) (72 lines, verbatim)
- [`RunHistorizer.java`](RunHistorizer.java) — partial reconstruction from IDE screenshots (imports and the tail of `convertDuration` missing)
- `spark-sql_2.12-3.5.4.jar` from the local Maven repository, read with `javap -c -l`

---

## 1. Summary

The Spark application now starts and runs — this is **not** a recurrence of the
Oozie `spark3` sharelib failure recorded in
[`../upgrade_spark/`](../upgrade_spark/). The job reaches `RunProjection.java:69`
and fails while appending to the run-history table.

`BasicWriteJobStatsTracker.metrics` calls `SparkContext.getActive.get` to build
the SQL metrics for a write. `getActive` holds the **global active
SparkContext** and is cleared when that context is stopped. `None.get` therefore
means one thing and only one thing:

> **The SparkContext was already stopped when `RunHistorizer.write` called
> `saveAsTable`.**

Nothing is wrong with the DataFrame, the target table, the schema, or the write
path — the table exists and the append path was taken (§4). Spark is attaching
metrics to a context that no longer exists.

`RunHistorizer.historize` takes a `hasTechnicalFailed` flag, so it is designed
to run on the failure path. That makes the masking scenario (§6, case 2) the
more likely of the two. The driver log still decides it.

---

## 2. Environment

| | Value |
|---|---|
| Spark | 3.5.4 (post-upgrade from 3.3.2) |
| Scala | 2.12 (`Option.scala:529`; `scala.collection.JavaConversions` in `RunHistorizer`) |
| Deploy mode | YARN cluster (`ApplicationMaster$$anon$2.run` on the stack) |
| JDK | 8 (`sun.reflect.NativeMethodAccessorImpl`, `Method.invoke`) |
| Launcher | Oozie `spark3` action, sharelib repaired |
| Driver log | **not yet collected** — see §8 |

---

## 3. The decisive frames

From `stacktrace.txt`, reading bottom-up:

```
 71  org.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:748)
 66  com.bnpp.itg.fresh.str.runners.impl.RunProjection.main(RunProjection.java:69)
 65  com.bnpp.itg.fresh.str.runHistorizer.RunHistorizer.historize(RunHistorizer.java:101)
 64  com.bnpp.itg.fresh.str.runHistorizer.RunHistorizer.write(RunHistorizer.java:124)
 61  org.apache.spark.sql.DataFrameWriter.saveAsTable(DataFrameWriter.scala:696)
 34  …CreateDataSourceTableAsSelectCommand.run(createDataSourceTables.scala:170)
 33  …CreateDataSourceTableAsSelectCommand.saveDataIntoTable(createDataSourceTables.scala:232)
 32  …DataSource.writeAndRead(DataSource.scala:513)
  5  …DataWritingCommand.metrics(DataWritingCommand.scala:55)
  4  …BasicWriteJobStatsTracker$.metrics(BasicWriteStatsTracker.scala:239)
  2  scala.None$.get(Option.scala:529)
```

`BasicWriteStatsTracker.scala:239` in Spark 3.5.4, checked against the bytecode
of `spark-sql_2.12-3.5.4.jar`:

```scala
object BasicWriteJobStatsTracker {
  …
  def metrics: Map[String, SQLMetric] = {
    val sparkContext = SparkContext.getActive.get          // ← line 239 (bytecode 0-12)
    Map(
      NUM_FILES_KEY        -> SQLMetrics.createMetric(sparkContext, "number of written files"),
      NUM_OUTPUT_BYTES_KEY -> SQLMetrics.createSizeMetric(sparkContext, "written output"),
      NUM_OUTPUT_ROWS_KEY  -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      NUM_PARTS_KEY        -> SQLMetrics.createMetric(sparkContext, "number of dynamic part"),
      TASK_COMMIT_TIME     -> SQLMetrics.createTimingMetric(sparkContext, "task commit time"),
      JOB_COMMIT_TIME      -> SQLMetrics.createTimingMetric(sparkContext, "job commit time")
    )
  }
}
```

> **B-01 — resolved.** The `LineNumberTable` of `BasicWriteJobStatsTracker$.metrics()`
> maps line 239 to bytecode 0: `SparkContext$.getActive()` → `Option.get()`. That
> matches the trace exactly. The jar checked is the Maven Central artifact, not
> the deployed parcel. The trace's line numbers agree with it, so treat them as
> the same build.

---

## 4. What the stack proves, and what it rules out

**Proven — `main` is still running.** `RunProjection.main` sits directly under
the ApplicationMaster's user-class thread. This is the normal execution path,
not a JVM shutdown hook.

**Ruled out — shutdown-hook ordering.** The common variant of this failure is a
cleanup task registered as a shutdown hook running *after* Spark's own hook has
stopped the context. That is not what happened here: `main` had not returned.

**Therefore the context was stopped while `main` was still executing**, which
leaves exactly two candidates. See §6.

**Proven — the run-history table existed (A-03, corrected).** The first version
of this analysis read the `CreateDataSourceTableAsSelectCommand` frames as "the
table did not exist". That was wrong. `RunHistorizer.write` calls
`saveAsTable` with `SaveMode.Append`, and in Spark 3.x `saveAsTable` routes an
append to an existing V1 table through `CreateDataSourceTableAsSelectCommand`
as well. The command itself branches on `catalog.tableExists`.

The bytecode settles which branch was taken. In `CreateDataSourceTableAsSelectCommand.run`,
`createDataSourceTables.scala:170` is bytecode 289-311:

```
158: SessionCatalog.tableExists(...)
161: ifeq 314                                   // table missing → jump to the create branch (lines 172-191)
     …                                          // mode checks: Overwrite assert, ErrorIfExists, Ignore
304: getstatic SaveMode.Append
307: iconst_1                                   // tableExists = true
308: invokespecial saveDataIntoTable(...)       // ← line 170, the frame in the trace
```

The create branch calls `saveDataIntoTable(..., SaveMode.Overwrite, false)` at
bytecode 449, which maps to lines 182-191, not 170. The write was a normal
append to an existing table.

---

## 5. What `RunHistorizer.java` shows

Source: [`RunHistorizer.java`](RunHistorizer.java). Line numbers below are
those of the reconstruction; see A-04 for how they relate to the trace.

### 5.1 The session is passed in, not looked up (answers C-02)

```java
public void historize(String usedConf, String usedWorkflow, Timestamp startingDate,
                      SparkSession sc, boolean hasTechnicalFailed)
```

`historize` receives the `SparkSession` from `RunProjection.main`. It never calls
`SparkSession.builder().getOrCreate()`, and `write` is `static` and uses the
Dataset's own session. There is one session in play. If its context was stopped
before the call, nothing in this class could have revived or replaced it.

### 5.2 `hasTechnicalFailed` puts `historize` on the failure path (strengthens A-01)

```java
if (hasTechnicalFailed) {
    modelRun.setStatus(CommonConstants.STATUS_FAILED);
} else {
    modelRun.setStatus(CommonConstants.STATUS_SUCCEEDED);
}
```

The method exists to record failed runs as well as successful ones. So
`RunProjection.java:69` is very likely inside a `catch` or `finally`. That is
exactly the shape in which case 2 happens:

1. The processing throws; something stops the context (a `spark.stop()` in a
   `finally`, or Spark stopping itself after a fatal error).
2. The `catch` / `finally` calls `historize(..., true)`.
3. `saveAsTable` throws `None.get`, which **replaces the original exception**
   in the YARN diagnostics.

This is not proof — `RunProjection.java` is still needed (C-01) — but it moves
case 2 from "possible" to "the design invites it".

### 5.3 Why the failure surfaces only at `saveAsTable`

Everything `historize` does before `write` still works on a stopped context:

| Call | Checks for a stopped context? |
|---|---|
| `sc.sparkContext().hadoopConfiguration()` | No — returns a field |
| `sparkUser()`, `applicationId()`, `startTime()`, `isLocal()` | No — return fields |
| `sc.createDataset(list, Encoders.bean(...))` | No — builds a `LocalRelation` |
| `ds.select(...)`, `withColumnRenamed(...)` | No — analysis only |
| `ds.write()…saveAsTable(...)` | **Yes, indirectly** — `BasicWriteJobStatsTracker.metrics` → `SparkContext.getActive.get` |

The first thing that needs the active context is write-metrics setup. So a guard
belongs at the **top of `historize`**, not inside `write` (§7).

### 5.4 Write path (confirms §4)

```java
ds.write().format("orc").option("delimiter", ";").mode(SaveMode.Append)
  .option("header", "false").option("compression", "ZLIB").saveAsTable(tableName);
```

`Append` + `saveAsTable` on an existing table: consistent with the bytecode
reading in §4. `delimiter` and `header` are CSV options and are ignored by the
ORC writer — harmless, noted only so nobody chases them.

---

## 6. The two candidate root causes

### Case 1 — `main` stopped the context itself, before line 69

The classic shape:

```java
try {
    process(props);
} finally {
    spark.stop();                              // ← context dies here
}
runHistorizer.historize(...);                  // line 69 — writes to a dead context
```

…or the mirror image, with `historize` inside a `finally` that runs after a
`stop()` in the `try`. An ordinary lifecycle-ordering bug. Fix is §7.

### Case 2 — something else stopped it, and this trace is masking the real failure

An earlier fatal error, driver OOM, or the AM starting to unregister stops the
context; `historize(..., hasTechnicalFailed = true)` then runs as failure
bookkeeping, hits the dead context, and its `None.get` **propagates in place of
the original exception**.

> **⚠ A-01 — masking risk.** In case 2 the exception in `stacktrace.txt` is the
> symptom of the cleanup path, not the cause of the run failing. Chasing
> `None.get` would be chasing the wrong defect. Settle §8 before any code
> change. §5.2 makes this the more likely case.

The two cases are distinguished by the driver log alone, in about a minute.

---

## 7. Fix

Worth making whichever case §8 returns: the ordering fix makes case 1 go away,
and the guard makes case 2 legible.

**1. `RunProjection` — historize before stopping, and never let it throw.**
`RunHistorizer` is a Spring `@Component` with an instance method, so the call is
on the injected bean:

```java
boolean failed = false;
try {
    process(props);
} catch (Exception e) {
    failed = true;
    throw e;                                            // the primary exception stays primary
} finally {
    try {
        runHistorizer.historize(usedConf, usedWorkflow, startingDate, spark, failed);
    } catch (Exception h) {
        LOGGER.error("run historization failed", h);    // log; never rethrow over the primary exception
    }
    spark.stop();                                       // last, after the history write
}
```

**2. `RunHistorizer.historize` — guard at the top.** If Spark stopped the
context on its own (case 2), no ordering fix can make the write succeed. The
guard turns `None.get` into a message that names the problem, and does not
throw:

```java
public void historize(String usedConf, String usedWorkflow, Timestamp startingDate,
                      SparkSession sc, boolean hasTechnicalFailed) throws IOException, URISyntaxException {
    if (!activeRunHistory) {
        return;
    }
    if (sc.sparkContext().isStopped()) {
        LOGGER.error("SparkContext already stopped - run history not written to " + historyTableName
                + " (hasTechnicalFailed=" + hasTechnicalFailed + ")");
        return;
    }
    …
```

> **A-05 — consequence.** With the guard, a run whose context Spark stopped
> itself produces **no** row in the history table. That is still better than
> today, where it produces no row *and* hides the cause. If failed runs must
> always be recorded, that needs a write path that does not depend on Spark
> (out of scope here).

---

## 8. Diagnostic procedure — run this first

```bash
yarn logs -applicationId application_<app-id> -appOwner <user> > driver.log

grep -nE "Invoking stop\(\) from shutdown hook|SparkContext is stopping|Stopped Spark web UI|Shutting down all executors|STARTING HISTORIZATION|ERROR" driver.log | head -40
```

`STARTING HISTORIZATION ON :` is logged by `historize` itself, just before it
builds the record. It pins where the history call sits relative to the stop.

Read the result as follows:

| What the log shows | Conclusion |
|---|---|
| `SparkContext is stopping` / `Stopped Spark web UI`, **no ERROR above it**, then `STARTING HISTORIZATION` | **Case 1** — lifecycle ordering. Apply §7 and the run should complete. |
| An `ERROR` **above** the stop line, then `STARTING HISTORIZATION` | **Case 2** — that ERROR is the real root cause. `None.get` is noise. Investigate the ERROR; still apply §7 so it stops being hidden. |

Record the outcome against C-03 in §11 before proceeding.

---

## 9. Inputs still needed

1. **`RunProjection.java`, lines ~40-80** — everything around line 69:
   any `spark.stop()`, `sparkContext().stop()`, `System.exit()`, `try/catch/finally`,
   and what value is passed as `hasTechnicalFailed`.
2. **The driver log** per §8.
3. *(Optional — resolves A-04)* the line table of the deployed class:
   ```bash
   javap -cp str_projection_engine-7.8.2-RELEASE.jar -l \
     com.bnpp.itg.fresh.str.runHistorizer.RunHistorizer | grep -A30 "historize("
   ```

With 1 and 2, the case is decidable.

~~`RunHistorizer.java`, lines ~90-130~~ — received, see §5.

---

## 10. Relationship to the Spark 3.3.2 → 3.5.4 upgrade

Undetermined, and the distinction matters.

The job never reached `RunHistorizer` while the Oozie sharelib was broken
([`../upgrade_spark/`](../upgrade_spark/)), so the simplest explanation is that
this code path is only now being exercised on 3.5.4 — a latent ordering bug that
was always there.

The alternative is a behavioural change in session/context lifecycle between
3.3.2 and 3.5.4. To separate them, answer one question: **did this same jar
version run end-to-end on 3.3.2?**

- Yes → something changed in the upgrade; worth pinning down before merging a fix.
- No, the last green run used an older jar → far more likely an ordering bug in
  `RunProjection`, and §7 is the whole story.

Nothing in `RunHistorizer.java` is version-sensitive in a way that would produce
this error: the session handling and the `saveAsTable` append are the same in
3.3 and 3.5.

---

## 11. Anomaly register

| Code | Type | Location | Description |
|---|---|---|---|
| A-01 | Masking risk | `RunProjection.main` | If the context was stopped by an earlier failure, this `None.get` replaces the original exception in the YARN diagnostics. Must be excluded via §8 before treating it as the root cause. `hasTechnicalFailed` (§5.2) shows `historize` is meant to run on the failure path, which makes this the more likely case |
| A-02 | Design | `RunHistorizer.historize` | End-of-run bookkeeping that throws will mask any primary exception. Should never propagate — §7 |
| A-03 | ~~Observation~~ **Corrected** | `RunHistorizer.write:124` | First read as "the run-history table did not exist". Wrong: `createDataSourceTables.scala:170` is `saveDataIntoTable(…, SaveMode.Append, tableExists = true)` (bytecode 304-308). The table existed; the write was a normal append (§4) |
| A-04 | Source drift | `RunHistorizer.java` | Reconstruction vs trace: the `write(...)` call is line 76 vs 101 (+25), `saveAsTable` is line 95 vs 124 (+29). A collapsed import block would shift both equally; the 4-line difference means blank lines were lost in transcription, or the screenshot is not byte-identical to 7.8.2-RELEASE. Call chain matches, conclusions unaffected. Resolve with §9.3 if exact lines matter |
| A-05 | Consequence | §7 guard | With the guard in place, a run whose context was stopped by Spark writes no history row. Accepted trade-off vs masking the cause |
| B-01 | ~~Low-confidence read~~ **Resolved** | Spark source | `BasicWriteStatsTracker.scala:239` confirmed against `spark-sql_2.12-3.5.4.jar` bytecode: line 239 = `SparkContext.getActive.get`. §3 snippet corrected to the 6 metrics actually built |
| C-01 | Pending | `RunProjection.java:40-80` | Not yet available — see §9.1 |
| C-02 | **Resolved** | `RunHistorizer.java` | Received (§5). Session is a parameter, not `getOrCreate`; guard location identified |
| C-03 | Pending | driver log | Not yet collected — see §8. Decides case 1 vs case 2 |
