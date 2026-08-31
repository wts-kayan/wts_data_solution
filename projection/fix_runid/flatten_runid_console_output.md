# Console output — `runId` flatten/rename job (migration_matrix_detailed)

Verbatim transcription from a screenshot of the notebook console.
Timestamps all `[11:36:22]`. Table: `dbprojection.db/migration_matrix_detailed`.

---

## 1. Tail of the source cell visible above the output

```scala
    else
      println(s">>> APPLIED to ${targets.length} table(s). Re-run with DRY_RUN = true: it " +
          "must report 0 to rename and every table already at " + PARTITION_COL + ".")
```

---

## 2. Console output (verbatim)

```text
jection.db/migration_matrix_detailed/runid=b664d5ce-db76-4da5-9dce-a2fe9f762ebe -> hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runId=b664d5ce-db76-4da5-9dce-a2fe9f762ebe
[11:36:22] PLAN      RENAME hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runid=d5f18121-6596-4a94-baf4-f880def1f330 -> hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runId=d5f18121-6596-4a94-baf4-f880def1f330  (82 file(s), 309.05 KB)
[11:36:22] PLAN      RENAME hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runid=e806ad86-15af-4194-b634-6e08d7d46d4a -> hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runId=e806ad86-15af-4194-b634-6e08d7d46d4a  (82 file(s), 309.10 KB)
[11:36:22] PLAN      RENAME hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runid=f1f1f29e-08d4-4377-bdb9-e75402e116d6 -> hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runId=f1f1f29e-08d4-4377-bdb9-e75402e116d6  (82 file(s), 309.10 KB)
[11:36:22] PLAN      RENAME hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runid=f63a0463-3eb6-4883-b3fb-ffd8e3551e53 -> hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runId=f63a0463-3eb6-4883-b3fb-ffd8e3551e53  (85 file(s), 311.48 KB)
[11:36:22] WARN      SKIP   hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runId=01e06330-c2f1-4418-b1cb-b40af98fc0a8 : still holds a nested 'runid=01e06330-c2f1-4418-b1cb-b40af98fc0a8' dir after phase F - either phase F refused it (a different run id, or a protected dir) or DO_FLATTEN is off. Renaming the wrapper would only move the nesting under a new name
[11:36:22] WARN      SKIP   hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/migration_matrix_detailed/runId=09d71b86-53ad-4ee3-a667-bf855b09cecf : still holds a nested 'runid=09d71b86-53ad-4ee3-a667-bf855b09cec
```

Cell prompt at the bottom of the screen: `n [0]:` (i.e. `In [0]:`), so the transcribed block is the *previous* cell's output.

---

## 3. What the output says

- **PLAN / RENAME** lines: dry-run plan to rename lowercase `runid=<uuid>` wrappers to canonical `runId=<uuid>`, with file count and size per partition.
- **WARN / SKIP** lines: partitions already named `runId=<uuid>` that **still contain a nested `runid=<uuid>` subdirectory** after phase F. The job refuses to rename because that would only relocate the nesting under a new name. Stated causes: phase F refused the directory (different run id, or protected dir), or `DO_FLATTEN` is off.

---

## 4. Flags / caveats for whoever consumes this file

1. **UUIDs are OCR-derived from a photo of a screen — do NOT hardcode them.** Re-derive every partition path at runtime from `hdfs dfs -ls` or the Hadoop `FileSystem` API. Characters at risk: `1`/`l`, `0`/`O`, `b`/`6`, `f`/`ff`.
2. **First line is truncated on the left.** It begins mid-path at `jection.db/...`; the missing prefix is `[11:36:22] PLAN      RENAME hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbpro`. It is also missing its trailing `(N file(s), X KB)` counter, which was cut off by the viewport.
3. **Last line is truncated on the right.** The nested dir name reads `runid=09d71b86-53ad-4ee3-a667-bf855b09cec` but the wrapper on the same line ends `...cecf`, so the final `f'` and the rest of the sentence were clipped. Assume the nested name equals the wrapper name.
4. **Column spacing between `PLAN`/`WARN`, `RENAME`/`SKIP` and the path is approximate** — the log appears to use fixed-width padding, exact widths not recoverable from the image.
5. `PARTITION_COL` in the source snippet is a variable, presumably holding `"runId"`. Its definition is not visible in the screenshot.
6. The `else` branch shown is the tail of an `if (DRY_RUN) ... else ...`; the `if` branch is scrolled off.
7. Scope in this screenshot is `migration_matrix_detailed` only — not `term_structure` / `term_structure_detailed`.
