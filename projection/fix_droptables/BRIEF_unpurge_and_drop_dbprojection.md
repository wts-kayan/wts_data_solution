# BRIEF — Disable `external.table.purge` then drop all Hive tables in `dbprojection`

**Target agent:** Claude Code
**Author context:** STCreditRisk_STE / BNP Paribas ITG — Cloudera CDP 7.1.9
**Status:** to implement
**Danger level:** HIGH — irreversible metastore destruction; data loss if step order is violated

---

## 1. Objective

Drop the **partitioned Hive tables** in the `dbprojection` database **without deleting the underlying HDFS data**.

**Scope restriction — partitioned tables only.** Of the 85 tables in `dbprojection`, only those that
declare at least one partition column are in scope. Non-partitioned tables are **left untouched**: no
`ALTER`, no `DROP`. The partitioned subset is **not known in advance** and must be determined at runtime
(see §4.2); the 85-table list in §9 is the full inventory, not the drop list.

Because these tables were created as `TRANSLATED_TO_EXTERNAL` with `external.table.purge=TRUE`, a plain
`DROP TABLE` **deletes the HDFS directory as well**. The purge flag must therefore be flipped to `FALSE`
on every table **before** any `DROP` is issued.

Required sequence, per **in-scope (partitioned)** table, in this exact order:

1. `ALTER TABLE dbprojection.<t> SET TBLPROPERTIES ('external.table.purge'='false');`
2. verify the property actually reads back as `false`
3. `DROP TABLE IF EXISTS dbprojection.<t>;`
4. verify the HDFS directory still exists

**No table may be dropped until its own purge flag has been verified as `false`.** Do not batch all
ALTERs and then batch all DROPs blindly — verify per table, and skip (do not drop) any table whose
verification fails.

---

## 2. Environment

| Item | Value |
|---|---|
| Platform | Cloudera CDP 7.1.9 |
| HDFS nameservice | `hahdfsnameservice` |
| Warehouse path | `/warehouse/tablespace/external/hive/dbprojection.db/` (confirm at runtime) |
| Spark | 3.5.4 / Scala 2.12 |
| Execution env | Dataiku DSS, Spark-Scala notebook kernel |
| Shell / CLI | **NOT AVAILABLE** — no `hdfs dfs`, no `beeline`, no `hive` CLI |
| Filesystem API | Hadoop `FileSystem` Java API only |
| SQL execution | `spark.sql(...)` from the Dataiku kernel, or Hue SQL editor for the generated `.sql` |
| Partition provider | `catalog` (explicit DDL required; `MSCK REPAIR` is forbidden) |

---

## 3. Hard constraints

- **Never use `MSCK REPAIR TABLE`.** These are Spark datasource tables with `partitionProvider=catalog`.
- **Never hardcode a UUID or an HDFS path read from a screenshot.** Resolve every location at runtime via
  `DESCRIBE FORMATTED` / `SHOW CREATE TABLE` or the `FileSystem` API.
- **`dry_run` defaults to `True`.** In dry-run mode the script prints every statement it *would* run and
  performs read-only introspection, but issues no `ALTER` and no `DROP`.
- **Idempotent.** Re-running after a partial failure must be safe: already-altered tables are detected and
  skipped, already-dropped tables are no-ops (`DROP TABLE IF EXISTS`).
- **Never `DROP DATABASE ... CASCADE`.** Tables are dropped one by one so a single failure cannot cascade.
- Preserve the confirmed upstream misspelling `used_worfklow` verbatim anywhere it appears — do not "fix" it.
- Property name is lowercase `external.table.purge`; value is the lowercase string `'false'`.
- **Non-partitioned tables are out of scope and must never be altered or dropped.** Partitioning status is
  determined at runtime, never assumed from the table name — `_detailed`, `_uat`, `tmp1` and similar
  suffixes carry no information about whether a table is partitioned.

---

## 4. Pre-flight (mandatory, runs even in dry-run)

Before touching anything, produce a full backup of the metastore definitions and a classification report.

### 4.1 DDL backup
For **all 85 tables** — in scope or not — capture `SHOW CREATE TABLE dbprojection.<t>` and write the
concatenated output to `dbprojection_ddl_backup.sql`. The backup is deliberately wider than the drop
scope: it is cheap, and it is the **only** way to recreate the tables afterwards.
**Abort the whole run if this file cannot be written or is missing any table.**

### 4.2 Classification report
For every table, record via `DESCRIBE FORMATTED`:

| Field | Purpose |
|---|---|
| table name | key |
| table type | `EXTERNAL_TABLE` / `MANAGED_TABLE` |
| `TRANSLATED_TO_EXTERNAL` | present / absent |
| `external.table.purge` | current value (`TRUE` / `FALSE` / absent) |
| `Location` | HDFS path (resolved at runtime) |
| provider / `spark.sql.sources.provider` | datasource vs native Hive |
| **partitioned** | **yes / no — drives the scope filter** |
| **partition columns** | **ordered list, e.g. `runid`** |
| **partition count** | **from `SHOW PARTITIONS`, for the record** |

### Partition detection

Do **not** rely on parsing the `# Partition Information` block out of `DESCRIBE FORMATTED` text output,
and do **not** rely on `SHOW PARTITIONS` alone (it throws on non-partitioned tables and returns an empty
set for a partitioned table that has no partitions yet — two different situations that must not be
conflated). Use the catalog metadata directly:

```scala
val meta = spark.sessionState.catalog.getTableMetadata(TableIdentifier("<t>", Some("dbprojection")))
val isPartitioned = meta.partitionColumnNames.nonEmpty
```

This works uniformly for native Hive tables and for Spark datasource tables with
`partitionProvider=catalog`. A partitioned table with zero registered partitions is still **in scope**.

Classify each table into one of:

- **MISSING** — not found in the metastore → no-op.
- **NOT_PARTITIONED** — no partition columns → **out of scope**. Log at INFO, count it in the summary,
  and move on. This check runs **first**, before any purge or table-type evaluation, so that an
  out-of-scope table is never even considered for an `ALTER`.
- **MANAGED** — partitioned but genuinely `MANAGED_TABLE` → `external.table.purge` does **not** apply;
  dropping will delete the data regardless. **Do not drop. Report and stop for these tables**, list them
  in the summary and let a human decide.
- **PURGE_TRUE** — partitioned + external + purge true → needs the ALTER, then drop. The normal case.
- **PURGE_FALSE** — partitioned + external + purge already false/absent → skip ALTER, drop directly.

Write the report to `dbprojection_preflight_report.csv` (semicolon-delimited, UTF-8 BOM, for French Excel).

### 4.3 Location capture
Persist the resolved HDFS location of every table to `dbprojection_locations.txt` **before** dropping,
so the data can be re-registered later. Once the table is dropped, the metastore no longer knows the path.

---

## 5. Execution phase

For each table in the list, in order:

```
if class == MISSING         -> skip, log INFO, continue
if class == NOT_PARTITIONED -> skip, log INFO, continue          <-- scope gate, evaluated first
if class == MANAGED         -> skip, log WARN, continue
if class == PURGE_TRUE      -> ALTER TABLE ... SET TBLPROPERTIES ('external.table.purge'='false')
re-read external.table.purge
if value != 'false'        -> skip DROP, log ERROR, continue
capture HDFS location + assert path exists via FileSystem API
DROP TABLE IF EXISTS dbprojection.<t>
assert HDFS path STILL exists via FileSystem API
if path vanished           -> log CRITICAL, abort the entire run immediately
```

The post-drop existence assertion is the safety net: if the very first table loses its directory, the
run stops before damaging the other 84.

---

## 6. Deliverables

Write all artifacts to `/mnt/user-data/outputs/`.

| File | Content |
|---|---|
| `unpurge_and_drop_dbprojection.scala` | Primary implementation, Spark-Scala for the Dataiku kernel. `dryRun: Boolean = true` as the first config val. |
| `unpurge_and_drop_dbprojection.py` | PySpark equivalent, same behaviour and same flags. |
| `dbprojection_alter_purge.sql` | `ALTER TABLE ... SET TBLPROPERTIES` statements for the **partitioned subset only**, generated at runtime from the pre-flight classification — not hand-written against the 85-table list. |
| `dbprojection_drop_tables.sql` | `DROP TABLE IF EXISTS` statements for the **same subset, in the same order**. Header comment stating it must never be run before the ALTER script has completed and been verified. |
| `dbprojection_out_of_scope.txt` | The non-partitioned tables that were deliberately skipped, so the exclusion is auditable. |
| `dbprojection_ddl_backup.sql` | Generated at runtime by the pre-flight — not hand-written. |
| `dbprojection_preflight_report.csv` | Generated at runtime. |
| `README.md` | Run order, rollback story, and the explicit warning about MANAGED tables. |

Each script carries a header comment block: purpose, environment, danger warning, run order, and the
`dry_run` default.

---

## 7. Acceptance criteria

- Running with `dryRun = true` mutates nothing and prints the full plan plus the classification report.
- The DDL backup is complete before any mutation.
- Not a single `DROP` is emitted for a table whose purge flag did not read back as `false`.
- No `MANAGED_TABLE` is dropped.
- **No non-partitioned table is altered or dropped.** After a real run, every table classified
  `NOT_PARTITIONED` still exists in the metastore with its `external.table.purge` property unchanged.
- The summary prints the counts: total inspected / partitioned / dropped / skipped-not-partitioned /
  skipped-managed / failed, and they add up to 85.
- After a real run, every HDFS directory under `dbprojection.db/` still exists and is byte-identical in
  file count.
- The script is re-runnable end to end with no errors.

---

## 8. Open points to confirm before the real run

- **Is the intent to recreate these tables afterwards?** If yes, the `SHOW CREATE TABLE` backup is the
  critical artifact and should be reviewed by a human before any drop.
- `term_structure` and `term_structure_detailed` carry the known double-nested
  `runId=<uuid>/runid=<uuid>/` partition defect. Decide whether to flatten first, or drop-and-recreate
  clean. Dropping does not fix the on-disk layout.
- **Review the computed partitioned subset before the real run.** The scope is derived from the catalog,
  so the exact drop list is only known after the first dry-run. Have a human sign off on
  `dbprojection_preflight_report.csv` before switching `dryRun` to `false`.
- Confirm the account running this has `ALTER`/`DROP` privileges on the in-scope tables (Ranger policy).
- Confirm no Oozie workflow is running against `dbprojection` during the operation.

---

## 9. Full inventory — `dbprojection` (85 tables)

> **This is the inventory to inspect, not the list to drop.** The drop list is the partitioned subset of
> the tables below, computed at runtime per §4.2. Treat every entry here as a candidate whose partitioning
> status is unknown until the catalog says otherwise.

Source: `SHOW TABLES IN dbprojection` (screenshot transcription, alphabetical, header row `tab_name` excluded).
**The script must re-derive this list at runtime** via `spark.sql("SHOW TABLES IN dbprojection")` and
**diff it against this list**, reporting any addition or removal rather than trusting the hardcoded copy.

1. `chr`
2. `chr_detailed`
3. `chr_detailed_mta`
4. `chr_idealised_detailed`
5. `chr_uat`
6. `ene_c29`
7. `ene_c29_and_borne`
8. `ene_c29_diff`
9. `ene_no_secto`
10. `ene_no_secto_borne`
11. `ene_no_secto_pivot`
12. `exceptions_npl`
13. `lgd`
14. `lgd_old`
15. `lgd_old_arr_03052024`
16. `lgd_term_structure_detailed`
17. `lgd_term_structure_detailed_old`
18. `lgd_term_structure_detailed_old_arr_03052024`
19. `migration_matrix`
20. `migration_matrix_21q2_npl`
21. `migration_matrix_22q3`
22. `migration_matrix_detailed`
23. `migration_matrix_detailed_2021_recette`
24. `migration_matrix_detailed_22q3`
25. `migration_matrix_npl`
26. `migration_matrix_npl_detail`
27. `migration_matrix_npl_exception`
28. `migration_matrix_npl_uat`
29. `migration_matrix_uat`
30. `model_cr_exceptions`
31. `model_drz_exceptions`
32. `model_drz_exceptions_2021_recette`
33. `model_migration_matrix_exceptions`
34. `model_term_structure_exceptions`
35. `model_term_structure_exceptions_2023_03_21`
36. `nka_scenarii_ponderation`
37. `npl_migration_matrix`
38. `npl_migration_matrix_bis`
39. `npl_migration_matrix_detailed`
40. `pcure`
41. `pcure_hlc_old`
42. `pcure_old_arr_03052024`
43. `projected_cr_detailed`
44. `projected_cr_detailed_old`
45. `projected_cr_detailed_old_arr_03052024`
46. `projected_dr`
47. `projected_dr_21q2_npl`
48. `projected_dr_22q3`
49. `projected_dr_detailed`
50. `projected_dr_detailed_2021_recette`
51. `projected_dr_detailed_22q3`
52. `projected_dr_uat`
53. `projected_z`
54. `projected_z_21q2_npl`
55. `projected_z_22q3`
56. `projected_z_detailed`
57. `projected_z_detailed_2021_recette`
58. `projected_z_detailed_22q3`
59. `projected_z_lgd_detailed`
60. `projected_z_uat`
61. `run_history`
62. `run_projection`
63. `run_projection_2021_2_7`
64. `run_projection_2021_recette`
65. `run_projection_21q2`
66. `run_projection_22q3`
67. `scenarii_ponderation`
68. `scenarii_ponderation_2021_recette`
69. `tabrbhconcurrent`
70. `term_structure`
71. `term_structure_21q2_npl`
72. `term_structure_22q3`
73. `term_structure_detailed`
74. `term_structure_detailed2`
75. `term_structure_detailed_2021_recette`
76. `term_structure_detailed_22q3`
77. `term_structure_exceptions_2021_recette`
78. `term_structure_idealised`
79. `term_structure_idealised_detailed`
80. `term_structure_uat`
81. `test2137_20154`
82. `tightening_corp`
83. `tightening_retail`
84. `tmp1`
85. `tmp2`
-e 
---

*Transcribed from two IDE screenshots (lines 2–86 of a `SHOW TABLES` dump). Line 1 was the `tab_name` header and is not a table. Sort order and absence of duplicates were verified.*
