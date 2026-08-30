# Unpurge and drop the partitioned tables of `dbprojection`

Implements `BRIEF_unpurge_and_drop_dbprojection.md`.

**Danger level: HIGH.** This drops Hive tables. The HDFS data is meant to
survive, and every guard exists to make sure it does, but a dropped table
*definition* is gone — the only way back is the DDL backup the script writes
before it touches anything.

## What it does

Drops the **partitioned** tables of `dbprojection` **without** deleting their
HDFS data.

The tables were created as `TRANSLATED_TO_EXTERNAL` with
`external.table.purge=true`. With that property set, `DROP TABLE` deletes the
data directory even though the table is EXTERNAL. So per table, in this order:

1. `ALTER TABLE ... SET TBLPROPERTIES ('external.table.purge'='false')`
2. re-read the property and verify it really is `false`
3. `DROP TABLE IF EXISTS ...`
4. verify the HDFS directory is **still there**

No table is dropped until its own flag has been verified. The ALTERs are never
batched ahead of the DROPs — each table walks the four steps alone, so a table
whose verification fails is skipped rather than dropped.

## Run order

```
1. DRY_RUN = true   (the default)   -> writes every artefact, changes nothing
2. a HUMAN reviews dbprojection_preflight_report.csv, above all the IN-SCOPE list
3. DRY_RUN = false                  -> executes
```

Step 2 is not a formality. The drop list is **derived from the catalog at
runtime** and is only known after the first pass; it is not the 85-table
inventory in the brief.

Paste the cell into the existing Dataiku Spark-Scala notebook. It reuses the
`spark` / `sparkContext` vals bound by cell 1 — do not re-create the session.

## Scope: partitioned tables only

Only tables declaring at least one partition column are in scope.
Non-partitioned tables are **never** altered and never dropped. Partitioning is
read from the catalog:

```scala
spark.sessionState.catalog.getTableMetadata(TableIdentifier(t, Some(DB)))
     .partitionColumnNames.nonEmpty
```

not by parsing `DESCRIBE FORMATTED` text and not from `SHOW PARTITIONS`, which
throws on a non-partitioned table and returns empty for a partitioned table
with no partitions yet — two situations that must not be conflated. A
partitioned table with **zero** partitions is still in scope.

Table names carry no information: `_detailed`, `_uat`, `tmp1` say nothing about
whether a table is partitioned. The scope gate is evaluated **first**, before
purge or table type, so an out-of-scope table is never even a candidate for an
`ALTER`.

| Class | Meaning | Action |
|---|---|---|
| `MISSING` | not in the metastore | no-op |
| `NOT_PARTITIONED` | no partition column | **out of scope, never touched** |
| `MANAGED` | partitioned but `MANAGED_TABLE` | **never dropped** — see below |
| `PURGE_TRUE` | partitioned + external + purge true | ALTER, then drop |
| `PURGE_FALSE` | partitioned + external + purge already false | drop directly |

## MANAGED tables — read this

`external.table.purge` does not apply to a MANAGED table. `DROP TABLE` deletes
its data **regardless** of any property. The script therefore **never** drops
one: it reports them, lists them in the summary, and leaves the decision to a
human. If a table you expected to drop shows up as MANAGED, that is a finding,
not an obstacle to work around.

## Artefacts

All written to `OUTPUT_DIR` before anything is modified, in dry-run too.

| File | Content |
|---|---|
| `dbprojection_ddl_backup.sql` | `SHOW CREATE TABLE` for **every** table, in scope or not. The only way back. |
| `dbprojection_preflight_report.csv` | The classification, semicolon-delimited with a UTF-8 BOM for French Excel. **This is the file to review.** |
| `dbprojection_locations.txt` | Every table's HDFS location, captured before any drop. |
| `dbprojection_alter_purge.sql` | `ALTER` statements for the partitioned subset, generated at runtime. |
| `dbprojection_drop_tables.sql` | `DROP` statements for the same subset, in the same order. |
| `dbprojection_out_of_scope.txt` | The non-partitioned tables skipped, so the exclusion is auditable. |

The two `.sql` files exist for the Hue route — the notebook does not need them.
They are generated from the same in-scope list the execution loop uses, so they
cannot drift from each other.

## Rollback

There is no undo for a `DROP`. The recovery path is:

1. `dbprojection_ddl_backup.sql` — replay the `CREATE` for the table you want back.
2. `dbprojection_locations.txt` — the `LOCATION` it must point at.
3. Re-register the partitions. **Never `MSCK REPAIR TABLE`**: these are
   datasource tables with `partitionProvider=catalog`, so partitions must be
   re-added with explicit `ALTER TABLE ... ADD PARTITION ... LOCATION`.

Which is why the data directory surviving is asserted after **every** drop: if
one vanishes, the run aborts immediately rather than working through the rest.

## Safety guards

Each of these stops the run or skips the table rather than guessing:

- the DDL backup must be **complete** — a missing table or an unwritable file
  aborts before anything is altered, in dry-run too, so a dry run cannot
  green-light a real run that has no rope;
- the purge flag is re-read from the metastore per table, and a table that does
  not read back as `false` is **not** dropped;
- the location must exist before the drop, or the table is not dropped;
- after the drop the location must **still** exist, or the whole run aborts;
- never `DROP DATABASE ... CASCADE` — tables go one at a time;
- never `MSCK REPAIR TABLE`;
- no UUID and no HDFS path is hardcoded; every location is resolved at runtime.

Re-running is safe: an already-altered table classifies as `PURGE_FALSE` and
skips its ALTER, an already-dropped one reads as `MISSING`.

## Before the real run

From the brief's open points, still to confirm:

- **Is the intent to recreate these tables afterwards?** If so, a human must
  review `dbprojection_ddl_backup.sql` before any drop.
- `term_structure` and `term_structure_detailed` carry the double-nested
  `runId=<uuid>/runid=<uuid>/` defect. Dropping does **not** fix the on-disk
  layout — decide whether to flatten first (see `../fix_runid/`) or
  drop-and-recreate clean.
- The account needs `ALTER`/`DROP` on the in-scope tables (Ranger).
- No Oozie workflow may run against `dbprojection` during the operation.

## Tests

The cell is compiled and run offline by the harness in
`../fix_runid/scala-test`, against a real local Spark with Hive support:

```bash
cd ../fix_runid/scala-test && mvn -o test
```

Covered: dry run mutates nothing; a non-partitioned table is never altered nor
dropped; a MANAGED table is reported and never dropped; the purge flag is
flipped and verified **before** the drop and the data directory survives; a
table already at `false` is dropped without an ALTER; the artefacts name every
table on the right side; a second run is a clean no-op; an unwritable DDL
backup aborts before any change.

Not covered, and worth knowing: the harness runs against a local derby
metastore, so it proves the *script's* logic and ordering, not CDP's
`TRANSLATED_TO_EXTERNAL` purge semantics. That behaviour is exactly what the
post-drop existence assertion is there to catch on the real cluster.

## Not built

`unpurge_and_drop_dbprojection.py`, the PySpark twin listed in the brief's
deliverables. The Scala cell was the one asked for; the Python one has no test
coverage yet and is not written rather than written blind.
