// =====================================================================
//  unpurge_and_drop_dbprojection.scala      (Spark-Scala -- drop-in cell)
//
//  ###################################################################
//  #  DANGER -- THIS SCRIPT DESTROYS METASTORE DEFINITIONS.          #
//  #  It DROPs Hive tables. The HDFS data is meant to survive, and   #
//  #  every guard below exists to make sure it does, but a dropped   #
//  #  table definition is gone: the only way back is the DDL backup  #
//  #  this script writes BEFORE it touches anything.                 #
//  ###################################################################
//
//  README -- HOW TO RUN
//  --------------------
//  Paste as a cell of the existing Dataiku Spark-Scala notebook. It REUSES
//  the `spark` / `sparkContext` vals bound by cell 1 -- do NOT re-create
//  the SparkSession.
//
//    1. Run with DRY_RUN = true (the default). NOTHING is modified. It
//       writes the DDL backup, the classification report, the location
//       list and the two .sql files, and prints the full plan.
//    2. Have a HUMAN review dbprojection_preflight_report.csv -- above all
//       the computed IN-SCOPE list. The drop list is derived from the
//       catalog at runtime and is only known after this first pass.
//    3. Only then set DRY_RUN = false and re-run.
//
//  GOAL
//  ----
//  Drop the PARTITIONED tables of `dbprojection` WITHOUT deleting their
//  HDFS data.
//
//  These tables were created as TRANSLATED_TO_EXTERNAL with
//  'external.table.purge'='true'. With that property set, DROP TABLE
//  deletes the data directory even though the table is EXTERNAL. So per
//  table, in this exact order:
//
//    1. ALTER TABLE ... SET TBLPROPERTIES ('external.table.purge'='false')
//    2. re-read the property and verify it really is 'false'
//    3. DROP TABLE IF EXISTS ...
//    4. verify the HDFS directory is STILL there
//
//  No table is dropped until its OWN flag has been verified. The ALTERs are
//  never batched ahead of the DROPs: each table walks the four steps alone,
//  so a table whose verification fails is skipped rather than dropped.
//
//  SCOPE -- PARTITIONED TABLES ONLY
//  --------------------------------
//  Of the tables in the database, only those declaring at least one
//  partition column are in scope. Non-partitioned tables are never ALTERed
//  and never DROPped. The subset is NOT known in advance: it is computed at
//  runtime from the catalog, never guessed from a table name. `_detailed`,
//  `_uat`, `tmp1` and friends say nothing about partitioning.
//
//  Partitioning is read from the catalog metadata:
//      spark.sessionState.catalog.getTableMetadata(...).partitionColumnNames
//  and NOT by parsing the '# Partition Information' block of DESCRIBE
//  FORMATTED, and NOT from SHOW PARTITIONS -- that throws on a
//  non-partitioned table and returns an empty set for a partitioned table
//  with no partitions yet, two situations that must not be conflated. A
//  partitioned table with zero partitions IS in scope.
//
//  CLASSES
//  -------
//    MISSING         not in the metastore                  -> no-op
//    NOT_PARTITIONED no partition columns                  -> OUT OF SCOPE,
//                                                             never touched
//    MANAGED         partitioned but MANAGED_TABLE         -> NEVER dropped;
//                    purge does not apply, DROP would delete the data.
//                    Reported for a human.
//    PURGE_TRUE      partitioned + external + purge true   -> ALTER, then drop
//    PURGE_FALSE     partitioned + external + purge false  -> drop directly
//
//  HARD CONSTRAINTS honoured here
//  ------------------------------
//    * never MSCK REPAIR TABLE (these are datasource tables with
//      partitionProvider=catalog);
//    * never DROP DATABASE ... CASCADE -- tables go one at a time so one
//      failure cannot cascade;
//    * no UUID and no HDFS path is hardcoded: every location is resolved at
//      runtime from the catalog;
//    * idempotent -- re-running after a partial failure is safe. An
//      already-altered table is detected (PURGE_FALSE) and its ALTER
//      skipped; an already-dropped table reads as MISSING.
//
//  THE SAFETY NET
//  --------------
//  After every DROP the HDFS directory is re-checked through the
//  FileSystem API. If it vanished, the run ABORTS immediately -- so if the
//  very first table loses its data the remaining ones are never touched.
//
//  Exact twin of unpurge_and_drop_dbprojection.py: same logic, same report,
//  same .sql output.
// =====================================================================

import java.net.URI
import java.time.LocalDateTime

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType

// ---------------------------------------------------------------------
// 1. CONFIG
// ---------------------------------------------------------------------

val DRY_RUN = true                // must stay true until the report is reviewed
val DB      = "dbprojection"

// Where the artefacts are written. Scheme-less on purpose: it resolves
// against the cluster's default filesystem, the same way the fix_runid
// scripts do. Point it anywhere the notebook can write.
val OUTPUT_DIR = "/Projects/STCreditRisk_UAT/tmp/fix_droptables"

// The property that makes DROP TABLE delete the data of an EXTERNAL table.
val PURGE_KEY = "external.table.purge"

// Compare SHOW TABLES against the inventory transcribed in the brief and
// report any addition or removal. The script always works from the RUNTIME
// list -- this only tells you the ground moved.
val CHECK_INVENTORY = true

val DDL_BACKUP_PATH   = OUTPUT_DIR + "/dbprojection_ddl_backup.sql"
val REPORT_PATH       = OUTPUT_DIR + "/dbprojection_preflight_report.csv"
val LOCATIONS_PATH    = OUTPUT_DIR + "/dbprojection_locations.txt"
val ALTER_SQL_PATH    = OUTPUT_DIR + "/dbprojection_alter_purge.sql"
val DROP_SQL_PATH     = OUTPUT_DIR + "/dbprojection_drop_tables.sql"
val OUT_OF_SCOPE_PATH = OUTPUT_DIR + "/dbprojection_out_of_scope.txt"

// Inventory transcribed from SHOW TABLES IN dbprojection (85 tables).
// Reference only -- the script re-derives the list at runtime and diffs
// against this one. Upstream misspellings are preserved verbatim.
val EXPECTED_TABLES = Seq(
  "chr", "chr_detailed", "chr_detailed_mta", "chr_idealised_detailed", "chr_uat",
  "ene_c29", "ene_c29_and_borne", "ene_c29_diff", "ene_no_secto",
  "ene_no_secto_borne", "ene_no_secto_pivot", "exceptions_npl", "lgd", "lgd_old",
  "lgd_old_arr_03052024", "lgd_term_structure_detailed",
  "lgd_term_structure_detailed_old", "lgd_term_structure_detailed_old_arr_03052024",
  "migration_matrix", "migration_matrix_21q2_npl", "migration_matrix_22q3",
  "migration_matrix_detailed", "migration_matrix_detailed_2021_recette",
  "migration_matrix_detailed_22q3", "migration_matrix_npl",
  "migration_matrix_npl_detail", "migration_matrix_npl_exception",
  "migration_matrix_npl_uat", "migration_matrix_uat", "model_cr_exceptions",
  "model_drz_exceptions", "model_drz_exceptions_2021_recette",
  "model_migration_matrix_exceptions", "model_term_structure_exceptions",
  "model_term_structure_exceptions_2023_03_21", "nka_scenarii_ponderation",
  "npl_migration_matrix", "npl_migration_matrix_bis",
  "npl_migration_matrix_detailed", "pcure", "pcure_hlc_old",
  "pcure_old_arr_03052024", "projected_cr_detailed", "projected_cr_detailed_old",
  "projected_cr_detailed_old_arr_03052024", "projected_dr",
  "projected_dr_21q2_npl", "projected_dr_22q3", "projected_dr_detailed",
  "projected_dr_detailed_2021_recette", "projected_dr_detailed_22q3",
  "projected_dr_uat", "projected_z", "projected_z_21q2_npl", "projected_z_22q3",
  "projected_z_detailed", "projected_z_detailed_2021_recette",
  "projected_z_detailed_22q3", "projected_z_lgd_detailed", "projected_z_uat",
  "run_history", "run_projection", "run_projection_2021_2_7",
  "run_projection_2021_recette", "run_projection_21q2", "run_projection_22q3",
  "scenarii_ponderation", "scenarii_ponderation_2021_recette", "tabrbhconcurrent",
  "term_structure", "term_structure_21q2_npl", "term_structure_22q3",
  "term_structure_detailed", "term_structure_detailed2",
  "term_structure_detailed_2021_recette", "term_structure_detailed_22q3",
  "term_structure_exceptions_2021_recette", "term_structure_idealised",
  "term_structure_idealised_detailed", "term_structure_uat", "test2137_20154",
  "tightening_corp", "tightening_retail", "tmp1", "tmp2")

// ---------------------------------------------------------------------
// 2. Logging helpers
// ---------------------------------------------------------------------

val TS = java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")

def log(level: String, msg: String): Unit =
  println(f"[${LocalDateTime.now().format(TS)}] $level%-8s $msg")

def section(title: String): Unit = {
  println("")
  println("=" * 100)
  println(title)
  println("=" * 100)
}

val MODE = if (DRY_RUN) "DRY-RUN" else "APPLY"

// ---------------------------------------------------------------------
// 3. FS handle + artefact writing
// ---------------------------------------------------------------------

val conf = sparkContext.hadoopConfiguration

/** Write a text artefact through the Hadoop FS API (no local FS assumption). */
def writeText(path: String, text: String): Boolean = {
  if (path == null) return false
  Try {
    val wfs    = FileSystem.get(new URI(path), conf)
    val p      = new Path(path)
    val parent = p.getParent
    if (parent != null && !wfs.exists(parent)) wfs.mkdirs(parent)
    val out = wfs.create(p, true)
    out.write(text.getBytes("UTF-8"))
    out.close()
    log("OK", s"written $path")
    true
  }.recover { case e =>
    log("ERROR", s"could not write $path : ${e.getMessage}")
    false
  }.get
}

/** Does this HDFS path exist? None when the path itself cannot be read. */
def pathExists(loc: String): Option[Boolean] =
  if (loc == null || loc.trim.isEmpty) None
  else Try {
    val lfs = FileSystem.get(new URI(loc), conf)
    lfs.exists(new Path(loc))
  }.toOption

/** Immediate children of a location, as a cheap before/after fingerprint.
  * Not recursive on purpose: the failure mode being guarded against is the
  * whole directory disappearing, and a recursive walk of 85 partitioned
  * tables would cost far more than it tells you. -1 means "unreadable". */
def childCount(loc: String): Int =
  if (loc == null || loc.trim.isEmpty) -1
  else Try {
    val lfs = FileSystem.get(new URI(loc), conf)
    lfs.listStatus(new Path(loc)).length
  }.getOrElse(-1)

/** CSV cell: escape the delimiter and the quotes rather than lose a column. */
def csvCell(s: String): String = {
  val v = if (s == null) "" else s
  if (v.contains(";") || v.contains("\"") || v.contains("\n"))
    "\"" + v.replace("\"", "\"\"").replace("\n", " ") + "\""
  else v
}

// ---------------------------------------------------------------------
// 4. Inventory  (runtime, then diffed against the transcribed list)
// ---------------------------------------------------------------------

section(s"1/6  INVENTORY  (mode=$MODE)  database=$DB")

val dbExists = Try(spark.sql(s"SHOW DATABASES LIKE '$DB'").collect().nonEmpty).getOrElse(false)
if (!dbExists)
  sys.error(s"ABORT: database $DB is not visible to this Spark session. " +
            "Nothing was inspected and nothing was modified.")

val actualTables: Seq[String] =
  Try(spark.sql(s"SHOW TABLES IN $DB").collect()
           .map(r => r.getAs[String]("tableName")).toSeq.sorted)
    .getOrElse(sys.error(s"ABORT: SHOW TABLES IN $DB failed. Nothing was modified."))

log("INFO", s"tables found at runtime : ${actualTables.length}")

if (CHECK_INVENTORY) {
  val added   = actualTables.filterNot(EXPECTED_TABLES.contains)
  val removed = EXPECTED_TABLES.filterNot(actualTables.contains)
  log("INFO", s"inventory in the brief  : ${EXPECTED_TABLES.length}")
  if (added.isEmpty && removed.isEmpty)
    log("OK", "runtime list matches the transcribed inventory exactly")
  else {
    if (added.nonEmpty)
      log("WARN", s"${added.length} table(s) present now but NOT in the brief: " +
                  added.mkString(", "))
    if (removed.nonEmpty)
      log("WARN", s"${removed.length} table(s) in the brief but NOT present now: " +
                  removed.mkString(", "))
    log("WARN", "the script works from the RUNTIME list; this is a heads-up, not a block")
  }
}

// ---------------------------------------------------------------------
// 5. Pre-flight: DDL backup for EVERY table (in scope or not)
// ---------------------------------------------------------------------

section(s"2/6  DDL BACKUP  (all ${actualTables.length} tables)")

/** SHOW CREATE TABLE, falling back to the SERDE form: Spark refuses the
  * plain form for some Hive-created tables and tells you to use AS SERDE. */
def showCreate(t: String): Option[String] = {
  val fq = s"$DB.$t"
  Try(spark.sql(s"SHOW CREATE TABLE $fq").collect().map(_.get(0).toString).mkString("\n"))
    .orElse(Try(spark.sql(s"SHOW CREATE TABLE $fq AS SERDE")
                     .collect().map(_.get(0).toString).mkString("\n")))
    .toOption
    .filter(_.trim.nonEmpty)
}

val ddlParts   = ArrayBuffer[String]()
val ddlMissing = ArrayBuffer[String]()

actualTables.foreach { t =>
  showCreate(t) match {
    case Some(ddl) =>
      ddlParts += s"-- ===== $DB.$t =====\n$ddl\n;\n"
    case None =>
      ddlMissing += t
      log("ERROR", s"SHOW CREATE TABLE failed for $DB.$t")
  }
}

val ddlText =
  s"-- DDL backup of $DB taken on ${LocalDateTime.now()}\n" +
  s"-- ${actualTables.length} table(s). Replay this file to recreate a definition.\n" +
  "-- Generated by unpurge_and_drop_dbprojection.scala -- do not hand-edit.\n\n" +
  ddlParts.mkString("\n")

val ddlOk = writeText(DDL_BACKUP_PATH, ddlText)

// The backup is the ONLY way back from a DROP, so an incomplete one stops
// the run -- in dry-run too, because a dry run whose backup silently failed
// would green-light a real run that has no rope.
if (!ddlOk)
  sys.error(s"ABORT: the DDL backup could not be written to $DDL_BACKUP_PATH. " +
            "Refusing to go any further. Nothing was modified.")
if (ddlMissing.nonEmpty)
  sys.error(s"ABORT: the DDL backup is missing ${ddlMissing.length} table(s): " +
            ddlMissing.mkString(", ") + ". A partial backup is not a backup. " +
            "Nothing was modified.")
log("OK", s"DDL backup complete for all ${actualTables.length} tables")

// ---------------------------------------------------------------------
// 6. Pre-flight: classification
// ---------------------------------------------------------------------

section(s"3/6  CLASSIFICATION  (${actualTables.length} tables)")

case class Info(name: String, cls: String, tableType: String, translated: String,
                purge: String, location: String, provider: String,
                partCols: Seq[String], partCount: String, note: String)

val infos = ArrayBuffer[Info]()

actualTables.foreach { t =>
  val fq = s"$DB.$t"
  val metaOpt =
    Try(spark.sessionState.catalog.getTableMetadata(TableIdentifier(t, Some(DB)))).toOption

  metaOpt match {
    case None =>
      infos += Info(t, "MISSING", "", "", "", "", "", Seq.empty, "",
                    "not in the metastore")

    case Some(meta) =>
      val ttype      = meta.tableType.name
      val props      = meta.properties
      val purge      = props.getOrElse(PURGE_KEY, "")
      val translated = props.getOrElse("TRANSLATED_TO_EXTERNAL", "")
      val location   = meta.storage.locationUri.map(_.toString).getOrElse("")
      val provider   = meta.provider.getOrElse(
                         props.getOrElse("spark.sql.sources.provider", "hive"))
      val partCols   = meta.partitionColumnNames

      // The scope gate FIRST, before purge or table type is even considered,
      // so an out-of-scope table is never a candidate for an ALTER.
      val cls =
        if (partCols.isEmpty) "NOT_PARTITIONED"
        else if (meta.tableType == CatalogTableType.MANAGED) "MANAGED"
        else if (purge.trim.toLowerCase == "true") "PURGE_TRUE"
        else "PURGE_FALSE"

      // Only meaningful for a partitioned table, and only for the record.
      val partCount =
        if (partCols.isEmpty) ""
        else Try(spark.sql(s"SHOW PARTITIONS $fq").count().toString).getOrElse("?")

      val note = cls match {
        case "NOT_PARTITIONED" => "out of scope - never altered, never dropped"
        case "MANAGED"         => "MANAGED: DROP would delete the data, purge does not apply"
        case "PURGE_TRUE"      => "needs the ALTER before it can be dropped"
        case "PURGE_FALSE"     => "purge already false/absent - drop directly"
        case _                 => ""
      }
      infos += Info(t, cls, ttype, translated, purge, location, provider,
                    partCols, partCount, note)
  }
}

val byClass = infos.groupBy(_.cls).map { case (k, v) => k -> v.length }
Seq("PURGE_TRUE", "PURGE_FALSE", "MANAGED", "NOT_PARTITIONED", "MISSING").foreach { c =>
  log("INFO", f"$c%-16s : ${byClass.getOrElse(c, 0)}")
}

// --- the report ------------------------------------------------------
// UTF-8 BOM (﻿) + semicolons so French Excel opens it in columns.
val reportText =
  "﻿" +
  Seq("table", "class", "table_type", "translated_to_external", "purge",
      "location", "provider", "partitioned", "partition_columns",
      "partition_count", "note").mkString(";") + "\n" +
  infos.map { i =>
    Seq(i.name, i.cls, i.tableType, i.translated, i.purge, i.location, i.provider,
        if (i.partCols.nonEmpty) "yes" else "no",
        i.partCols.mkString(","), i.partCount, i.note).map(csvCell).mkString(";")
  }.mkString("\n") + "\n"
writeText(REPORT_PATH, reportText)

// --- locations, captured BEFORE any drop -----------------------------
// Once the table is gone the metastore no longer knows where its data is.
val locationsText =
  s"# HDFS location of every table in $DB, captured ${LocalDateTime.now()}\n" +
  "# BEFORE any DROP. This is how the data is found again afterwards.\n" +
  infos.map(i => s"${i.name}\t${i.cls}\t${i.location}").mkString("\n") + "\n"
writeText(LOCATIONS_PATH, locationsText)

// --- the auditable exclusion list ------------------------------------
val outOfScope = infos.filter(_.cls == "NOT_PARTITIONED")
val outOfScopeText =
  s"# Tables in $DB deliberately left out of scope: NO partition column.\n" +
  s"# Neither ALTERed nor DROPped. ${outOfScope.length} table(s), " +
  s"${LocalDateTime.now()}\n" +
  outOfScope.map(_.name).mkString("\n") + "\n"
writeText(OUT_OF_SCOPE_PATH, outOfScopeText)

// ---------------------------------------------------------------------
// 7. The plan, and the two .sql files
// ---------------------------------------------------------------------

section(s"4/6  PLAN  (mode=$MODE)")

// The in-scope order is fixed once, here, and reused by both .sql files and
// by the execution loop, so the ALTER file and the DROP file cannot drift.
val inScope = infos.filter(i => i.cls == "PURGE_TRUE" || i.cls == "PURGE_FALSE")
                   .sortBy(_.name)

if (inScope.isEmpty)
  log("WARN", "no partitioned table is in scope - nothing would be altered or dropped")

inScope.foreach { i =>
  if (i.cls == "PURGE_TRUE")
    log("PLAN", s"ALTER  $DB.${i.name}  SET $PURGE_KEY = false  (currently '${i.purge}')")
  log("PLAN", s"DROP   $DB.${i.name}  [${i.partCols.mkString(",")}, " +
              s"${i.partCount} partition(s)]  keeps ${i.location}")
}
infos.filter(_.cls == "MANAGED").foreach { i =>
  log("WARN", s"NOT DROPPED (MANAGED): $DB.${i.name} at ${i.location} - " +
              "DROP would delete the data. A human must decide.")
}

val alterSql =
  s"-- ALTER statements for the PARTITIONED subset of $DB, " +
  s"generated ${LocalDateTime.now()}\n" +
  s"-- ${inScope.count(_.cls == "PURGE_TRUE")} statement(s). Derived from the catalog " +
  "at runtime, not hand-written.\n" +
  s"-- Run this FIRST and verify every $PURGE_KEY reads back as false.\n\n" +
  inScope.filter(_.cls == "PURGE_TRUE")
         .map(i => s"ALTER TABLE $DB.${i.name} SET TBLPROPERTIES ('$PURGE_KEY'='false');")
         .mkString("\n") + "\n"
writeText(ALTER_SQL_PATH, alterSql)

val dropSql =
  s"-- DROP statements for the PARTITIONED subset of $DB, " +
  s"generated ${LocalDateTime.now()}\n" +
  s"-- ${inScope.length} statement(s), SAME ORDER as $ALTER_SQL_PATH.\n" +
  "--\n" +
  "-- #################################################################\n" +
  s"-- #  NEVER run this file before $ALTER_SQL_PATH\n" +
  "-- #  has completed AND every external.table.purge has been verified\n" +
  "-- #  to read back as 'false'. Dropping a table whose purge flag is\n" +
  "-- #  still true DELETES ITS HDFS DATA.\n" +
  "-- #################################################################\n\n" +
  inScope.map(i => s"DROP TABLE IF EXISTS $DB.${i.name};").mkString("\n") + "\n"
writeText(DROP_SQL_PATH, dropSql)

// ---------------------------------------------------------------------
// 8. Execution
// ---------------------------------------------------------------------

section(s"5/6  EXECUTION  (mode=$MODE)")

var cAltered = 0
var cDropped = 0
var cFailed  = 0
val failures = ArrayBuffer[(String, String)]()

/** Re-read the purge property from the metastore, not from the cached plan.
  * Returns "" when the property is absent, which is as good as false. */
def readPurge(t: String): String = {
  Try(spark.sessionState.catalog.refreshTable(TableIdentifier(t, Some(DB))))
  Try {
    spark.sessionState.catalog.getTableMetadata(TableIdentifier(t, Some(DB)))
         .properties.getOrElse(PURGE_KEY, "")
  }.getOrElse("<unreadable>")
}

if (DRY_RUN) {
  log("INFO", "DRY_RUN=true -> no ALTER and no DROP was issued")
  log("INFO", s"review $REPORT_PATH, get the in-scope list signed off, then set " +
              "DRY_RUN = false and re-run")
} else {
  inScope.foreach { i =>
    val t  = i.name
    val fq = s"$DB.$t"

    // --- step 1: the ALTER, only when the flag is actually true --------
    var flagOk = true
    if (i.cls == "PURGE_TRUE") {
      Try {
        spark.sql(s"ALTER TABLE $fq SET TBLPROPERTIES ('$PURGE_KEY'='false')")
        cAltered += 1
        log("OK", s"ALTER  $fq  $PURGE_KEY -> false")
      }.failed.foreach { e =>
        flagOk = false
        cFailed += 1
        failures += ((t, s"ALTER failed: ${e.getMessage}"))
        log("ERROR", s"ALTER FAILED $fq : ${e.getMessage} -- NOT dropping it")
      }
    } else {
      log("INFO", s"SKIP ALTER $fq  ($PURGE_KEY already '${i.purge}')")
    }

    // --- step 2: verify, per table, before its own DROP ----------------
    if (flagOk) {
      val now = readPurge(t).trim
      if (now.isEmpty || now.toLowerCase == "false") {
        log("OK", s"VERIFY $fq  $PURGE_KEY = '${if (now.isEmpty) "<absent>" else now}'")
      } else {
        flagOk = false
        cFailed += 1
        failures += ((t, s"$PURGE_KEY reads back as '$now', not 'false'"))
        log("ERROR", s"VERIFY FAILED $fq : $PURGE_KEY = '$now' -- NOT dropping it")
      }
    }

    // --- step 3: capture + assert the location, then DROP --------------
    if (flagOk) {
      val loc     = i.location
      val before  = pathExists(loc)
      val beforeN = childCount(loc)
      if (before.isEmpty) {
        cFailed += 1
        failures += ((t, s"location '$loc' could not be read - refusing to drop blind"))
        log("ERROR", s"LOCATION UNREADABLE $fq ('$loc') -- NOT dropping it")
      } else if (!before.get) {
        cFailed += 1
        failures += ((t, s"location $loc does not exist before the drop"))
        log("ERROR", s"LOCATION MISSING $fq : $loc -- NOT dropping it")
      } else {
        var dropped = true
        Try {
          spark.sql(s"DROP TABLE IF EXISTS $fq")
          cDropped += 1
          log("OK", s"DROP   $fq")
        }.failed.foreach { e =>
          dropped = false
          cFailed += 1
          failures += ((t, s"DROP failed: ${e.getMessage}"))
          log("ERROR", s"DROP FAILED $fq : ${e.getMessage}")
        }

        // --- step 4: the safety net ---------------------------------
        // Checked even when the DROP reported a failure: a half-failed
        // drop that still removed the directory must stop the run.
        val after  = pathExists(loc)
        val afterN = childCount(loc)
        if (!after.getOrElse(false)) {
          log("CRITICAL", s"THE DATA DIRECTORY OF $fq IS GONE: $loc")
          log("CRITICAL", "aborting the entire run so no further table is touched")
          sys.error(s"ABORT: dropping $fq removed its HDFS directory $loc. " +
                    s"$cDropped table(s) had been dropped at that point. The " +
                    s"definitions are in $DDL_BACKUP_PATH and the locations in " +
                    s"$LOCATIONS_PATH. Investigate before re-running.")
        }
        if (beforeN >= 0 && afterN >= 0 && beforeN != afterN)
          log("WARN", s"$fq : entries under $loc went from $beforeN to $afterN")
        else if (dropped)
          log("OK", s"DATA KEPT $fq : $loc still exists ($afterN entr(y/ies))")
      }
    }
  }
}

// ---------------------------------------------------------------------
// 9. Summary
// ---------------------------------------------------------------------

section(s"6/6  SUMMARY  (mode=$MODE)")

val nTotal          = infos.length
val nPartitioned    = infos.count(i => i.cls == "PURGE_TRUE" || i.cls == "PURGE_FALSE")
val nNotPartitioned = infos.count(_.cls == "NOT_PARTITIONED")
val nManaged        = infos.count(_.cls == "MANAGED")
val nMissing        = infos.count(_.cls == "MISSING")

println(s"database              : $DB")
println(s"mode                  : $MODE")
println(s"tables inspected      : $nTotal")
println(s"  partitioned (scope) : $nPartitioned")
println(s"  skipped NOT_PART.   : $nNotPartitioned")
println(s"  skipped MANAGED     : $nManaged")
println(s"  missing             : $nMissing")
println(s"altered               : " +
        s"${if (DRY_RUN) inScope.count(_.cls == "PURGE_TRUE") else cAltered}" +
        s"${if (DRY_RUN) "  (planned, DRY_RUN)" else ""}")
println(s"dropped               : ${if (DRY_RUN) inScope.length else cDropped}" +
        s"${if (DRY_RUN) "  (planned, DRY_RUN)" else ""}")
println(s"failed                : $cFailed")
println("")
println(s"ddl backup            : $DDL_BACKUP_PATH")
println(s"report                : $REPORT_PATH")
println(s"locations             : $LOCATIONS_PATH")
println(s"alter sql             : $ALTER_SQL_PATH")
println(s"drop sql              : $DROP_SQL_PATH")
println(s"out of scope          : $OUT_OF_SCOPE_PATH")

val accounted = nPartitioned + nNotPartitioned + nManaged + nMissing
if (accounted != nTotal)
  log("ERROR", s"the classes add up to $accounted but $nTotal tables were inspected")
else
  log("OK", s"every one of the $nTotal inspected tables is accounted for")

if (nManaged > 0) {
  println("")
  println("MANAGED -- NOT dropped, a human must decide")
  println("-" * 100)
  infos.filter(_.cls == "MANAGED").foreach(i => println(s"  $DB.${i.name}   ${i.location}"))
}

if (failures.nonEmpty) {
  println("")
  println("FAILED -- not dropped")
  println("-" * 100)
  failures.foreach { case (t, why) => println(s"  $DB.$t\n      $why") }
}

println("")
if (DRY_RUN)
  println(">>> DRY RUN: nothing was altered and nothing was dropped. Review " +
          s"$REPORT_PATH, have the in-scope list signed off, then set DRY_RUN = false.")
else
  println(s">>> APPLIED. $cDropped table(s) dropped, data left in place. The " +
          s"definitions are in $DDL_BACKUP_PATH and the locations in $LOCATIONS_PATH.")
