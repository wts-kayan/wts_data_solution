// =====================================================================
//  recreate_table_partcol_runid.scala        (Spark-Scala -- drop-in cell)
//
//  README -- HOW TO RUN
//  --------------------
//  Paste as a cell of the existing Dataiku Spark-Scala notebook. It REUSES
//  the `spark` / `sparkContext` vals bound by cell 1 -- do NOT re-create
//  the SparkSession.
//
//    1. Set HIVE_TABLE / TABLE_ROOT and pick a FIX_MODE (see below).
//    2. Run with DRY_RUN = true (the default). NOTHING is modified; the
//       full DDL is printed and written to a .sql file.
//    3. Review the .sql AND the backup file, then set DRY_RUN = false.
//
//  Exact twin of recreate_table_partcol_runid.py: same logic, same report,
//  same .sql output.
//
//  GOAL
//  ----
//  Guarantee the table carries
//      'spark.sql.sources.schema.numPartCols' = '1'
//      'spark.sql.sources.schema.partCol.0'   = 'runId'
//  and that the field for the partition column inside the
//  'spark.sql.sources.schema' JSON is named `runId` too, so that Spark's view
//  of the table uses the camelCase name -- including when it creates
//  partition directories.
//
//  WHAT CANNOT BE CHANGED (and why the two cases coexist)
//  -----------------------------------------------------
//  The Hive metastore LOWERCASES every column name. There is no DDL that
//  makes it store `runId`:
//      SHOW CREATE TABLE  -> PARTITIONED BY (runid string)     always
//      SHOW PARTITIONS    -> runid=<uuid>                       always
//  So the Hive column stays `runid` and only Spark's schema properties carry
//  `runId`. That is not a defect, it is how Spark preserves column case on
//  Hive -- the same mechanism already keeps the camelCase data columns
//  (matrixMigrationName, asOfDate, notationCode) readable.
//
//  Consequence, and the reason this matters: Spark names the partition
//  DIRECTORIES after its own schema, so once partCol.0 is `runId` a Spark
//  write produces runId=<uuid>/ rather than runid=<uuid>/.
//
//  The script therefore RENAMES the partition field inside the schema JSON to
//  PARTITION_COL. Data columns are never re-cased.
//
//  SPARK WILL NOT LET YOU SET THESE PROPERTIES BY HAND
//  ---------------------------------------------------
//  This is the constraint that decides which mode is usable:
//
//      AnalysisException: Cannot persist <table> into Hive metastore as table
//      property keys may not start with 'spark.sql.'
//
//  Spark rejects ANY user-issued SET TBLPROPERTIES / CREATE ... TBLPROPERTIES
//  whose key starts with "spark.sql.". So the schema properties can only be
//  written in one of two ways:
//    * let SPARK write them, by creating a DATASOURCE table (USING ORC +
//      PARTITIONED BY) whose column list already carries the casing. Spark
//      then persists spark.sql.sources.schema / numPartCols / partCol.0 for
//      you, taking the names verbatim from the DDL. This is FIX_MODE
//      "recreate", and it is the only mode that works from Spark;
//    * replay the ALTER through HIVE / beeline, which has no such guard.
//
//  TWO MODES -- read this before choosing
//  --------------------------------------
//  FIX_MODE = "alter"     (cannot be executed from Spark -- emits DDL only)
//      Generates
//          ALTER TABLE ... SET TBLPROPERTIES (...)
//      and REFUSES to run it, because Spark rejects it (see above). Use this
//      to get a .sql file you replay through beeline. It touches nothing:
//      no drop, no partition re-registration.
//
//  FIX_MODE = "recreate"  (DROP + CREATE, the mode that works from Spark)
//      Captures the schema, the location and EVERY partition location, drops
//      the table definition, recreates it, then re-adds each partition.
//      The data is NOT touched because the table is EXTERNAL -- but the
//      metastore entry is destroyed and rebuilt, so if the session dies
//      between DROP and the last ADD PARTITION you must replay the generated
//      .sql by hand. That file is written BEFORE the drop, on purpose.
//
//  SAFETY GUARDS (both modes abort rather than risk data)
//  ------------------------------------------------------
//    * the table must be EXTERNAL -- DROP TABLE on a MANAGED table deletes
//      the data directory;
//    * 'external.table.purge' must NOT be 'true' -- with that property set,
//      DROP TABLE deletes the data even for an EXTERNAL table;
//    * in "recreate" mode the partition inventory must be non-empty and
//      fully captured, otherwise the script refuses to drop;
//    * the schema must be capturable in Spark's own JSON form.
// =====================================================================

import java.net.URI
import java.time.LocalDateTime

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.util.Try

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

// ---------------------------------------------------------------------
// 1. CONFIG
// ---------------------------------------------------------------------

val HIVE_TABLE = "dbprojection.term_structure"
val TABLE_ROOT =
  "/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/term_structure"
val DRY_RUN = true                // must stay true until the DDL is reviewed

val FIX_MODE = "recreate"         // "alter" (safe, in place) | "recreate" (drop + create)

// The partition column AS SPARK SEES IT -- the case written into
// spark.sql.sources.schema.partCol.0 and into the schema JSON field name.
// NOTE: the HIVE column is ALWAYS lowercase; the metastore lowercases every
// column name, so SHOW CREATE TABLE keeps showing PARTITIONED BY (runid
// string) and SHOW PARTITIONS keeps returning runid=<uuid> whatever you put
// here. Only Spark's own view of the table can carry the camelCase.
val PARTITION_COL  = "runId"
val PARTITION_TYPE = "string"
val HIVE_PARTITION_COL = PARTITION_COL.toLowerCase   // what the metastore stores

// Directory key used by the partition LOCATIONs when they have to be rebuilt.
// Set to "runId" if you ran rename_partitions_to_runId.scala, "runid" otherwise.
val ON_DISK_KEY = "runId"

val DDL_OUTPUT_PATH =
  "/Projects/STCreditRisk_UAT/tmp/recreate_table_ddl.sql"
val BACKUP_OUTPUT_PATH =
  "/Projects/STCreditRisk_UAT/tmp/table_definition_backup.sql"

// ---------------------------------------------------------------------
// 2. Logging helpers
// ---------------------------------------------------------------------

val TS = java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")

def log(level: String, msg: String): Unit =
  println(f"[${LocalDateTime.now().format(TS)}] $level%-7s $msg")

def section(title: String): Unit = {
  println("")
  println("=" * 100)
  println(title)
  println("=" * 100)
}

val MODE = if (DRY_RUN) "DRY-RUN" else "APPLY"

require(FIX_MODE == "alter" || FIX_MODE == "recreate",
        s"FIX_MODE must be 'alter' or 'recreate', got '$FIX_MODE'")

// ---------------------------------------------------------------------
// 3. FS handle (for writing the .sql artefacts)
// ---------------------------------------------------------------------

val conf = sparkContext.hadoopConfiguration

/** Write a text artefact through the Hadoop FS API (no local FS assumption). */
def writeText(path: String, text: String): Boolean = {
  if (path == null) return false
  Try {
    val wfs = FileSystem.get(new URI(path), conf)
    val p   = new Path(path)
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

def describeRows(sql: String): Seq[(String, String, String)] =
  spark.sql(sql).collect().map { r =>
    (Option(r.get(0)).map(_.toString).getOrElse(""),
     Option(r.get(1)).map(_.toString).getOrElse(""),
     if (r.length > 2) Option(r.get(2)).map(_.toString).getOrElse("") else "")
  }.toSeq

// ---------------------------------------------------------------------
// 4. Capture the current definition
// ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// Catalog pre-flight
//   A missing table otherwise produces one stack trace per metastore call.
//   Check it once, up front, and report what the session can actually see.
// ---------------------------------------------------------------------

def tableExists(name: String): Boolean =
  Try(spark.catalog.tableExists(name)).getOrElse(
    Try { spark.sql(s"DESCRIBE TABLE $name").collect(); true }.getOrElse(false))

/** Why can't we see the table? Returns a list of report lines. */
def catalogDiagnostics(name: String): Seq[String] = {
  val out  = ArrayBuffer[String]()
  val impl = Try(spark.conf.get("spark.sql.catalogImplementation", "<unset>"))
             .getOrElse("<unknown>")
  out += s"spark.sql.catalogImplementation = $impl"

  val db = if (name.contains(".")) name.substring(0, name.indexOf('.')) else null
  Try {
    val dbs = spark.sql("SHOW DATABASES").collect().map(_.get(0).toString).sorted
    val shown = dbs.take(25).mkString(", ") + (if (dbs.length > 25) " ..." else "")
    out += s"databases visible (${dbs.length}): $shown"
    if (db != null && !dbs.contains(db)) {
      out += s"-> database '$db' is NOT in that list"
    } else if (db != null) {
      Try {
        val tbs = spark.sql(s"SHOW TABLES IN $db").collect().map(_.get(1).toString).sorted
        val ts  = tbs.take(25).mkString(", ") + (if (tbs.length > 25) " ..." else "")
        out += s"tables in $db (${tbs.length}): $ts"
      }.failed.foreach(e => out += s"SHOW TABLES IN $db failed: ${e.getMessage}")
    }
  }.failed.foreach(e => out += s"SHOW DATABASES failed: ${e.getMessage}")

  if (impl != "hive") {
    out += "-> this session has NO Hive support, so no Hive table can be found."
    out += "   The `spark` val from cell 1 must be built with Hive support enabled;"
    out += "   restart the kernel if it was created without it."
  } else {
    out += "-> Hive support is on, so check the database/table name against the"
    out += "   listing above (and current_catalog()/current_schema())."
  }
  out.toSeq
}

section(s"1/5  CAPTURE  (mode=$MODE, fix_mode=$FIX_MODE)  table=$HIVE_TABLE")

// Everything below reads the table definition. If the table is not visible at
// all, fail once with a useful message instead of one stack trace per query.
if (!tableExists(HIVE_TABLE))
  sys.error(s"ABORT: table $HIVE_TABLE is not visible to this Spark session, so its " +
            "definition cannot be captured. Nothing was modified.\n  " +
            catalogDiagnostics(HIVE_TABLE).mkString("\n  "))
log("OK", s"table $HIVE_TABLE is visible to this session")

var showCreate = ""
Try {
  showCreate = spark.sql(s"SHOW CREATE TABLE $HIVE_TABLE").collect()
                    .map(_.get(0).toString).mkString("\n")
  println(showCreate)
}.failed.foreach(e => log("WARN", s"SHOW CREATE TABLE failed: ${e.getMessage}"))

var tableType: String = null
var location: String  = null
val dataColsBuf = ArrayBuffer[(String, String)]()
val partColsBuf = ArrayBuffer[(String, String)]()
val detail      = LinkedHashMap[String, String]()

// DESCRIBE FORMATTED emits three blocks in order:
//   1. the data columns
//   2. '# Partition Information' -> the partition columns
//   3. '# Detailed Table Information' (and '# Storage Information') -> key/value
// Tracking the block explicitly matters: without it the detail rows
// (Database, Owner, Provider, ...) get mistaken for partition columns.
var block = "cols"
Try {
  describeRows(s"DESCRIBE FORMATTED $HIVE_TABLE").foreach { case (col, typ, _) =>
    val c = col.trim
    if (c.startsWith("# Partition Information")) {
      block = "partcols"
    } else if (c.startsWith("# Detailed Table Information") ||
               c.startsWith("# Storage Information")) {
      block = "detail"
    } else if (c.startsWith("#") || c.isEmpty) {
      // '# col_name' headers, blank rows
    } else {
      val t = typ.trim
      if (block == "cols" && t.nonEmpty) dataColsBuf += ((c, t))
      else if (block == "partcols" && t.nonEmpty) partColsBuf += ((c, t))
      else if (block == "detail") detail(c.stripSuffix(":")) = t
    }
  }
}.failed.foreach(e => log("WARN", s"DESCRIBE FORMATTED failed: ${e.getMessage}"))

Seq("Table Type", "Type").find(detail.contains).foreach { k =>
  val v = detail(k).toUpperCase
  tableType = if (v.contains("EXTERNAL")) "EXTERNAL"
              else if (v.contains("MANAGED")) "MANAGED" else null
}
location = detail.getOrElse("Location", null)

// The partition column is also listed among the data columns; drop it there.
val partNames = if (partColsBuf.isEmpty) Set(HIVE_PARTITION_COL)
                else partColsBuf.map(_._1.toLowerCase).toSet
val dataCols  = dataColsBuf.filterNot { case (n, _) => partNames.contains(n.toLowerCase) }.toSeq

if (location == null) {
  location = TABLE_ROOT
  log("WARN", "no Location read from the metastore, falling back to TABLE_ROOT")
}

// external.table.purge decides whether DROP TABLE deletes the data, so it has
// to be read reliably -- not guessed.
var tblprops = Map[String, String]()
var tblpropsReadOk = false
Try {
  tblprops = spark.sql(s"SHOW TBLPROPERTIES $HIVE_TABLE").collect()
                  .map(r => (r.get(0).toString, Option(r.get(1)).map(_.toString).getOrElse("")))
                  .toMap
  tblpropsReadOk = true
}.failed.foreach(e => log("WARN", s"SHOW TBLPROPERTIES failed: ${e.getMessage}"))

var purgeFlag: String = tblprops.getOrElse("external.table.purge", null)
if (purgeFlag == null) {
  // Fallback: the same properties appear as a blob in DESCRIBE FORMATTED.
  val blob = detail.getOrElse("Table Properties", "").replaceAll(" ", "").toLowerCase
  if (blob.contains("external.table.purge=true")) purgeFlag = "true"
  else if (tblpropsReadOk) purgeFlag = "false"
}

log("INFO", s"table type        : ${if (tableType == null) "UNKNOWN" else tableType}")
log("INFO", s"external.table.purge : ${if (purgeFlag == null) "UNKNOWN" else purgeFlag}")
log("INFO", s"location          : $location")
log("INFO", s"data columns      : ${dataCols.length}  " +
            dataCols.map { case (n, t) => s"$n $t" }.mkString(", "))
log("INFO", "partition columns : " +
            (if (partColsBuf.isEmpty) "(none read)"
             else partColsBuf.map { case (n, t) => s"$n $t" }.mkString(", ")))
log("INFO", s"tblproperties     : ${tblprops.size}")

// ---------------------------------------------------------------------
// 5. Guards
// ---------------------------------------------------------------------

section("2/5  GUARDS")

if (tableType == "MANAGED")
  sys.error(s"ABORT: $HIVE_TABLE is a MANAGED_TABLE. DROP TABLE would DELETE the data " +
            s"directory $location. Nothing was modified.")
if (tableType != "EXTERNAL")
  sys.error(s"ABORT: could not confirm $HIVE_TABLE is EXTERNAL (got " +
            s"'${if (tableType == null) "null" else tableType}'). Refusing to touch the " +
            "table definition. Nothing was modified.")
log("OK", "table is EXTERNAL")

if (purgeFlag != null && purgeFlag.trim.toLowerCase == "true")
  sys.error(s"ABORT: $HIVE_TABLE has external.table.purge=true. DROP TABLE would DELETE " +
            "the data even though the table is EXTERNAL. Unset it first:\n" +
            s"  ALTER TABLE $HIVE_TABLE UNSET TBLPROPERTIES ('external.table.purge');\n" +
            "Nothing was modified.")
if (purgeFlag == null && FIX_MODE == "recreate")
  sys.error(s"ABORT: could not read the TBLPROPERTIES of $HIVE_TABLE, so " +
            "external.table.purge cannot be ruled out. If it were true, DROP TABLE would " +
            "DELETE the data. Refusing to recreate. Use FIX_MODE='alter', or fix the " +
            "metastore access first. Nothing was modified.")
log("OK", "external.table.purge is not set -> DROP TABLE is metastore-only")

if (dataCols.isEmpty)
  sys.error(s"ABORT: no data columns could be read from DESCRIBE FORMATTED $HIVE_TABLE. " +
            "Recreating the table from an empty schema would destroy the definition. " +
            "Nothing was modified.")

// --- partition inventory (needed to rebuild the table) ----------------
val partitions = ArrayBuffer[(String, String)]()
Try {
  val specs = spark.sql(s"SHOW PARTITIONS $HIVE_TABLE").collect().map(_.get(0).toString)
  specs.foreach { spec =>
    val i    = spec.indexOf('=')
    val pkey = if (i < 0) PARTITION_COL else spec.substring(0, i)
    val pval = if (i < 0) spec else spec.substring(i + 1)
    var loc: String = null
    Try {
      describeRows(s"DESCRIBE FORMATTED $HIVE_TABLE PARTITION ($pkey='$pval')")
        .find { case (c, _, _) => c.trim.stripSuffix(":") == "Location" }
        .foreach { case (_, v, _) => loc = v.trim }
    }.failed.foreach(e => log("WARN", s"DESCRIBE failed for $spec : ${e.getMessage}"))
    if (loc == null) {
      loc = s"${location.stripSuffix("/")}/$ON_DISK_KEY=$pval"
      log("WARN", s"no location for $spec, assuming $loc")
    }
    partitions += ((pval, loc))
  }
  log("INFO", s"partitions captured: ${partitions.length}")
}.failed.foreach(e => log("WARN", s"SHOW PARTITIONS failed: ${e.getMessage}"))

if (FIX_MODE == "recreate" && partitions.isEmpty)
  sys.error(s"ABORT: no partition could be captured for $HIVE_TABLE. Dropping the table " +
            "now would lose every partition registration with no way to rebuild it. " +
            "Nothing was modified.")

// ---------------------------------------------------------------------
// 6. Build the DDL
// ---------------------------------------------------------------------

section("3/5  GENERATED DDL")

// The schema JSON must be the shape Spark itself produces: DataType.fromJson
// only accepts primitive names as bare strings, so a nested type such as
// array<array<double>> has to be a nested JSON object, NOT the DDL string.
// Reading it back from Spark also preserves the existing column-name casing
// (the camelCase data columns are normal Spark-on-Hive behaviour and must not
// be "fixed" here).
var schemaJson: String   = null
var schemaSource: String = null

Try {
  schemaJson   = spark.table(HIVE_TABLE).schema.json
  schemaSource = s"spark.table($HIVE_TABLE).schema"
}.failed.foreach(e =>
  log("WARN", s"could not read the schema through spark.table(): ${e.getMessage}"))

if (schemaJson == null) {
  // Fallback: rebuild it from DESCRIBE FORMATTED via Spark's own DDL parser,
  // so nested types still come out in the correct nested-object form.
  Try {
    val ddl = (dataCols :+ ((PARTITION_COL, PARTITION_TYPE)))
              .map { case (n, t) => s"`$n` $t" }.mkString(", ")
    schemaJson   = StructType.fromDDL(ddl).json
    schemaSource = "parsed from DESCRIBE FORMATTED"
  }.failed.foreach(e =>
    log("WARN", s"could not parse the schema from DESCRIBE FORMATTED: ${e.getMessage}"))
}

if (schemaJson == null)
  sys.error(s"ABORT: the schema of $HIVE_TABLE could not be captured in Spark's JSON " +
            "form. Writing spark.sql.sources.schema by hand would risk an unreadable " +
            "table definition. Nothing was modified.")

// The partition column must be present in the schema JSON, and last.
var schemaNames = Seq[String]()
Try {
  val st = DataType.fromJson(schemaJson).asInstanceOf[StructType]
  schemaNames = st.fieldNames.toSeq
  val hit = schemaNames.indexWhere(_.toLowerCase == HIVE_PARTITION_COL)
  if (hit < 0) {
    val st2 = StructType(st.fields :+ StructField(PARTITION_COL, StringType, nullable = true))
    schemaJson  = st2.json
    schemaNames = st2.fieldNames.toSeq
    log("WARN", s"partition column '$PARTITION_COL' was missing from the schema JSON - appended")
  } else if (schemaNames(hit) != PARTITION_COL) {
    // This rename is the whole point: Hive cannot store the camelCase, so the
    // schema JSON is the only place the wanted case can live.
    val was = schemaNames(hit)
    val st2 = StructType(st.fields.updated(hit, st.fields(hit).copy(name = PARTITION_COL)))
    schemaJson  = st2.json
    schemaNames = st2.fieldNames.toSeq
    log("INFO", s"partition column re-cased in the schema JSON: '$was' -> '$PARTITION_COL'")
  }
}.failed.foreach(e => log("WARN", s"could not re-parse the schema JSON: ${e.getMessage}"))

log("INFO", s"schema source     : $schemaSource")
log("INFO", s"schema fields     : ${schemaNames.mkString(", ")}")

// Properties Spark/Hive regenerate on their own -- never replay them.
val VOLATILE = Set("transient_lastDdlTime", "totalSize", "numFiles", "numRows",
                   "rawDataSize", "numPartitions", "COLUMN_STATS_ACCURATE",
                   "last_modified_time", "last_modified_by", "spark.sql.create.version")

val preserved = LinkedHashMap[String, String]()
tblprops.filterKeys(k => !VOLATILE.contains(k) && !k.startsWith("spark.sql.sources.schema"))
        .foreach { case (k, v) => preserved(k) = v }
preserved("spark.sql.sources.schema") = schemaJson
preserved("spark.sql.sources.schema.numPartCols") = "1"
preserved("spark.sql.sources.schema.partCol.0") = PARTITION_COL
if (!preserved.contains("spark.sql.partitionProvider"))
  preserved("spark.sql.partitionProvider") = "catalog"

def propsBlock(d: Seq[(String, String)], indent: String = "  "): String =
  d.sortBy(_._1).map { case (k, v) =>
    indent + "'" + k + "'='" + v.replace("'", "\\'") + "'"
  }.mkString(",\n")

val schemaProps = Seq(("spark.sql.sources.schema", schemaJson),
                      ("spark.sql.sources.schema.numPartCols", "1"),
                      ("spark.sql.sources.schema.partCol.0", PARTITION_COL))

val statements = ArrayBuffer[String]()

// Spark REFUSES to persist any table property whose key starts with
// "spark.sql." :
//   AnalysisException: Cannot persist <table> into Hive metastore as table
//   property keys may not start with 'spark.sql.'
// so spark.sql.sources.schema* can never be written by an explicit
// SET TBLPROPERTIES / CREATE ... TBLPROPERTIES issued from Spark. There are
// exactly two ways to get the wanted casing in there:
//   * let Spark write the properties ITSELF, by creating a DATASOURCE table
//     (USING ORC + PARTITIONED BY) whose column list already carries the
//     casing -- that is what "recreate" does below;
//   * replay an ALTER TABLE through Hive/beeline, which has no such guard --
//     that is what "alter" emits, without executing it.
val st         = DataType.fromJson(schemaJson).asInstanceOf[StructType]
val dataFields = st.fields.filter(_.name.toLowerCase != HIVE_PARTITION_COL)

if (FIX_MODE == "alter") {
  statements += s"ALTER TABLE $HIVE_TABLE SET TBLPROPERTIES (\n${propsBlock(schemaProps)}\n);"
} else {
  val colsDdl = dataFields.map(f => s"  `${f.name}` ${f.dataType.sql}").mkString(",\n")
  statements += s"DROP TABLE IF EXISTS $HIVE_TABLE;"
  // A datasource table: LOCATION makes it EXTERNAL, and Spark persists
  // spark.sql.sources.schema / numPartCols / partCol.0 itself, taking the
  // column names verbatim from this DDL -- casing included.
  statements +=
    s"CREATE TABLE $HIVE_TABLE (\n$colsDdl,\n  `$PARTITION_COL` $PARTITION_TYPE\n)\n" +
    s"USING ORC\n" +
    s"PARTITIONED BY (`$PARTITION_COL`)\n" +
    s"LOCATION '$location';"
  partitions.foreach { case (value, loc) =>
    statements += s"ALTER TABLE $HIVE_TABLE ADD IF NOT EXISTS PARTITION " +
                  s"($HIVE_PARTITION_COL='$value') LOCATION '$loc';"
  }
}

val header =
  s"-- generated by recreate_table_partcol_runid.scala on ${LocalDateTime.now()}\n" +
  s"-- table     : $HIVE_TABLE   (type: $tableType, purge: $purgeFlag)\n" +
  s"-- fix mode  : $FIX_MODE\n" +
  s"-- run mode  : $MODE\n" +
  s"-- partitions: ${if (FIX_MODE == "recreate") partitions.length else 0} re-registered\n"
val ddlText = header + "\n" + statements.mkString("\n\n") + "\n"
println(ddlText)

// ---------------------------------------------------------------------
// 7. Backup + execution
// ---------------------------------------------------------------------

section(s"4/5  BACKUP & EXECUTION  (mode=$MODE)")

val backupProps = if (tblprops.isEmpty) "-- (none)"
                  else tblprops.toSeq.sortBy(_._1).map { case (k, v) => s"-- $k = $v" }.mkString("\n")
val backupParts = if (partitions.isEmpty) "-- (none)"
                  else partitions.map { case (v, l) =>
                    s"ALTER TABLE $HIVE_TABLE ADD IF NOT EXISTS PARTITION " +
                    s"($HIVE_PARTITION_COL='$v') LOCATION '$l';" }.mkString("\n")

val backupText =
  s"-- BACKUP of $HIVE_TABLE taken on ${LocalDateTime.now()}\n" +
  "-- Replay this file to restore the definition if the recreate goes wrong.\n\n" +
  "-- ===== SHOW CREATE TABLE =====\n" +
  (if (showCreate.isEmpty) "-- (unavailable)" else showCreate) + "\n\n" +
  "-- ===== TBLPROPERTIES =====\n" + backupProps + "\n\n" +
  s"-- ===== PARTITIONS (${partitions.length}) =====\n" + backupParts + "\n"

val backupOk = writeText(BACKUP_OUTPUT_PATH, backupText)
writeText(DDL_OUTPUT_PATH, ddlText)

if (DRY_RUN) {
  log("INFO", s"DRY_RUN=true -> nothing executed. Review $DDL_OUTPUT_PATH and " +
              BACKUP_OUTPUT_PATH + ".")
} else if (FIX_MODE == "alter") {
  log("ERROR", "FIX_MODE='alter' cannot be executed from Spark: it refuses table " +
               "property keys starting with 'spark.sql.'.")
  log("ERROR", s"Replay $DDL_OUTPUT_PATH through Hive/beeline instead, which has no " +
               "such guard, or use FIX_MODE='recreate'.")
} else if (FIX_MODE == "recreate" && !backupOk) {
  sys.error(s"ABORT: the backup file could not be written to $BACKUP_OUTPUT_PATH. " +
            "Refusing to DROP the table without a replayable backup. Nothing was modified.")
} else {
  var failures = 0
  statements.foreach { stmt =>
    val sql = stmt.trim.stripSuffix(";")
    if (sql.nonEmpty && !sql.startsWith("--")) {
      val flat = sql.replaceAll("\n", " ")
      val shown = if (flat.length > 160) flat.substring(0, 160) else flat
      val res = Try(spark.sql(sql))
      if (res.isSuccess) {
        log("OK", "EXEC   " + shown)
      } else {
        failures += 1
        log("ERROR", "SQL FAILED (" + shown + "): " + res.failed.get.getMessage)
        if (sql.toUpperCase.startsWith("DROP TABLE"))
          sys.error("DROP TABLE failed, aborting before CREATE: " + res.failed.get.getMessage)
      }
    }
  }
  if (failures > 0)
    log("ERROR", s"$failures statement(s) failed -- replay $DDL_OUTPUT_PATH by hand")
}

// ---------------------------------------------------------------------
// 8. Verification
// ---------------------------------------------------------------------

section("5/5  VERIFICATION")

if (DRY_RUN) {
  log("INFO", "DRY_RUN=true -> verification skipped")
} else {
  Try {
    val after = spark.sql(s"SHOW TBLPROPERTIES $HIVE_TABLE").collect()
                     .map(r => (r.get(0).toString, Option(r.get(1)).map(_.toString).getOrElse("")))
                     .toMap
    Seq("spark.sql.sources.schema.numPartCols", "spark.sql.sources.schema.partCol.0")
      .foreach { k =>
        val v = after.getOrElse(k, "<missing>")
        log(if (after.contains(k)) "OK" else "ERROR", s"$k = $v")
      }
    val pc = after.getOrElse("spark.sql.sources.schema.partCol.0", "<missing>")
    if (pc != PARTITION_COL)
      log("ERROR", s"partCol.0 is '$pc', expected '$PARTITION_COL'")
  }.failed.foreach(e => log("ERROR", s"SHOW TBLPROPERTIES failed: ${e.getMessage}"))

  Try {
    val n = spark.sql(s"SHOW PARTITIONS $HIVE_TABLE").collect().length
    log(if (n == partitions.length || FIX_MODE == "alter") "OK" else "ERROR",
        s"partitions after: $n (captured before: ${partitions.length})")
  }.failed.foreach(e => log("ERROR", s"SHOW PARTITIONS failed: ${e.getMessage}"))

  Try {
    spark.sql(s"SELECT $PARTITION_COL, count(*) AS n FROM $HIVE_TABLE GROUP BY $PARTITION_COL")
         .show(100, false)
  }.failed.foreach(e => log("ERROR", s"count per $PARTITION_COL failed: ${e.getMessage}"))
}

println("")
println(s"table            : $HIVE_TABLE")
println(s"fix mode         : $FIX_MODE")
println(s"table type       : $tableType   (purge: $purgeFlag)")
println(s"location         : $location")
println(s"data columns     : ${dataCols.length}")
println(s"partitions       : ${partitions.length}")
println(s"statements       : ${statements.length} " +
        s"${if (DRY_RUN) "(printed only, DRY_RUN)" else "(executed)"}")
println(s"ddl file         : $DDL_OUTPUT_PATH")
println(s"backup file      : $BACKUP_OUTPUT_PATH")
println("")
if (DRY_RUN)
  println(">>> DRY RUN: nothing was changed. Review the DDL and the backup, then set " +
          "DRY_RUN = false.")
else if (FIX_MODE == "recreate")
  println(s">>> RECREATED. If anything failed above, replay $DDL_OUTPUT_PATH by hand -- " +
          s"the pre-drop definition is in $BACKUP_OUTPUT_PATH.")
else
  println(">>> TBLPROPERTIES updated in place. No partition was touched.")
