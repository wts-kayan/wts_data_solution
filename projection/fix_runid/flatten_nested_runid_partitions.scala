// =====================================================================
//  flatten_nested_runid_partitions.scala     (Spark-Scala -- drop-in cell 2)
//
//  README -- HOW TO RUN
//  --------------------
//  Paste this as cell 2 of the existing Dataiku Spark-Scala notebook. It
//  REUSES the `spark` / `sparkContext` vals already bound by cell 1 (the
//  DataikuSparkContext boilerplate) -- do NOT re-create the SparkSession.
//  Nothing here uses `dkuContext` or any DSS dataset, so the
//  "Dataset not found (UTILITIES_ENGINE . mydataset)" error from the stock
//  cell 2 template is irrelevant and can be ignored.
//
//    1. Set TABLE_ROOT / HIVE_TABLE in the CONFIG block below.
//       Run it once for dbprojection.term_structure, then once for
//       dbprojection.term_structure_detailed.
//    2. Run with DRY_RUN = true (the default). NOTHING is modified: the
//       script prints the metastore pre-flight, then the planned actions
//       (RENAME / MERGE / DELETE) and the DDL it would execute.
//    3. Read the report and the generated .sql file carefully.
//    4. Flip DRY_RUN = false and run again to apply.
//    5. Re-run once more: the script is idempotent, the second run must
//       report 0 wrapper found.
//
//  This is the exact twin of flatten_nested_runid_partitions.py: identical
//  logic, identical report, identical .sql output. The Python version is
//  the primary deliverable; use this one when the live notebook kernel is
//  Spark-Scala.
//
//  WHAT IT FIXES
//  -------------
//  Two layouts coexist under the table root:
//
//    correct : term_structure/runid=<uuid>/part-*.orc
//    broken  : term_structure/runId=<uuid>/runid=<uuid>/part-*.orc
//                             ^^^^^ uppercase I wrapper, holds no data
//
//  PARTITIONED BY (runid string) with numPartCols=1 / partCol.0=runid, so
//  lowercase `runid` is the canonical form and `runId=` on disk is
//  unambiguously the writer defect -- flatten toward lowercase.
//
//  The data files only live in the INNER directory, so the wrapper must be
//  FLATTENED, never deleted outright:
//     runId=<X>/runid=<X>/*  ->  runid=<X>/*      then  rmdir runId=<X>
//
//  WHAT IT RELIES ON (from SHOW CREATE TABLE)
//  ------------------------------------------
//  * EXTERNAL table -> DROP PARTITION removes the metastore entry only and
//    leaves the HDFS files intact. Re-checked at runtime: the script ABORTS
//    if the table turns out to be MANAGED_TABLE.
//  * spark.sql.partitionProvider = catalog -> Spark does NOT discover
//    partitions from the directory tree, it reads them from the metastore.
//    Explicit ALTER TABLE ... ADD PARTITION ... LOCATION is therefore
//    mandatory; MSCK REPAIR / filesystem discovery are not options.
//  * `matrix array<array<double>>` -> no read or rewrite of file contents.
//    The remediation is pure HDFS metadata movement (rename) plus metastore
//    DDL. No Spark job ever opens these ORC files.
//  * The camelCase data columns in spark.sql.sources.schema are normal
//    Spark-on-Hive behaviour. The script touches partitions ONLY, never the
//    table schema or TBLPROPERTIES.
//
//  SAFETY
//  ------
//  UAT data with no backup: every action is logged with its full path,
//  every ambiguous case is SKIPPED and reported instead of guessed, and a
//  wrapper is deleted only after it has been verified empty.
// =====================================================================

import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

// ---------------------------------------------------------------------
// 1. CONFIG
// ---------------------------------------------------------------------

val TABLE_ROOT =
  "/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/term_structure"
val DRY_RUN       = true          // must stay true until the plan has been reviewed
val HIVE_TABLE    = "dbprojection.term_structure"
val EMIT_HIVE_DDL = true

// Canonical on-disk partition key, case-sensitive. Any first-level directory
// whose key is not EXACTLY this one is treated as a wrapper.
//
// The Hive column is always lowercase `runid` (the metastore lowercases every
// column name); this constant is the DIRECTORY spelling, which Spark takes
// from spark.sql.sources.schema.partCol.0. Keep the two consistent: if
// partCol.0 is `runId` (see recreate_table_partcol_runid.scala) then the
// directories should be runId= too.
val PARTITION_KEY    = "runId"
val PARTITION_KEY_LC = PARTITION_KEY.toLowerCase   // what the metastore stores

// Where the generated DDL is written. Any Hadoop-addressable path
// (hdfs://..., file:/...). Set to null to skip the file and only print.
// NOTE: this file is written in dry-run too -- it is the review artefact.
// It lives outside TABLE_ROOT, so the table itself stays untouched in dry-run.
val DDL_OUTPUT_PATH =
  "/Projects/STCreditRisk_UAT/tmp/generated_partition_ddl.sql"

// A merge (target partition already exists) never moves _SUCCESS / _temporary
// / .hive-staging* / dot-files: they are listed in the report and left in
// place, which keeps the inner directory non-empty and therefore keeps the
// wrapper alive. Set to true to let the script delete those markers so the
// wrapper can be removed. Data files are NEVER deleted, whatever the value.
val DELETE_MARKERS_ON_MERGE = false

// Metastore pre-flight: one DESCRIBE FORMATTED per registered partition
// -> can be slow on a table with thousands of partitions. Turning it off
// also disables ORPHAN detection and the disk/metastore cross-check.
val METASTORE_PREFLIGHT = true

// ---------------------------------------------------------------------
// 2. Logging helpers
// ---------------------------------------------------------------------

val TS = DateTimeFormatter.ofPattern("HH:mm:ss")

def log(level: String, msg: String): Unit =
  println(f"[${LocalDateTime.now().format(TS)}] $level%-7s $msg")

def section(title: String): Unit = {
  println("")
  println("=" * 100)
  println(title)
  println("=" * 100)
}

val MODE = if (DRY_RUN) "DRY-RUN" else "APPLY"

// ---------------------------------------------------------------------
// 3. Hadoop FileSystem handle
//    `spark` / `sparkContext` come from cell 1. Do NOT call fs.close():
//    the handle comes from the shared cache and is used by the session.
// ---------------------------------------------------------------------

val conf = sparkContext.hadoopConfiguration
val fs   = FileSystem.get(new URI(TABLE_ROOT), conf)
val root = new Path(TABLE_ROOT)

require(fs.exists(root), s"Table root does not exist: $TABLE_ROOT")
require(fs.getFileStatus(root).isDirectory, s"Table root is not a directory: $TABLE_ROOT")

def ls(path: Path): Array[FileStatus] =
  fs.listStatus(path).sortBy(_.getPath.getName)

/** Markers / staging / hidden entries: _SUCCESS, _temporary, .hive-staging*,
  * and anything starting with '.'. Never moved file-by-file, never deleted
  * (unless DELETE_MARKERS_ON_MERGE), always reported. */
def isProtected(name: String): Boolean = name.startsWith("_") || name.startsWith(".")

/** "runId=abc" -> ("runId", Some("abc")); "foo" -> ("foo", None) */
def splitKey(name: String): (String, Option[String]) = {
  val i = name.indexOf('=')
  if (i < 0) (name, None) else (name.substring(0, i), Some(name.substring(i + 1)))
}

/** Drop scheme://authority so two locations can be compared even when one of
  * them is written without the nameservice. */
def pathOnly(uri: String): String = {
  val u = uri.stripSuffix("/")
  val i = u.indexOf("://")
  if (i < 0) u
  else {
    val j = u.indexOf("/", i + 3)
    if (j >= 0) u.substring(j) else "/"
  }
}

val ROOT_PATH_ONLY = pathOnly(TABLE_ROOT)

/** Path segments of `uri` relative to the table root, or None if the location
  * is not under the table root at all. */
def relToRoot(uri: String): Option[Array[String]] = {
  val p = pathOnly(uri)
  if (p == ROOT_PATH_ONLY) Some(Array.empty[String])
  else if (!p.startsWith(ROOT_PATH_ONLY + "/")) None
  else Some(p.substring(ROOT_PATH_ONLY.length).split("/").filter(_.nonEmpty))
}

def human(nbytes: Long): String = {
  var v = nbytes.toDouble
  val units = Array("B", "KB", "MB", "GB", "TB")
  var i = 0
  while (v >= 1024.0 && i < units.length - 1) { v /= 1024.0; i += 1 }
  f"$v%.2f ${units(i)}"
}

case class Plan(var kind: String, wrapper: Path, inner: Path, target: Path,
                value: String, files: Int, bytes: Long,
                protectedNames: Seq[String], selfNested: Boolean)

// ---------------------------------------------------------------------
// 4. Discovery (read-only)
// ---------------------------------------------------------------------

section(s"1/7  DISCOVERY  (mode=$MODE)  root=$TABLE_ROOT")

val plans           = ArrayBuffer[Plan]()
val skipped         = ArrayBuffer[(String, String)]()   // reported, never touched
val protectedSeen   = ArrayBuffer[(String, String)]()
val canonical       = ArrayBuffer[String]()
val canonicalValues = ArrayBuffer[String]()
val strays          = ArrayBuffer[(String, String)]()

val rootEntries = ls(root)

rootEntries.foreach { st =>
  val name = st.getPath.getName
  val full = st.getPath.toString

  if (isProtected(name)) {
    protectedSeen += ((full, "protected entry at the table root - left untouched"))
  } else if (!st.isDirectory) {
    strays += ((full, "file directly under the table root"))
  } else {
    val (key, valueOpt) = splitKey(name)
    if (valueOpt.isEmpty) {
      strays += ((full, "directory without a 'key=value' name"))
    } else {
      val value          = valueOpt.get
      val isCanonicalKey = key == PARTITION_KEY   // case-sensitive on purpose

      val children   = ls(st.getPath)
      val childDirs  = children.filter(c => c.isDirectory && !isProtected(c.getPath.getName))
      val childFiles = children.filter(c => !c.isDirectory && !isProtected(c.getPath.getName))
      val nestedPartDirs = childDirs.filter(c =>
        splitKey(c.getPath.getName)._1.toLowerCase == PARTITION_KEY_LC)

      if (isCanonicalKey && childDirs.isEmpty) {
        // runid=<X>/part-*.orc -> already flat, nothing to do
        canonical += full
        canonicalValues += value
      } else if (!isCanonicalKey && childDirs.isEmpty) {
        // runId=<X>/part-*.orc -> data sits directly under a non-canonical key.
        // Not the documented defect (no nesting). Renaming the directory would
        // be a partition rename, so report instead of guessing.
        skipped += ((full, s"non-canonical key '$key=' holding data files directly " +
          s"(no nested $PARTITION_KEY= dir) - manual decision required"))
      } else if (childDirs.length > 1) {
        skipped += ((full, s"${childDirs.length} child directories " +
          s"(${childDirs.map(_.getPath.getName).mkString(", ")}) - expected exactly one"))
      } else {
        val inner                    = childDirs(0)
        val innerName                = inner.getPath.getName
        val (innerKey, innerValueOpt) = splitKey(innerName)
        val innerValue               = innerValueOpt.getOrElse("")

        if (nestedPartDirs.isEmpty) {
          skipped += ((full, s"single child dir '$innerName' is not a $PARTITION_KEY= partition dir"))
        // Case-insensitive on purpose: runId=<X>/runid=<X> is exactly the
        // defect, so the inner dir may differ from the canonical key in case.
        } else if (innerKey.toLowerCase != PARTITION_KEY_LC) {
          skipped += ((full, s"nested dir key is '$innerKey=' not '$PARTITION_KEY=' - " +
            "unknown nesting, review manually"))
        } else if (childFiles.nonEmpty) {
          skipped += ((full, s"wrapper holds ${childFiles.length} data file(s) AND a " +
            s"nested '$innerName' dir - ambiguous, review manually"))
        } else if (value.toLowerCase != innerValue.toLowerCase) {
          skipped += ((full, s"UUID MISMATCH outer='$value' inner='$innerValue' - would " +
            "merge two distinct runs, never guessed"))
        } else {
          val innerChildren = ls(inner.getPath)
          val innerData     = innerChildren.filter(c => !isProtected(c.getPath.getName))
          val innerProt     = innerChildren.filter(c => isProtected(c.getPath.getName))
          val innerSubdirs  = innerData.filter(_.isDirectory)

          if (innerSubdirs.nonEmpty) {
            skipped += ((inner.getPath.toString,
              s"nested dir contains sub-directories " +
              s"(${innerSubdirs.map(_.getPath.getName).mkString(", ")}) - deeper nesting, " +
              "flatten manually or re-run after review"))
          } else if (innerChildren.isEmpty) {
            // empty nested dir: no data at risk, drop the whole wrapper
            plans += Plan("drop_empty", st.getPath, inner.getPath, null, value, 0, 0L,
                          Seq.empty, isCanonicalKey)
          } else if (innerData.isEmpty) {
            skipped += ((inner.getPath.toString,
              s"nested dir only contains protected entries " +
              s"(${innerChildren.map(_.getPath.getName).mkString(", ")}) - no data to " +
              "promote, remove manually"))
            innerProt.foreach(c =>
              protectedSeen += ((c.getPath.toString, "marker inside a data-less nested dir")))
          } else {
            val target       = new Path(root, s"$PARTITION_KEY=$value")
            val targetExists = fs.exists(target)
            // runid=<X>/runid=<X> : the wrapper IS the canonical target, so the
            // atomic directory rename is impossible -> always the merge path.
            val selfNested = isCanonicalKey
            val nbytes     = innerData.filter(!_.isDirectory).map(_.getLen).sum

            innerProt.foreach(c =>
              protectedSeen += ((c.getPath.toString, "marker inside the nested dir")))

            plans += Plan(
              if (targetExists || selfNested) "merge" else "rename",
              st.getPath, inner.getPath, target, value, innerData.length, nbytes,
              innerProt.map(_.getPath.getName).toSeq, selfNested)
          }
        }
      }
    }
  }
}

log("INFO", s"first-level entries scanned      : ${rootEntries.length}")
log("INFO", s"already-canonical $PARTITION_KEY= partitions : ${canonical.length}")
log("INFO", s"wrappers to flatten              : ${plans.length}")
log("INFO", s"skipped (reported, not touched)  : ${skipped.length}")
log("INFO", s"protected entries seen           : ${protectedSeen.length}")
log("INFO", s"stray entries at root            : ${strays.length}")

// ---------------------------------------------------------------------
// 5. Metastore pre-flight (read-only, BEFORE anything is touched on HDFS)
//
//    Two guards and one inventory:
//      * abort if the table is MANAGED_TABLE (DROP PARTITION would delete
//        the data instead of only the metastore entry);
//      * classify every registered partition as OK / NESTED / ORPHAN;
//      * cross-check the metastore against the directory scan.
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

section(s"2/7  METASTORE PRE-FLIGHT  (table=$HIVE_TABLE)")

var tableType: String = null      // "EXTERNAL" | "MANAGED" | null (undetermined)
val partOk       = ArrayBuffer[(String, String)]()
val partNested   = ArrayBuffer[(String, String)]()
val partOrphan   = ArrayBuffer[(String, String)]()
val partOutside  = ArrayBuffer[(String, String)]()
val registeredValues = scala.collection.mutable.Set[String]()
val unregistered = ArrayBuffer[(String, String, String)]()   // (path, value, kind)
var ddlExecutionAllowed = true

def describeRows(sql: String): Seq[(String, String)] =
  spark.sql(sql).collect().map { r =>
    (Option(r.get(0)).map(_.toString).getOrElse(""),
     Option(r.get(1)).map(_.toString).getOrElse(""))
  }.toSeq

if (METASTORE_PREFLIGHT && !tableExists(HIVE_TABLE)) {
  ddlExecutionAllowed = false
  log("ERROR", s"table $HIVE_TABLE is NOT visible to this Spark session")
  catalogDiagnostics(HIVE_TABLE).foreach(l => log("ERROR", "  " + l))
  log("ERROR", "-> the HDFS flattening below can still run (it needs no metastore),")
  log("ERROR", "   but NO partition DDL will be executed. Fix the table name or the")
  log("ERROR", "   Hive support first if you want the metastore re-registered.")
} else if (METASTORE_PREFLIGHT) {
  // --- guard 1: EXTERNAL vs MANAGED -----------------------------------
  Try {
    describeRows(s"DESCRIBE FORMATTED $HIVE_TABLE")
      .find { case (c, _) => Set("Table Type", "Type").contains(c.trim.stripSuffix(":")) }
      .foreach { case (_, v) =>
        val up = v.trim.toUpperCase
        if (up.contains("EXTERNAL")) tableType = "EXTERNAL"
        else if (up.contains("MANAGED")) tableType = "MANAGED"
      }
  }.failed.foreach(e => log("WARN", s"DESCRIBE FORMATTED $HIVE_TABLE failed: ${e.getMessage}"))

  if (tableType == "MANAGED") {
    sys.error(
      s"ABORT: $HIVE_TABLE is a MANAGED_TABLE. ALTER TABLE ... DROP PARTITION would " +
      "DELETE the underlying HDFS data, not just the metastore entry. This script only " +
      "supports EXTERNAL tables (SHOW CREATE TABLE reported CREATE EXTERNAL TABLE). " +
      "Nothing was modified.")
  }
  if (tableType == "EXTERNAL") {
    log("OK", "table type confirmed EXTERNAL -> DROP PARTITION is metastore-only")
  } else {
    ddlExecutionAllowed = false
    log("ERROR", "could not confirm the table type is EXTERNAL. HDFS flattening will " +
                 "still run, but NO DDL will be executed (the .sql file is still " +
                 "generated for manual review).")
  }

  // --- inventory: OK / NESTED / ORPHAN --------------------------------
  Try {
    val specs = spark.sql(s"SHOW PARTITIONS $HIVE_TABLE").collect().map(_.get(0).toString)
    log("INFO", s"registered partitions: ${specs.length}")
    specs.foreach { spec =>
      val i    = spec.indexOf('=')
      val pkey = if (i < 0) PARTITION_KEY else spec.substring(0, i)
      val pval = if (i < 0) spec else spec.substring(i + 1)
      registeredValues += pval

      val rowsTry = Try(describeRows(
        s"DESCRIBE FORMATTED $HIVE_TABLE PARTITION ($pkey='$pval')"))
      if (rowsTry.isFailure) {
        log("WARN", s"DESCRIBE failed for $spec : ${rowsTry.failed.get.getMessage}")
      } else {
        val loc = rowsTry.get.find { case (c, _) => c.trim.stripSuffix(":") == "Location" }
                         .map(_._2.trim).getOrElse("")
        if (loc.isEmpty) {
          log("WARN", s"no Location reported for $spec")
        } else {
          val existsTry = Try(FileSystem.get(new URI(loc), conf).exists(new Path(loc)))
          if (existsTry.isFailure) {
            log("WARN", s"cannot stat $loc : ${existsTry.failed.get.getMessage}")
          } else {
            val rel = relToRoot(loc)
            if (!existsTry.get)                                partOrphan  += ((spec, loc))
            else if (rel.isEmpty)                              partOutside += ((spec, loc))
            else if (rel.get.length == 1 &&
                     rel.get(0).toLowerCase ==
                       s"$PARTITION_KEY=$pval".toLowerCase)      partOk      += ((spec, loc))
            else                                               partNested  += ((spec, loc))
          }
        }
      }
    }
  }.failed.foreach(e =>
    log("WARN", s"SHOW PARTITIONS failed, inventory skipped: ${e.getMessage}"))

  log("INFO", s"  OK      (root/$PARTITION_KEY=<X>, exists)      : ${partOk.length}")
  log("INFO", s"  NESTED  (points inside a wrapper)  : ${partNested.length}")
  log("INFO", s"  ORPHAN  (location gone from HDFS)  : ${partOrphan.length}")
  if (partOutside.nonEmpty)
    log("WARN", s"  OUTSIDE (location not under root)  : ${partOutside.length}")

  partNested.foreach  { case (spec, loc) => log("NESTED", s"$spec -> $loc   (re-pointed after flattening)") }
  partOrphan.foreach  { case (spec, loc) => log("ORPHAN", s"$spec -> $loc   (location missing on HDFS)") }
  partOutside.foreach { case (spec, loc) => log("WARN", s"$spec -> $loc   (outside the table root - NOT touched)") }

  // --- cross-check disk vs metastore ----------------------------------
  val diskValues = (canonicalValues ++ plans.map(_.value)).toSet
  // drop_empty carries no data, so "unregistered" would be misleading: there
  // is nothing to ADD PARTITION for it.
  plans.filter(p => p.kind != "drop_empty" && !registeredValues.contains(p.value))
       .foreach(p => unregistered += ((p.wrapper.toString, p.value, "wrapper")))
  canonicalValues.zip(canonical).filter { case (v, _) => !registeredValues.contains(v) }
       .foreach { case (v, full) => unregistered += ((full, v, "canonical dir")) }

  log("INFO", s"cross-check: ${diskValues.size} run id(s) on disk, " +
              s"${registeredValues.size} registered in the metastore")
  if (unregistered.nonEmpty) {
    log("WARN", s"*** ${unregistered.length} run id(s) exist on HDFS but are NOT registered ***")
    unregistered.foreach { case (full, v, kind) =>
      log("WARN", s"  UNREGISTERED $kind $PARTITION_KEY=$v -> $full  (a run wrote data " +
                  "that was never registered)")
    }
  }
} else {
  ddlExecutionAllowed = false
  log("WARN", "METASTORE_PREFLIGHT=false -> table-type guard and partition inventory " +
              "skipped, DDL will not be executed")
}

// ---------------------------------------------------------------------
// 6. Plan
// ---------------------------------------------------------------------

section("3/7  PLANNED ACTIONS")

if (plans.isEmpty)
  log("INFO", "nothing to flatten - the layout is already canonical (idempotent no-op)")

plans.foreach { p =>
  p.kind match {
    case "rename" =>
      log("PLAN", s"RENAME ${p.inner} -> ${p.target}   (${p.files} file(s), ${human(p.bytes)})")
      log("PLAN", s"DELETE ${p.wrapper}   (empty wrapper)")
    case "merge" =>
      val why = if (p.selfNested) "self-nested" else "target already exists"
      log("PLAN", s"MERGE  ${p.files} file(s) (${human(p.bytes)}) ${p.inner} -> ${p.target}   [$why]")
      log("PLAN", s"DELETE ${p.inner}   (emptied nested dir)")
      if (!p.selfNested) log("PLAN", s"DELETE ${p.wrapper}   (empty wrapper)")
      if (p.protectedNames.nonEmpty)
        log("PLAN", s"KEEP   protected entries in ${p.inner} : ${p.protectedNames.mkString(", ")}")
    case "drop_empty" =>
      log("PLAN", s"DELETE ${p.inner}   (nested dir is empty, no data at risk)")
      if (!p.selfNested) log("PLAN", s"DELETE ${p.wrapper}   (empty wrapper)")
    case _ =>
  }
}

// ---------------------------------------------------------------------
// 7. Execution
// ---------------------------------------------------------------------

section(s"4/7  EXECUTION  (mode=$MODE)")

val flattened = ArrayBuffer[String]()   // run ids successfully flattened -> feed the DDL
var cRename = 0; var cMerge = 0; var cDropEmpty = 0
var cFilesMoved = 0; var cWrappersDeleted = 0; var cFailed = 0

/** Delete `path` only after verifying it holds nothing at all. */
def deleteIfEmpty(path: Path, label: String): Boolean = {
  val remaining = fs.listStatus(path)
  if (remaining.nonEmpty) {
    log("KEEP", s"$label NOT deleted, still contains " +
                s"[${remaining.map(_.getPath.getName).mkString(", ")}] -- $path")
    false
  } else if (fs.delete(path, true)) {
    log("OK", s"DELETE $path   ($label)")
    true
  } else {
    log("ERROR", s"DELETE FAILED $path   ($label)")
    false
  }
}

if (DRY_RUN) {
  log("INFO", s"DRY_RUN=true -> zero mutation performed under $TABLE_ROOT")
  log("INFO", "review the plan above, then set DRY_RUN = false and re-run")
  flattened ++= plans.filter(p => p.kind == "rename" || p.kind == "merge").map(_.value)
} else {
  plans.foreach { p =>
    val wrapper = p.wrapper
    val inner   = p.inner
    val target  = p.target
    var ok      = true

    if (p.kind == "drop_empty") {
      cDropEmpty += 1
      deleteIfEmpty(inner, "empty nested dir")
      if (!p.selfNested && deleteIfEmpty(wrapper, "empty wrapper")) cWrappersDeleted += 1
    } else {
      if (p.kind == "rename") {
        // Re-check just before mutating: the plan was built earlier.
        if (fs.exists(target)) {
          log("WARN", s"target appeared since the scan, falling back to MERGE: $target")
          p.kind = "merge"
        } else if (fs.rename(inner, target)) {
          log("OK", s"RENAME $inner -> $target")
          cRename += 1
          cFilesMoved += p.files
        } else {
          ok = false
          cFailed += 1
          log("ERROR", s"RENAME FAILED $inner -> $target -- wrapper KEPT, no data lost")
        }
      }

      if (p.kind == "merge" && ok) {
        if (!fs.exists(target)) {
          fs.mkdirs(target)
          log("OK", s"MKDIR  $target")
        }
        var moved = 0
        ls(inner).foreach { st =>
          val src  = st.getPath
          val name = src.getName
          if (isProtected(name)) {
            if (DELETE_MARKERS_ON_MERGE) {
              if (fs.delete(src, true)) log("OK", s"DELETE marker $src")
              else log("ERROR", s"DELETE marker FAILED $src")
            } else {
              log("KEEP", s"protected entry left in place: $src")
            }
          } else {
            var dst = new Path(target, name)
            if (fs.exists(dst)) {
              val short = UUID.randomUUID().toString.replaceAll("-", "").substring(0, 8)
              dst = new Path(target, "merged_" + short + "_" + name)
              log("WARN", s"collision on $name -> renamed to ${dst.getName}")
            }
            if (fs.rename(src, dst)) {
              log("OK", s"MOVE   $src -> $dst")
              moved += 1
            } else {
              ok = false
              cFailed += 1
              log("ERROR", s"MOVE FAILED $src -> $dst")
            }
          }
        }
        cMerge += 1
        cFilesMoved += moved
        if (ok) deleteIfEmpty(inner, "emptied nested dir")
      }

      if (!ok) {
        skipped += ((wrapper.toString,
          "at least one rename failed - wrapper kept as is, no data lost"))
      } else if (p.selfNested) {
        flattened += p.value
      } else if (deleteIfEmpty(wrapper, "empty wrapper")) {
        cWrappersDeleted += 1
        flattened += p.value
      } else {
        skipped += ((wrapper.toString,
          "wrapper not empty after flattening - review (protected entries are kept " +
          "unless DELETE_MARKERS_ON_MERGE=true)"))
      }
    }
  }

  log("INFO", s"renamed=$cRename merged=$cMerge dropped_empty=$cDropEmpty " +
              s"files_moved=$cFilesMoved wrappers_deleted=$cWrappersDeleted failures=$cFailed")

  // Without this the next spark.read still sees the pre-move file index.
  spark.catalog.refreshByPath(TABLE_ROOT)
  log("INFO", s"Spark file index refreshed for $TABLE_ROOT")
}

// ---------------------------------------------------------------------
// 8. Hive re-registration DDL
//    partitionProvider=catalog -> explicit DDL is mandatory, MSCK REPAIR is
//    not an option.
// ---------------------------------------------------------------------

section(s"5/7  HIVE DDL  (table=$HIVE_TABLE)")

val ddlStatements = ArrayBuffer[String]()

if (EMIT_HIVE_DDL) {
  val unregisteredValues = unregistered.map(_._2).toSet

  flattened.distinct.sorted.foreach { value =>
    val loc = s"${TABLE_ROOT.stripSuffix("/")}/$PARTITION_KEY=$value"
    val tag =
      if (unregisteredValues.contains(value))
        "-- NOTE: this run id was NOT registered in the metastore before the fix\n"
      else ""
    ddlStatements += s"${tag}ALTER TABLE $HIVE_TABLE\n  DROP IF EXISTS PARTITION ($PARTITION_KEY_LC='$value');"
    ddlStatements += s"ALTER TABLE $HIVE_TABLE\n  ADD IF NOT EXISTS PARTITION ($PARTITION_KEY_LC='$value')\n  LOCATION '$loc';"
  }

  partOrphan.foreach { case (spec, loc) =>
    val i    = spec.indexOf('=')
    val pkey = if (i < 0) PARTITION_KEY else spec.substring(0, i)
    val pval = if (i < 0) spec else spec.substring(i + 1)
    ddlStatements += s"-- ORPHAN: $loc no longer exists on HDFS\n" +
                     s"ALTER TABLE $HIVE_TABLE\n  DROP IF EXISTS PARTITION ($pkey='$pval');"
  }

  // Canonical directories that were never registered: registering them is a
  // judgement call (they may be an aborted run), so they are emitted
  // commented out for a human to enable.
  val commented = unregistered.filter(_._3 == "canonical dir").map { case (full, value, _) =>
    "-- UNREGISTERED canonical dir, review before enabling:\n" +
    s"-- ALTER TABLE $HIVE_TABLE ADD IF NOT EXISTS PARTITION ($PARTITION_KEY_LC='$value') LOCATION '$full';"
  }

  val header =
    s"-- generated by flatten_nested_runid_partitions.scala on ${LocalDateTime.now()}\n" +
    s"-- table      : $HIVE_TABLE   (type: ${if (tableType == null) "UNKNOWN" else tableType})\n" +
    s"-- table root : $TABLE_ROOT\n" +
    s"-- mode       : $MODE\n" +
    s"-- flattened  : ${flattened.distinct.length} run id(s)   orphan : ${partOrphan.length}   " +
    s"unregistered : ${unregistered.length}\n"

  val all     = ddlStatements ++ commented
  val ddlText = header + "\n" + all.mkString("\n\n") + (if (all.nonEmpty) "\n" else "")

  println(ddlText)

  if (DDL_OUTPUT_PATH != null) {
    Try {
      val ddlFs   = FileSystem.get(new URI(DDL_OUTPUT_PATH), conf)
      val ddlPath = new Path(DDL_OUTPUT_PATH)
      val parent  = ddlPath.getParent
      if (parent != null && !ddlFs.exists(parent)) ddlFs.mkdirs(parent)
      val out = ddlFs.create(ddlPath, true)      // true = overwrite
      out.write(ddlText.getBytes("UTF-8"))
      out.close()
      log("OK", s"DDL written to $DDL_OUTPUT_PATH")
    }.failed.foreach(e =>
      log("WARN", s"could not write the DDL file ($DDL_OUTPUT_PATH): ${e.getMessage}"))
  }

  if (DRY_RUN) {
    log("INFO", "DRY_RUN=true -> the DDL above was NOT executed")
  } else if (!ddlExecutionAllowed) {
    log("ERROR", "DDL NOT executed: the table type could not be confirmed EXTERNAL. " +
                 s"Review $DDL_OUTPUT_PATH and replay it by hand.")
  } else {
    ddlStatements.foreach { stmt =>
      val sql = stmt.split("\n").filterNot(_.startsWith("--")).mkString("\n").trim.stripSuffix(";")
      if (sql.nonEmpty) {
        val flat = sql.replaceAll("\n", " ")
        Try {
          spark.sql(sql)
          log("OK", "EXEC   " + flat)
        }.failed.foreach { e =>
          cFailed += 1
          log("ERROR", "SQL FAILED (" + flat + "): " + e.getMessage)
        }
      }
    }
  }
}

// ---------------------------------------------------------------------
// 9. Validation
// ---------------------------------------------------------------------

section("6/7  VALIDATION")

val remainingWrappers = ls(root).filter { st =>
  st.isDirectory && !isProtected(st.getPath.getName) && {
    val (k, v) = splitKey(st.getPath.getName)
    v.isDefined && k != PARTITION_KEY
  }
}.map(_.getPath.toString)

// A wrapper the script deliberately skipped (ambiguous case) is EXPECTED to
// still be there: it needs a human. A wrapper the script planned to flatten
// and that is still there is a real failure.
val plannedPaths = plans.map(_.wrapper.toString).toSet
val skippedPaths = skipped.map(_._1).toSet

val unresolved    = remainingWrappers.filter(w => plannedPaths.contains(w) && !DRY_RUN)
val leftByDesign  = remainingWrappers.filterNot(unresolved.contains)

if (remainingWrappers.isEmpty) {
  log("OK", "no non-canonical first-level partition directory left under the table root")
} else {
  log("WARN", s"${remainingWrappers.length} first-level dir(s) still use a non-canonical key")
  leftByDesign.foreach { rp =>
    val why =
      if (DRY_RUN) "planned, dry-run"
      else if (skippedPaths.contains(rp)) "deliberately skipped - see the SKIPPED table below"
      else "not handled - see the report below"
    log("WARN", s"  $rp   ($why)")
  }
  unresolved.foreach(rp =>
    log("ERROR", s"  $rp   (was planned for flattening and is STILL there)"))
}

if (DRY_RUN) {
  log("INFO", "DRY_RUN=true -> Spark/Hive read validation skipped")
} else {
  // These reads legitimately fail while any non-canonical wrapper survives:
  // report the error, never hide the final report behind a stack trace.
  Try {
    val n = spark.read.orc(TABLE_ROOT).select(PARTITION_KEY).distinct().count()
    log("OK", s"spark.read.orc(TABLE_ROOT) distinct $PARTITION_KEY = $n")
  }.failed.foreach { e =>
    log("ERROR", s"spark.read.orc(TABLE_ROOT) failed: ${e.getMessage}")
    if (remainingWrappers.nonEmpty)
      log("ERROR", "-> expected while the wrappers listed above survive; resolve them " +
                   "manually and re-run")
  }
  Try {
    spark.sql(s"SELECT $PARTITION_KEY, count(*) AS n FROM $HIVE_TABLE GROUP BY $PARTITION_KEY")
         .show(100, false)
  }.failed.foreach(e => log("ERROR", s"count per $PARTITION_KEY failed: ${e.getMessage}"))
}

// ---------------------------------------------------------------------
// 10. Report
// ---------------------------------------------------------------------

section(s"7/7  REPORT  (mode=$MODE)")

def planCount(kind: String): Int = plans.count(_.kind == kind)

println(s"table root            : $TABLE_ROOT")
println(s"hive table            : $HIVE_TABLE  (type: ${if (tableType == null) "UNKNOWN" else tableType})")
println(s"canonical partitions  : ${canonical.length}")
println(s"wrappers found        : ${plans.length}")
println(s"  by RENAME           : ${if (DRY_RUN) planCount("rename") else cRename}")
println(s"  by MERGE            : ${if (DRY_RUN) planCount("merge") else cMerge}")
println(s"  empty nested dropped: ${if (DRY_RUN) planCount("drop_empty") else cDropEmpty}")
println(s"files moved           : $cFilesMoved")
println(s"wrappers deleted      : $cWrappersDeleted")
println(s"failures              : $cFailed")
println(s"skipped               : ${skipped.length}")
println(s"protected entries     : ${protectedSeen.length}")
println(s"stray root entries    : ${strays.length}")
println(s"registered partitions : ${registeredValues.size}  (OK ${partOk.length} / " +
        s"NESTED ${partNested.length} / ORPHAN ${partOrphan.length} / OUTSIDE ${partOutside.length})")
println(s"unregistered on disk  : ${unregistered.length}")
println(s"DDL statements        : ${ddlStatements.length} " +
        s"${if (DRY_RUN) "(printed only, DRY_RUN)" else "(executed)"}")

if (skipped.nonEmpty) {
  println("")
  println("SKIPPED -- nothing was modified for these, they need a human decision")
  println("-" * 100)
  skipped.foreach { case (sp, reason) => println(s"  $sp\n      reason: $reason") }
}

if (unregistered.nonEmpty) {
  println("")
  println("UNREGISTERED ON DISK -- data exists on HDFS but no metastore partition")
  println("-" * 100)
  unregistered.foreach { case (full, v, kind) =>
    println(s"  $PARTITION_KEY=$v  ($kind)\n      $full") }
}

if (partNested.nonEmpty) {
  println("")
  println("NESTED METASTORE PARTITIONS -- location pointed inside a wrapper")
  println("-" * 100)
  partNested.foreach { case (spec, loc) => println(s"  $spec -> $loc") }
}

if (partOrphan.nonEmpty) {
  println("")
  println("ORPHAN METASTORE PARTITIONS -- location missing on HDFS")
  println("-" * 100)
  partOrphan.foreach { case (spec, loc) => println(s"  $spec -> $loc") }
}

if (partOutside.nonEmpty) {
  println("")
  println("OUTSIDE METASTORE PARTITIONS -- location not under the table root")
  println("-" * 100)
  partOutside.foreach { case (spec, loc) => println(s"  $spec -> $loc") }
}

if (protectedSeen.nonEmpty) {
  println("")
  println("PROTECTED ENTRIES -- never moved file-by-file, never deleted")
  println("-" * 100)
  protectedSeen.foreach { case (sp, reason) => println(s"  $sp\n      $reason") }
}

if (strays.nonEmpty) {
  println("")
  println("STRAY ENTRIES AT THE TABLE ROOT -- out of scope, review manually")
  println("-" * 100)
  strays.foreach { case (sp, reason) => println(s"  $sp\n      $reason") }
}

println("")
if (DRY_RUN)
  println(">>> DRY RUN: nothing was changed. Review the plan and the DDL, then set " +
          "DRY_RUN = false and run again.")
else
  println(">>> APPLIED. Re-run with DRY_RUN = true: it must report 0 wrapper found " +
          "(idempotency check).")

if (leftByDesign.nonEmpty && !DRY_RUN) {
  println("")
  println(s">>> MANUAL ACTION REQUIRED: ${leftByDesign.length} non-canonical first-level " +
          "director(ies) are still present.")
  println("    They were skipped on purpose (see the SKIPPED table). Until they are " +
          "resolved by hand,")
  println(s"    spark.read.orc('$TABLE_ROOT') still sees mixed partition depths.")
}

// Raised last, once the whole report has been printed: only for wrappers the
// script actually tried to flatten and failed to.
require(unresolved.isEmpty,
  s"${unresolved.length} wrapper(s) were planned for flattening but are still present - " +
  s"see the ERROR lines above: ${unresolved.mkString(", ")}")
