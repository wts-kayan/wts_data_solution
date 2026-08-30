// =====================================================================
//  rename_partitions_to_runId.scala          (Spark-Scala -- drop-in cell)
//
//  README -- HOW TO RUN
//  --------------------
//  Paste as a cell of the existing Dataiku Spark-Scala notebook. It REUSES
//  the `spark` / `sparkContext` vals bound by cell 1 -- do NOT re-create
//  the SparkSession.
//
//    1. Set TABLE_ROOT / HIVE_TABLE in the CONFIG block below.
//    2. Run with DRY_RUN = true (the default). NOTHING is modified.
//    3. Review the plan and the generated .sql, then set DRY_RUN = false.
//    4. Re-run once more: idempotent, the second run must report 0 rename.
//
//  Exact twin of rename_partitions_to_runId.py: same logic, same report,
//  same .sql output.
//
//  WHAT IT DOES
//  ------------
//  Makes the on-disk partition directory casing uniformly `runId=<uuid>`
//  (camelCase I), and re-points the metastore at the new locations:
//
//      runid=<uuid>/   ->   runId=<uuid>/          (HDFS, atomic rename)
//      ALTER TABLE ... DROP PARTITION (runid='<uuid>');
//      ALTER TABLE ... ADD  PARTITION (runid='<uuid>')
//                     LOCATION '<root>/runId=<uuid>';
//
//  The METASTORE COLUMN STAYS LOWERCASE `runid`. Only the directory name
//  changes. That combination is legal because the table is registered with
//  spark.sql.partitionProvider=catalog: Spark reads partitions from the
//  metastore with an explicit LOCATION per partition, so the directory name
//  does not have to match the column name.
//
//  READ THIS BEFORE RUNNING
//  ------------------------
//  `runId=` on disk is only stable while EVERY writer builds its paths by
//  hand. Anything that writes through the Hive table -- INSERT INTO,
//  df.write.saveAsTable(...).partitionBy("runid"), dynamic partition
//  overwrite -- names the directory after the CATALOG column, i.e.
//  lowercase `runid=`. One such write re-introduces the mixed casing.
//  If that is a risk in your pipelines, prefer the opposite direction:
//  flatten_nested_runid_partitions.scala normalises everything to lowercase
//  `runid=`, which is what Spark itself produces.
//
//  PRE-REQUISITE
//  -------------
//  The tree must already be FLAT. If any first-level directory still holds
//  a nested runid= directory (the double-nesting defect), this script
//  refuses to touch it and tells you to run
//  flatten_nested_runid_partitions.scala first.
//
//  SAFETY
//  ------
//  UAT data with no backup: HDFS renames are metadata-only, every action is
//  logged with its full path, and every ambiguous case is skipped and
//  reported. The table must be EXTERNAL -- DROP PARTITION on a MANAGED
//  table would delete the data. The script aborts if it is not.
// =====================================================================

import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
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

// Desired on-disk directory key (case-sensitive) and the metastore column
// name (Hive stores partition column names lowercase -- do NOT change it).
val TARGET_KEY  = "runId"
val CATALOG_KEY = "runid"

val DDL_OUTPUT_PATH =
  "/Projects/STCreditRisk_UAT/tmp/generated_rename_partition_ddl.sql"

// When both runid=<X> and runId=<X> exist, move the files of the lowercase
// one into the camelCase one (renaming on collision) instead of skipping.
val MERGE_ON_COLLISION = true

// Markers are moved with the directory on the atomic-rename path. On the
// merge path they are left in place unless this is true.
val DELETE_MARKERS_ON_MERGE = false

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
// ---------------------------------------------------------------------

val conf = sparkContext.hadoopConfiguration
val fs   = FileSystem.get(new URI(TABLE_ROOT), conf)
val root = new Path(TABLE_ROOT)

require(fs.exists(root), s"Table root does not exist: $TABLE_ROOT")

def ls(path: Path): Array[FileStatus] = fs.listStatus(path).sortBy(_.getPath.getName)

def isProtected(name: String): Boolean = name.startsWith("_") || name.startsWith(".")

def splitKey(name: String): (String, Option[String]) = {
  val i = name.indexOf('=')
  if (i < 0) (name, None) else (name.substring(0, i), Some(name.substring(i + 1)))
}

def pathOnly(uri: String): String = {
  val u = uri.stripSuffix("/")
  val i = u.indexOf("://")
  if (i >= 0) {
    val j = u.indexOf("/", i + 3)
    if (j >= 0) u.substring(j) else "/"
  } else {
    // scheme:/path -- a URI with a scheme but NO authority, which is what
    // Hive reports for e.g. file:/a/b. Without this branch such a location is
    // never recognised as living under a root written file:///a.
    val c = u.indexOf(":/")
    // the scheme must be longer than one char so a Windows drive letter
    // (C:/...) is not mistaken for a URI scheme
    if (c > 1 && !u.substring(0, c).contains("/")) u.substring(c + 1) else u
  }
}

val ROOT_PATH_ONLY = pathOnly(TABLE_ROOT)

def human(nbytes: Long): String = {
  var v = nbytes.toDouble
  val units = Array("B", "KB", "MB", "GB", "TB")
  var i = 0
  while (v >= 1024.0 && i < units.length - 1) { v /= 1024.0; i += 1 }
  f"$v%.2f ${units(i)}"
}

case class RenamePlan(var kind: String, src: Path, target: Path, value: String,
                      srcKey: String, files: Int, bytes: Long,
                      protectedNames: Seq[String])

// ---------------------------------------------------------------------
// 4. Discovery (read-only)
// ---------------------------------------------------------------------

section(s"1/6  DISCOVERY  (mode=$MODE)  root=$TABLE_ROOT")
log("INFO", s"target on-disk key = '$TARGET_KEY='   metastore column = '$CATALOG_KEY'")

val plans         = ArrayBuffer[RenamePlan]()
val already       = ArrayBuffer[(String, String)]()
val skipped       = ArrayBuffer[(String, String)]()
val strays        = ArrayBuffer[(String, String)]()
val protectedSeen = ArrayBuffer[(String, String)]()

ls(root).foreach { st =>
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
    } else if (key.toLowerCase != CATALOG_KEY) {
      strays += ((full, s"unrelated partition key '$key=' - not touched"))
    } else {
      val value     = valueOpt.get
      val children  = ls(st.getPath)
      val childDirs = children.filter(c => c.isDirectory && !isProtected(c.getPath.getName))
      val nested    = childDirs.filter(c =>
        splitKey(c.getPath.getName)._1.toLowerCase == CATALOG_KEY)

      if (nested.nonEmpty) {
        // Renaming a wrapper would only move the broken nesting under a new name.
        skipped += ((full, s"still holds a nested '${nested(0).getPath.getName}' dir - " +
          "run flatten_nested_runid_partitions.scala FIRST, then re-run this script"))
      } else if (childDirs.nonEmpty) {
        skipped += ((full, s"contains sub-directories " +
          s"(${childDirs.map(_.getPath.getName).mkString(", ")}) - unexpected layout, " +
          "review manually"))
      } else if (key == TARGET_KEY) {
        already += ((full, value))
      } else {
        val target       = new Path(root, s"$TARGET_KEY=$value")
        val targetExists = fs.exists(target)
        val data   = children.filter(c => !isProtected(c.getPath.getName))
        val prot   = children.filter(c => isProtected(c.getPath.getName))
        val nbytes = data.filter(!_.isDirectory).map(_.getLen).sum

        if (targetExists && !MERGE_ON_COLLISION) {
          skipped += ((full, s"target $target already exists and MERGE_ON_COLLISION=false"))
        } else {
          prot.foreach(c => protectedSeen += ((c.getPath.toString, s"marker inside $name")))
          plans += RenamePlan(if (targetExists) "merge" else "rename",
                              st.getPath, target, value, key, data.length, nbytes,
                              prot.map(_.getPath.getName).toSeq)
        }
      }
    }
  }
}

log("INFO", s"already '$TARGET_KEY=' partitions : ${already.length}")
log("INFO", s"to rename                : ${plans.count(_.kind == "rename")}")
log("INFO", s"to merge (both cases)    : ${plans.count(_.kind == "merge")}")
log("INFO", s"skipped                  : ${skipped.length}")
log("INFO", s"stray entries            : ${strays.length}")

// ---------------------------------------------------------------------
// 5. Metastore pre-flight
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

section(s"2/6  METASTORE PRE-FLIGHT  (table=$HIVE_TABLE)")

var tableType: String = null
val registered = LinkedHashMap[String, String]()
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
  log("ERROR", "-> the HDFS renames below can still run (they need no metastore),")
  log("ERROR", "   but NO partition DDL will be executed. Fix the table name or the")
  log("ERROR", "   Hive support first if you want the metastore re-pointed.")
} else if (METASTORE_PREFLIGHT) {
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
    sys.error(s"ABORT: $HIVE_TABLE is a MANAGED_TABLE. ALTER TABLE ... DROP PARTITION " +
              "would DELETE the underlying HDFS data. Nothing was modified.")
  }
  if (tableType == "EXTERNAL") {
    log("OK", "table type confirmed EXTERNAL -> DROP PARTITION is metastore-only")
  } else {
    ddlExecutionAllowed = false
    log("ERROR", "could not confirm the table type is EXTERNAL. The HDFS renames will " +
                 "still run, but NO DDL will be executed.")
  }

  Try {
    val specs = spark.sql(s"SHOW PARTITIONS $HIVE_TABLE").collect().map(_.get(0).toString)
    specs.foreach { spec =>
      val i    = spec.indexOf('=')
      val pkey = if (i < 0) CATALOG_KEY else spec.substring(0, i)
      val pval = if (i < 0) spec else spec.substring(i + 1)
      val rowsTry = Try(describeRows(
        s"DESCRIBE FORMATTED $HIVE_TABLE PARTITION ($pkey='$pval')"))
      if (rowsTry.isFailure)
        log("WARN", s"DESCRIBE failed for $spec : ${rowsTry.failed.get.getMessage}")
      else
        rowsTry.get.find { case (c, _) => c.trim.stripSuffix(":") == "Location" }
               .foreach { case (_, v) => registered(pval) = v.trim }
    }
    log("INFO", s"registered partitions: ${registered.size}")
  }.failed.foreach(e => log("WARN", s"SHOW PARTITIONS failed: ${e.getMessage}"))

  val notRegistered = plans.map(_.value).filterNot(registered.contains).sorted
  if (notRegistered.nonEmpty) {
    log("WARN", s"${notRegistered.length} director(ies) being renamed have NO metastore " +
                "partition; they will be ADDed as new")
    notRegistered.foreach(v => log("WARN", s"  unregistered $CATALOG_KEY=$v"))
  }
} else {
  ddlExecutionAllowed = false
  log("WARN", "METASTORE_PREFLIGHT=false -> no table-type guard, DDL not executed")
}

// ---------------------------------------------------------------------
// 6. Plan
// ---------------------------------------------------------------------

section("3/6  PLANNED ACTIONS")

if (plans.isEmpty)
  log("INFO", s"nothing to rename - every partition dir already uses '$TARGET_KEY=' " +
              "(idempotent no-op)")

plans.foreach { p =>
  if (p.kind == "rename") {
    log("PLAN", s"RENAME ${p.src} -> ${p.target}   (${p.files} file(s), ${human(p.bytes)})")
  } else {
    log("PLAN", s"MERGE  ${p.files} file(s) (${human(p.bytes)}) ${p.src} -> ${p.target}   " +
                "[both casings exist]")
    log("PLAN", s"DELETE ${p.src}   (emptied source dir)")
    if (p.protectedNames.nonEmpty)
      log("PLAN", s"KEEP   protected entries in ${p.src} : ${p.protectedNames.mkString(", ")}")
  }
}

// ---------------------------------------------------------------------
// 7. Execution
// ---------------------------------------------------------------------

section(s"4/6  EXECUTION  (mode=$MODE)")

val renamedValues = ArrayBuffer[String]()
var cRename = 0; var cMerge = 0; var cFilesMoved = 0; var cDirsDeleted = 0; var cFailed = 0

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
  renamedValues ++= plans.map(_.value)
} else {
  plans.foreach { p =>
    val src    = p.src
    val target = p.target
    var ok     = true

    if (p.kind == "rename") {
      // fs.rename into an EXISTING directory would move src INSIDE it, so the
      // existence check is mandatory, not an optimisation.
      if (fs.exists(target)) {
        log("WARN", s"target appeared since the scan, falling back to MERGE: $target")
        p.kind = "merge"
      } else if (fs.rename(src, target)) {
        log("OK", s"RENAME $src -> $target")
        cRename += 1
        cFilesMoved += p.files
      } else {
        ok = false
        cFailed += 1
        log("ERROR", s"RENAME FAILED $src -> $target -- source KEPT, no data lost")
      }
    }

    if (p.kind == "merge" && ok) {
      var moved = 0
      ls(src).foreach { st =>
        val s    = st.getPath
        val name = s.getName
        if (isProtected(name)) {
          if (DELETE_MARKERS_ON_MERGE) {
            if (fs.delete(s, true)) log("OK", s"DELETE marker $s")
            else log("ERROR", s"DELETE marker FAILED $s")
          } else {
            log("KEEP", s"protected entry left in place: $s")
          }
        } else {
          var dst = new Path(target, name)
          if (fs.exists(dst)) {
            val short = UUID.randomUUID().toString.replaceAll("-", "").substring(0, 8)
            dst = new Path(target, "merged_" + short + "_" + name)
            log("WARN", s"collision on $name -> renamed to ${dst.getName}")
          }
          if (fs.rename(s, dst)) {
            log("OK", s"MOVE   $s -> $dst")
            moved += 1
          } else {
            ok = false
            cFailed += 1
            log("ERROR", s"MOVE FAILED $s -> $dst")
          }
        }
      }
      cMerge += 1
      cFilesMoved += moved
      if (ok && deleteIfEmpty(src, "emptied source dir")) cDirsDeleted += 1
    }

    if (ok) renamedValues += p.value
    else skipped += ((src.toString, "at least one rename failed - kept as is"))
  }

  log("INFO", s"renamed=$cRename merged=$cMerge files_moved=$cFilesMoved " +
              s"dirs_deleted=$cDirsDeleted failures=$cFailed")
  spark.catalog.refreshByPath(TABLE_ROOT)
  log("INFO", s"Spark file index refreshed for $TABLE_ROOT")
}

// ---------------------------------------------------------------------
// 8. Hive re-registration DDL
// ---------------------------------------------------------------------

section(s"5/6  HIVE DDL  (table=$HIVE_TABLE)")

val ddlStatements = ArrayBuffer[String]()

if (EMIT_HIVE_DDL) {
  // Every partition must end up pointing at <root>/runId=<X>, including the
  // ones already named correctly on disk whose metastore LOCATION says otherwise.
  val toRegister = scala.collection.mutable.Set[String]()
  renamedValues.foreach(toRegister += _)
  already.foreach { case (_, v) =>
    val expected = s"$ROOT_PATH_ONLY/$TARGET_KEY=$v"
    registered.get(v) match {
      case Some(loc) if pathOnly(loc) == expected => // already correct
      case _                                      => toRegister += v
    }
  }

  toRegister.toSeq.sorted.foreach { value =>
    val loc = s"${TABLE_ROOT.stripSuffix("/")}/$TARGET_KEY=$value"
    ddlStatements += s"ALTER TABLE $HIVE_TABLE\n  DROP IF EXISTS PARTITION ($CATALOG_KEY='$value');"
    ddlStatements += s"ALTER TABLE $HIVE_TABLE\n  ADD IF NOT EXISTS PARTITION ($CATALOG_KEY='$value')\n  LOCATION '$loc';"
  }

  val header =
    s"-- generated by rename_partitions_to_runId.scala on ${LocalDateTime.now()}\n" +
    s"-- table      : $HIVE_TABLE   (type: ${if (tableType == null) "UNKNOWN" else tableType})\n" +
    s"-- table root : $TABLE_ROOT\n" +
    s"-- mode       : $MODE\n" +
    s"-- on-disk key: $TARGET_KEY=   metastore column: $CATALOG_KEY\n" +
    s"-- partitions : ${toRegister.size} re-pointed\n"
  val ddlText = header + "\n" + ddlStatements.mkString("\n\n") +
                (if (ddlStatements.nonEmpty) "\n" else "")
  println(ddlText)

  if (DDL_OUTPUT_PATH != null) {
    Try {
      val ddlFs   = FileSystem.get(new URI(DDL_OUTPUT_PATH), conf)
      val ddlPath = new Path(DDL_OUTPUT_PATH)
      val parent  = ddlPath.getParent
      if (parent != null && !ddlFs.exists(parent)) ddlFs.mkdirs(parent)
      val out = ddlFs.create(ddlPath, true)
      out.write(ddlText.getBytes("UTF-8"))
      out.close()
      log("OK", s"DDL written to $DDL_OUTPUT_PATH")
    }.failed.foreach(e => log("WARN", s"could not write the DDL file: ${e.getMessage}"))
  }

  if (DRY_RUN) {
    log("INFO", "DRY_RUN=true -> the DDL above was NOT executed")
  } else if (!ddlExecutionAllowed) {
    log("ERROR", "DDL NOT executed (table type not confirmed EXTERNAL). Replay " +
                 DDL_OUTPUT_PATH + " by hand.")
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
// 9. Validation + report
// ---------------------------------------------------------------------

section(s"6/6  VALIDATION & REPORT  (mode=$MODE)")

val wrongCase = ls(root).filter { st =>
  st.isDirectory && !isProtected(st.getPath.getName) && {
    val (k, v) = splitKey(st.getPath.getName)
    v.isDefined && k.toLowerCase == CATALOG_KEY && k != TARGET_KEY
  }
}.map(_.getPath.toString)

val plannedPaths = plans.map(_.src.toString).toSet
val unresolved   = wrongCase.filter(w => plannedPaths.contains(w) && !DRY_RUN)

if (wrongCase.isEmpty) {
  log("OK", s"every partition directory now uses '$TARGET_KEY='")
} else {
  log("WARN", s"${wrongCase.length} director(ies) still not named '$TARGET_KEY='")
  wrongCase.foreach(w => log("WARN", s"  $w"))
}

if (!DRY_RUN) {
  Try {
    spark.sql(s"SELECT $CATALOG_KEY, count(*) AS n FROM $HIVE_TABLE GROUP BY $CATALOG_KEY")
         .show(100, false)
  }.failed.foreach(e => log("ERROR", s"count per $CATALOG_KEY failed: ${e.getMessage}"))
}

println("")
println(s"table root            : $TABLE_ROOT")
println(s"hive table            : $HIVE_TABLE  (type: ${if (tableType == null) "UNKNOWN" else tableType})")
println(s"on-disk key           : $TARGET_KEY=   (metastore column: $CATALOG_KEY)")
println(s"already correct       : ${already.length}")
println(s"renamed               : ${if (DRY_RUN) plans.count(_.kind == "rename") else cRename}")
println(s"merged                : ${if (DRY_RUN) plans.count(_.kind == "merge") else cMerge}")
println(s"files moved           : $cFilesMoved")
println(s"failures              : $cFailed")
println(s"skipped               : ${skipped.length}")
println(s"stray entries         : ${strays.length}")
println(s"DDL statements        : ${ddlStatements.length} " +
        s"${if (DRY_RUN) "(printed only, DRY_RUN)" else "(executed)"}")

if (skipped.nonEmpty) {
  println("")
  println("SKIPPED -- nothing was modified, these need a human decision")
  println("-" * 100)
  skipped.foreach { case (sp, reason) => println(s"  $sp\n      reason: $reason") }
}

if (strays.nonEmpty) {
  println("")
  println("STRAY / UNRELATED ENTRIES -- not touched")
  println("-" * 100)
  strays.foreach { case (sp, reason) => println(s"  $sp\n      $reason") }
}

if (protectedSeen.nonEmpty) {
  println("")
  println("PROTECTED ENTRIES -- never moved file-by-file, never deleted")
  println("-" * 100)
  protectedSeen.foreach { case (sp, reason) => println(s"  $sp\n      $reason") }
}

println("")
if (DRY_RUN) {
  println(">>> DRY RUN: nothing was changed. Review the plan and the DDL, then set " +
          "DRY_RUN = false and run again.")
} else {
  println(">>> APPLIED. Re-run with DRY_RUN = true: it must report 0 rename " +
          "(idempotency check).")
  println(">>> REMINDER: a later INSERT INTO / saveAsTable through the Hive table will " +
          "create lowercase 'runid=' dirs again -- keep the writers building paths by hand.")
}

require(unresolved.isEmpty,
  s"${unresolved.length} director(ies) were planned for rename but are still " +
  s"wrong-cased: ${unresolved.mkString(", ")}")
