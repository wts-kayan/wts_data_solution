// =====================================================================
//  fix_all_runid_tables.scala               (Spark-Scala -- drop-in cell)
//
//  ###################################################################
//  #  DANGER -- this runs the recreate (DROP + CREATE) over MANY      #
//  #  tables. The data is EXTERNAL and is not touched, but every      #
//  #  table definition is destroyed and rebuilt. A per-table backup   #
//  #  is written BEFORE its own drop.                                 #
//  ###################################################################
//
//  README -- HOW TO RUN
//  --------------------
//  Paste as a cell of the existing Dataiku Spark-Scala notebook. It REUSES
//  the `spark` / `sparkContext` vals bound by cell 1 -- do NOT re-create
//  the SparkSession.
//
//    1. Run with DRY_RUN = true (the default). Nothing is modified. It
//       prints the discovered table list and, per table, the exact plan.
//    2. Review the list -- above all which tables are SKIPPED and why.
//    3. Set DRY_RUN = false and re-run.
//
//  WHAT THIS IS
//  ------------
//  The batch form of the two single-table cells that have already been run
//  successfully against dbprojection.term_structure:
//
//    rename_partitions_to_runId.scala   on-disk  runid=<X>  ->  runId=<X>
//    recreate_table_partcol_runid.scala Spark's  partCol.0  ->  runId
//
//  It finds every table in the database partitioned by `runid` and applies
//  the same two phases to each, in the same order, with the same guards.
//  The single-table cells remain the right tool for one table; this one is
//  for doing all of them without editing a config 60 times.
//
//  THE TWO PHASES, PER TABLE
//  -------------------------
//  Phase 1 RENAME   -- renames each on-disk `runid=<X>` directory to
//                      `runId=<X>` and re-points the metastore partition at
//                      the new path. Where both casings already exist the
//                      files are merged, and a name collision is resolved by
//                      renaming the incoming file, never by overwriting.
//  Phase 2 RECREATE -- captures the schema, the location and every partition
//                      location, writes a backup, DROPs the table definition
//                      and recreates it as a datasource table whose column
//                      list carries `runId`, then re-adds every partition.
//                      Spark then writes spark.sql.sources.schema.partCol.0
//                      itself -- it refuses any property key starting with
//                      'spark.sql.', so this is the only route from Spark.
//
//  Phase 1 runs first on purpose: phase 2 re-registers partitions at the
//  locations the metastore holds, so those must already point at `runId=`.
//
//  WHY THE HIVE COLUMN STAYS LOWERCASE
//  -----------------------------------
//  The metastore lowercases every column name. SHOW CREATE TABLE will keep
//  printing PARTITIONED BY (runid string) and SHOW PARTITIONS will keep
//  returning runid=<uuid> whatever is done here. Only Spark's own view
//  carries the camelCase -- and that is what decides the directory name
//  Spark writes next time. That is the whole point.
//
//  SCOPE
//  -----
//  A table is in scope when it has EXACTLY ONE partition column and that
//  column is `runid`. Everything else is skipped and reported: no partition
//  column, a different partition column, more than one, MANAGED, or
//  external.table.purge=true.
//
//  A table still carrying the double-nested `runId=<X>/runid=<X>/` defect is
//  SKIPPED, not guessed at -- run flatten_nested_runid_partitions.scala on
//  it first, then re-run this cell.
//
//  SAFETY
//  ------
//    * DRY_RUN defaults to true;
//    * per-table isolation: one table failing does not stop the others,
//      unless STOP_ON_FAILURE = true;
//    * MANAGED tables are never touched -- DROP would delete their data;
//    * external.table.purge=true is never touched -- DROP would delete the
//      data even though the table is EXTERNAL;
//    * the per-table backup is written BEFORE that table's drop, and a
//      failure to write it means that table is not dropped;
//    * if a table is gone after its recreate, the whole batch ABORTS -- the
//      first table to lose its definition is also the last;
//    * idempotent: a table already fully fixed is a no-op on the next run.
// =====================================================================

import java.net.URI
import java.time.LocalDateTime
import java.util.UUID

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.util.Try

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

// ---------------------------------------------------------------------
// 1. CONFIG
// ---------------------------------------------------------------------

val DRY_RUN = true                // must stay true until the plan is reviewed
val DB      = "dbprojection"

// The partition column AS SPARK SHOULD SEE IT. The Hive column is always
// the lowercase form -- the metastore allows nothing else.
val PARTITION_COL      = "runId"
val PARTITION_TYPE     = "string"
val HIVE_PARTITION_COL = PARTITION_COL.toLowerCase

// Phases. Both default on; turn one off to do a pass of just that phase.
val DO_RENAME   = true
val DO_RECREATE = true

// Optional comma-separated allow-list, e.g. "term_structure,chr_detailed".
// Empty means every in-scope table in the database.
val ONLY_TABLES = ""

// Stop the whole batch at the first table that fails, instead of carrying on
// and reporting it at the end.
val STOP_ON_FAILURE = false

// Phase-1 behaviour, same meaning as in rename_partitions_to_runId.scala.
val MERGE_ON_COLLISION      = true
val DELETE_MARKERS_ON_MERGE = false

// One .sql per table lands here: <table>_rename.sql, <table>_recreate.sql
// and <table>_backup.sql. Scheme-less, like the other cells.
val OUTPUT_DIR = "/Projects/STCreditRisk_UAT/tmp/fix_all_runid"

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

def subsection(title: String): Unit = {
  println("")
  println("-" * 100)
  println(title)
  println("-" * 100)
}

val MODE = if (DRY_RUN) "DRY-RUN" else "APPLY"

// ---------------------------------------------------------------------
// 3. Shared filesystem helpers  (identical to the single-table cells)
// ---------------------------------------------------------------------

val conf = sparkContext.hadoopConfiguration

def ls(fs: FileSystem, path: Path): Array[FileStatus] =
  fs.listStatus(path).sortBy(_.getPath.getName)

def isProtected(name: String): Boolean = name.startsWith("_") || name.startsWith(".")

def splitKey(name: String): (String, Option[String]) = {
  val i = name.indexOf('=')
  if (i < 0) (name, None) else (name.substring(0, i), Some(name.substring(i + 1)))
}

/** Strip scheme and authority from a location. Hive reports both
  * hdfs://nameservice/a/b and file:/a/b, and only comparing the path part
  * makes a partition recognisable as living under the table root. */
def pathOnly(uri: String): String = {
  val u = uri.stripSuffix("/")
  val i = u.indexOf("://")
  if (i >= 0) {
    val j = u.indexOf("/", i + 3)
    if (j >= 0) u.substring(j) else "/"
  } else {
    val c = u.indexOf(":/")
    // the scheme must be longer than one char, so a Windows drive letter
    // (C:/...) is not mistaken for a URI scheme
    if (c > 1 && !u.substring(0, c).contains("/")) u.substring(c + 1) else u
  }
}

def human(nbytes: Long): String = {
  var v = nbytes.toDouble
  val units = Array("B", "KB", "MB", "GB", "TB")
  var i = 0
  while (v >= 1024.0 && i < units.length - 1) { v /= 1024.0; i += 1 }
  f"$v%.2f ${units(i)}"
}

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
    true
  }.recover { case e =>
    log("ERROR", s"could not write $path : ${e.getMessage}")
    false
  }.get
}

def deleteIfEmpty(fs: FileSystem, path: Path, label: String): Boolean = {
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

// ---------------------------------------------------------------------
// 4. Discovery -- which tables are in scope
// ---------------------------------------------------------------------

section(s"1/4  DISCOVERY  (mode=$MODE)  database=$DB")

if (!Try(spark.sql(s"SHOW DATABASES LIKE '$DB'").collect().nonEmpty).getOrElse(false))
  sys.error(s"ABORT: database $DB is not visible to this Spark session. " +
            "Nothing was inspected and nothing was modified.")

val allTables: Seq[String] =
  Try(spark.sql(s"SHOW TABLES IN $DB").collect()
           .map(r => r.getAs[String]("tableName")).toSeq.sorted)
    .getOrElse(sys.error(s"ABORT: SHOW TABLES IN $DB failed. Nothing was modified."))

val onlyFilter = ONLY_TABLES.split(",").map(_.trim).filter(_.nonEmpty).toSet

case class Target(name: String, location: String, purge: String, tableType: String)

val targets  = ArrayBuffer[Target]()
val outOf    = ArrayBuffer[(String, String)]()   // (table, why)

allTables.foreach { t =>
  val fq = s"$DB.$t"
  if (onlyFilter.nonEmpty && !onlyFilter.contains(t)) {
    outOf += ((t, "not in ONLY_TABLES"))
  } else {
    Try(spark.sessionState.catalog.getTableMetadata(TableIdentifier(t, Some(DB)))).toOption match {
      case None =>
        outOf += ((t, "not readable from the catalog"))
      case Some(meta) =>
        val parts = meta.partitionColumnNames
        val purge = meta.properties.getOrElse("external.table.purge", "")
        val loc   = meta.storage.locationUri.map(_.toString).getOrElse("")
        if (parts.isEmpty)
          outOf += ((t, "not partitioned"))
        else if (parts.length > 1)
          outOf += ((t, s"${parts.length} partition columns (${parts.mkString(", ")}) - " +
                        "this cell only handles a single runid column"))
        else if (parts.head.toLowerCase != HIVE_PARTITION_COL)
          outOf += ((t, s"partitioned by '${parts.head}', not '$HIVE_PARTITION_COL'"))
        else if (meta.tableType == CatalogTableType.MANAGED)
          outOf += ((t, "MANAGED - DROP would delete the data, never touched"))
        else if (purge.trim.toLowerCase == "true")
          outOf += ((t, "external.table.purge=true - DROP would delete the data " +
                        "even though the table is EXTERNAL"))
        else if (loc.trim.isEmpty)
          outOf += ((t, "no location in the catalog - refusing to work blind"))
        else
          targets += Target(t, loc, purge, meta.tableType.name)
    }
  }
}

log("INFO", s"tables in $DB            : ${allTables.length}")
log("INFO", s"partitioned by $HIVE_PARTITION_COL, in scope : ${targets.length}")
log("INFO", s"out of scope             : ${outOf.length}")

targets.foreach(t => log("PLAN", s"IN SCOPE  $DB.${t.name}   ${t.location}"))
if (outOf.nonEmpty) {
  println("")
  println("OUT OF SCOPE -- not touched")
  println("-" * 100)
  outOf.foreach { case (t, why) => println(f"  $t%-46s $why") }
}

val unknownOnly = onlyFilter.diff(allTables.toSet)
if (unknownOnly.nonEmpty)
  log("WARN", s"ONLY_TABLES names ${unknownOnly.size} table(s) that do not exist in $DB: " +
              unknownOnly.toSeq.sorted.mkString(", ") + " - check for a typo")

if (targets.isEmpty)
  log("WARN", s"no table in $DB is partitioned by '$HIVE_PARTITION_COL' - nothing to do")

// ---------------------------------------------------------------------
// 5. Phase 1 -- rename runid=<X> to runId=<X>, per table
// ---------------------------------------------------------------------

case class RenameResult(renamed: Int, merged: Int, filesMoved: Int, dirsDeleted: Int,
                        repointed: Int, failed: Int, skipped: Seq[(String, String)],
                        nested: Seq[String])

/** Every `<key>=<X>/<key>=<Y>` nesting under the table root, read fresh from
  * disk. Phase 2 must never run against one of these: it registers partitions
  * at <root>/runId=<X> while the data sits a level deeper, so the partition
  * would read empty. Deliberately NOT derived from phase 1's result -- phase 1
  * can be turned off, and the guard has to survive that. */
def nestedDirsOf(t: Target): Seq[String] = {
  Try {
    val fs   = FileSystem.get(new URI(t.location), conf)
    val root = new Path(t.location)
    if (!fs.exists(root)) Seq.empty[String]
    else ls(fs, root).filter(st => st.isDirectory && !isProtected(st.getPath.getName))
      .filter(st => splitKey(st.getPath.getName)._1.toLowerCase == HIVE_PARTITION_COL)
      .flatMap { st =>
        ls(fs, st.getPath)
          .filter(c => c.isDirectory &&
                       splitKey(c.getPath.getName)._1.toLowerCase == HIVE_PARTITION_COL)
          .map(_.getPath.toString)
      }.toSeq
  }.getOrElse(Seq.empty[String])
}

case class RPlan(var kind: String, src: Path, target: Path, value: String,
                 files: Int, bytes: Long, protectedNames: Seq[String])

def renameOneTable(t: Target): RenameResult = {
  val fq         = s"$DB.${t.name}"
  val tableRoot  = t.location
  val fs         = FileSystem.get(new URI(tableRoot), conf)
  val root       = new Path(tableRoot)
  val rootPathOnly = pathOnly(tableRoot)

  if (!fs.exists(root))
    return RenameResult(0, 0, 0, 0, 0, 1,
                        Seq((tableRoot, "table root does not exist")), Seq.empty)

  val plans   = ArrayBuffer[RPlan]()
  val already = ArrayBuffer[String]()
  val skipped = ArrayBuffer[(String, String)]()
  val nested  = ArrayBuffer[String]()

  ls(fs, root).foreach { st =>
    val name = st.getPath.getName
    val full = st.getPath.toString
    if (isProtected(name)) {
      // markers at the table root are never touched
    } else if (!st.isDirectory) {
      skipped += ((full, "file directly under the table root"))
    } else {
      val (key, valueOpt) = splitKey(name)
      if (valueOpt.isEmpty) {
        skipped += ((full, "directory without a 'key=value' name"))
      } else if (key.toLowerCase != HIVE_PARTITION_COL) {
        skipped += ((full, s"unrelated partition key '$key=' - not touched"))
      } else {
        val value     = valueOpt.get
        val children  = ls(fs, st.getPath)
        val childDirs = children.filter(c => c.isDirectory && !isProtected(c.getPath.getName))
        val nestedDirs = childDirs.filter(c =>
          splitKey(c.getPath.getName)._1.toLowerCase == HIVE_PARTITION_COL)

        if (nestedDirs.nonEmpty) {
          // Renaming a wrapper would only move the broken nesting under a new
          // name. This is the flatten script's job, and it is not guessed at.
          nested += full
          skipped += ((full, s"still holds a nested '${nestedDirs(0).getPath.getName}' dir - " +
            "run flatten_nested_runid_partitions.scala on this table FIRST"))
        } else if (childDirs.nonEmpty) {
          skipped += ((full, s"contains sub-directories " +
            s"(${childDirs.map(_.getPath.getName).mkString(", ")}) - unexpected layout"))
        } else if (key == PARTITION_COL) {
          already += value
        } else {
          val target       = new Path(root, s"$PARTITION_COL=$value")
          val targetExists = fs.exists(target)
          val data   = children.filter(c => !isProtected(c.getPath.getName))
          val prot   = children.filter(c => isProtected(c.getPath.getName))
          val nbytes = data.filter(!_.isDirectory).map(_.getLen).sum
          if (targetExists && !MERGE_ON_COLLISION) {
            skipped += ((full, s"target $target exists and MERGE_ON_COLLISION=false"))
          } else {
            plans += RPlan(if (targetExists) "merge" else "rename", st.getPath, target,
                           value, data.length, nbytes, prot.map(_.getPath.getName).toSeq)
          }
        }
      }
    }
  }

  log("INFO", s"$fq : ${plans.count(_.kind == "rename")} to rename, " +
              s"${plans.count(_.kind == "merge")} to merge, ${already.length} already " +
              s"'$PARTITION_COL=', ${skipped.length} skipped")
  plans.foreach { p =>
    if (p.kind == "rename")
      log("PLAN", s"  RENAME ${p.src} -> ${p.target}  (${p.files} file(s), ${human(p.bytes)})")
    else
      log("PLAN", s"  MERGE  ${p.files} file(s) (${human(p.bytes)}) ${p.src} -> ${p.target}")
  }
  skipped.foreach { case (p, why) => log("WARN", s"  SKIP   $p : $why") }

  // --- read the registered partitions, to know what to re-point ------
  val registered = LinkedHashMap[String, String]()
  Try {
    spark.sql(s"SHOW PARTITIONS $fq").collect().map(_.get(0).toString).foreach { spec =>
      val i    = spec.indexOf('=')
      val pval = if (i < 0) spec else spec.substring(i + 1)
      Try {
        spark.sql(s"DESCRIBE FORMATTED $fq PARTITION ($HIVE_PARTITION_COL='$pval')")
             .collect()
             .find(r => Option(r.get(0)).map(_.toString.trim.stripSuffix(":")).contains("Location"))
             .foreach(r => registered(pval) = r.get(1).toString.trim)
      }
    }
  }.failed.foreach(e => log("WARN", s"$fq : SHOW PARTITIONS failed: ${e.getMessage}"))

  var cRename = 0; var cMerge = 0; var cMoved = 0; var cDeleted = 0; var cFailed = 0
  val renamedValues = ArrayBuffer[String]()

  if (!DRY_RUN) {
    plans.foreach { p =>
      var ok = true
      if (p.kind == "rename") {
        if (fs.exists(p.target)) {
          log("WARN", s"  target appeared since the scan, falling back to MERGE: ${p.target}")
          p.kind = "merge"
        } else if (fs.rename(p.src, p.target)) {
          cRename += 1
          log("OK", s"  RENAME ${p.src} -> ${p.target}")
        } else {
          ok = false; cFailed += 1
          log("ERROR", s"  RENAME FAILED ${p.src} -- source KEPT, no data lost")
        }
      }
      if (p.kind == "merge" && ok) {
        if (!fs.exists(p.target)) fs.mkdirs(p.target)
        ls(fs, p.src).foreach { st =>
          val src  = st.getPath
          val name = src.getName
          if (isProtected(name)) {
            if (DELETE_MARKERS_ON_MERGE) {
              if (fs.delete(src, true)) log("OK", s"  DELETE marker $src")
              else log("ERROR", s"  DELETE marker FAILED $src")
            } else log("KEEP", s"  protected entry left in place: $src")
          } else {
            var dst = new Path(p.target, name)
            if (fs.exists(dst)) {
              // HDFS rename onto an existing name fails, so the incoming file
              // is renamed rather than the existing one overwritten.
              val short = UUID.randomUUID().toString.replaceAll("-", "").substring(0, 8)
              dst = new Path(p.target, "merged_" + short + "_" + name)
              log("WARN", s"  collision on $name -> ${dst.getName}")
            }
            if (fs.rename(src, dst)) { cMoved += 1; log("OK", s"  MOVE   $src -> $dst") }
            else { ok = false; cFailed += 1; log("ERROR", s"  MOVE FAILED $src") }
          }
        }
        cMerge += 1
        if (ok && deleteIfEmpty(fs, p.src, "emptied source dir")) cDeleted += 1
      }
      if (ok) renamedValues += p.value
    }
    Try(spark.catalog.refreshByPath(tableRoot))
  }

  // --- re-point the metastore ----------------------------------------
  val toRegister = scala.collection.mutable.Set[String]()
  if (DRY_RUN) plans.foreach(p => toRegister += p.value)
  else renamedValues.foreach(toRegister += _)
  // a directory already named correctly whose LOCATION still says otherwise
  already.foreach { v =>
    val expected = s"$rootPathOnly/$PARTITION_COL=$v"
    registered.get(v) match {
      case Some(loc) if pathOnly(loc) == expected => ()
      case _                                      => toRegister += v
    }
  }

  val stmts = ArrayBuffer[String]()
  toRegister.toSeq.sorted.foreach { v =>
    val loc = s"${tableRoot.stripSuffix("/")}/$PARTITION_COL=$v"
    stmts += s"ALTER TABLE $fq DROP IF EXISTS PARTITION ($HIVE_PARTITION_COL='$v');"
    stmts += s"ALTER TABLE $fq ADD IF NOT EXISTS PARTITION ($HIVE_PARTITION_COL='$v') " +
             s"LOCATION '$loc';"
  }
  writeText(s"$OUTPUT_DIR/${t.name}_rename.sql",
    s"-- generated by fix_all_runid_tables.scala on ${LocalDateTime.now()}\n" +
    s"-- table: $fq   root: $tableRoot   mode: $MODE\n" +
    s"-- ${toRegister.size} partition(s) re-pointed\n\n" + stmts.mkString("\n") + "\n")

  var cRepointed = 0
  if (!DRY_RUN) {
    stmts.foreach { s =>
      val sql = s.stripSuffix(";")
      Try { spark.sql(sql); cRepointed += 1 }
        .failed.foreach { e =>
          cFailed += 1
          log("ERROR", s"  SQL FAILED ($sql): ${e.getMessage}")
        }
    }
  }

  RenameResult(cRename, cMerge, cMoved, cDeleted, cRepointed, cFailed,
               skipped.toSeq, nested.toSeq)
}

// ---------------------------------------------------------------------
// 6. Phase 2 -- recreate so Spark's partCol.0 carries the camelCase
// ---------------------------------------------------------------------

/** `failed` separates a refusal that needs attention (no backup, unreadable
  * schema, a DROP that did not work) from a legitimate skip (already correct,
  * or a dry run). Both leave `done` false, but only one is a problem. */
case class RecreateResult(done: Boolean, why: String, partitions: Int,
                          failed: Boolean = false)

// Properties Spark/Hive regenerate on their own -- never replay them.
val VOLATILE = Set("transient_lastDdlTime", "totalSize", "numFiles", "numRows",
                   "rawDataSize", "numPartitions", "COLUMN_STATS_ACCURATE",
                   "last_modified_time", "last_modified_by", "spark.sql.create.version")

def recreateOneTable(t: Target): RecreateResult = {
  val fq = s"$DB.${t.name}"

  val meta = Try(spark.sessionState.catalog
                      .getTableMetadata(TableIdentifier(t.name, Some(DB)))).toOption
  if (meta.isEmpty) return RecreateResult(false, "table not readable from the catalog", 0, failed = true)
  val m = meta.get

  // The guards, re-evaluated here rather than trusted from discovery: phase 1
  // ran in between.
  if (m.tableType == CatalogTableType.MANAGED)
    return RecreateResult(false, "MANAGED - DROP would delete the data", 0, failed = true)
  if (m.properties.getOrElse("external.table.purge", "").trim.toLowerCase == "true")
    return RecreateResult(false, "external.table.purge=true - DROP would delete the data", 0,
                          failed = true)

  val location = m.storage.locationUri.map(_.toString).getOrElse("")
  if (location.trim.isEmpty)
    return RecreateResult(false, "no location - refusing to recreate blind", 0, failed = true)

  // --- schema, in Spark's own JSON form ------------------------------
  var schemaJson: String = null
  Try { schemaJson = spark.table(fq).schema.json }
    .failed.foreach(e => log("WARN", s"$fq : schema via spark.table failed: ${e.getMessage}"))
  if (schemaJson == null)
    return RecreateResult(false, "the schema could not be captured in Spark's JSON form", 0,
                          failed = true)

  // The partition column must be present, and carry PARTITION_COL's casing.
  var st = DataType.fromJson(schemaJson).asInstanceOf[StructType]
  val hit = st.fieldNames.indexWhere(_.toLowerCase == HIVE_PARTITION_COL)
  if (hit < 0) {
    st = StructType(st.fields :+ StructField(PARTITION_COL, StringType, nullable = true))
    log("WARN", s"$fq : partition column was missing from the schema - appended")
  } else if (st.fieldNames(hit) != PARTITION_COL) {
    st = StructType(st.fields.updated(hit, st.fields(hit).copy(name = PARTITION_COL)))
  }
  val dataFields = st.fields.filter(_.name.toLowerCase != HIVE_PARTITION_COL)
  if (dataFields.isEmpty)
    return RecreateResult(false, "no data column could be read - recreating from an " +
                                 "empty schema would destroy the definition", 0, failed = true)

  // Already correct? Then this table is a no-op rather than a needless drop.
  val currentPartCol = m.properties.getOrElse("spark.sql.sources.schema.partCol.0", "")
  val alreadyOk = currentPartCol == PARTITION_COL
  if (alreadyOk) return RecreateResult(false, s"already at partCol.0 = $PARTITION_COL", 0)

  // --- every partition location --------------------------------------
  val partitions = ArrayBuffer[(String, String)]()
  Try {
    spark.sql(s"SHOW PARTITIONS $fq").collect().map(_.get(0).toString).foreach { spec =>
      val i    = spec.indexOf('=')
      val pval = if (i < 0) spec else spec.substring(i + 1)
      var loc: String = null
      Try {
        spark.sql(s"DESCRIBE FORMATTED $fq PARTITION ($HIVE_PARTITION_COL='$pval')")
             .collect()
             .find(r => Option(r.get(0)).map(_.toString.trim.stripSuffix(":")).contains("Location"))
             .foreach(r => loc = r.get(1).toString.trim)
      }
      if (loc == null) {
        loc = s"${location.stripSuffix("/")}/$PARTITION_COL=$pval"
        log("WARN", s"$fq : no location for $spec, assuming $loc")
      }
      partitions += ((pval, loc))
    }
  }.failed.foreach(e => log("WARN", s"$fq : SHOW PARTITIONS failed: ${e.getMessage}"))

  if (partitions.isEmpty)
    return RecreateResult(false, "no partition could be captured - dropping now would " +
                                 "lose every partition registration", 0, failed = true)

  // --- the DDL --------------------------------------------------------
  val colsDdl = dataFields.map(f => s"  `${f.name}` ${f.dataType.sql}").mkString(",\n")
  val createSql =
    s"CREATE TABLE $fq (\n$colsDdl,\n  `$PARTITION_COL` $PARTITION_TYPE\n)\n" +
    s"USING ORC\nPARTITIONED BY (`$PARTITION_COL`)\nLOCATION '$location'"
  val addSql = partitions.map { case (v, l) =>
    s"ALTER TABLE $fq ADD IF NOT EXISTS PARTITION ($HIVE_PARTITION_COL='$v') LOCATION '$l';"
  }

  val ddlText =
    s"-- generated by fix_all_runid_tables.scala on ${LocalDateTime.now()}\n" +
    s"-- table: $fq   type: ${m.tableType.name}   mode: $MODE\n" +
    s"-- ${partitions.length} partition(s) re-registered\n\n" +
    s"DROP TABLE IF EXISTS $fq;\n\n$createSql;\n\n" + addSql.mkString("\n") + "\n"
  writeText(s"$OUTPUT_DIR/${t.name}_recreate.sql", ddlText)

  // --- the backup, BEFORE the drop -----------------------------------
  val showCreate = Try(spark.sql(s"SHOW CREATE TABLE $fq")
                            .collect().map(_.get(0).toString).mkString("\n")).getOrElse("")
  val backupText =
    s"-- BACKUP of $fq taken on ${LocalDateTime.now()}\n" +
    "-- Replay this file to restore the definition if the recreate goes wrong.\n\n" +
    "-- ===== SHOW CREATE TABLE =====\n" +
    (if (showCreate.trim.isEmpty) "-- (unavailable)" else showCreate) + "\n\n" +
    "-- ===== TBLPROPERTIES =====\n" +
    m.properties.filterKeys(k => !VOLATILE.contains(k)).toSeq.sortBy(_._1)
     .map { case (k, v) => s"-- $k = $v" }.mkString("\n") + "\n\n" +
    s"-- ===== PARTITIONS (${partitions.length}) =====\n" +
    partitions.map { case (v, l) =>
      s"ALTER TABLE $fq ADD IF NOT EXISTS PARTITION ($HIVE_PARTITION_COL='$v') " +
      s"LOCATION '$l';" }.mkString("\n") + "\n"
  val backupOk = writeText(s"$OUTPUT_DIR/${t.name}_backup.sql", backupText)

  log("PLAN", s"  RECREATE $fq  (${dataFields.length} data col(s), " +
              s"${partitions.length} partition(s))  location kept: $location")

  if (DRY_RUN) return RecreateResult(false, "DRY_RUN", partitions.length)

  if (!backupOk)
    return RecreateResult(false, "the backup could not be written - refusing to DROP " +
                                 "without a replayable backup", partitions.length,
                          failed = true)

  var ok = true
  Try(spark.sql(s"DROP TABLE IF EXISTS $fq"))
    .failed.foreach { e => ok = false; log("ERROR", s"$fq : DROP failed: ${e.getMessage}") }
  if (!ok) return RecreateResult(false, "DROP failed - the table is untouched",
                                 partitions.length, failed = true)

  Try { spark.sql(createSql); log("OK", s"  CREATE $fq") }
    .failed.foreach { e =>
      ok = false
      log("ERROR", s"$fq : CREATE FAILED AFTER THE DROP: ${e.getMessage}")
    }
  if (!ok)
    sys.error(s"ABORT: $fq was DROPPED and the CREATE then failed. The table is GONE. " +
              s"Replay $OUTPUT_DIR/${t.name}_backup.sql to restore it. " +
              "The batch is stopped so no other table is touched.")

  var added = 0
  addSql.foreach { s =>
    Try { spark.sql(s.stripSuffix(";")); added += 1 }
      .failed.foreach(e => log("ERROR", s"$fq : ADD PARTITION failed: ${e.getMessage}"))
  }
  if (added != partitions.length)
    log("ERROR", s"$fq : only $added of ${partitions.length} partitions were re-registered " +
                 s"- replay $OUTPUT_DIR/${t.name}_recreate.sql")

  // The safety net: if the table is not there now, stop the whole batch.
  if (!Try(spark.catalog.tableExists(fq)).getOrElse(false))
    sys.error(s"ABORT: $fq does not exist after its recreate. Replay " +
              s"$OUTPUT_DIR/${t.name}_backup.sql. The batch is stopped.")

  RecreateResult(true, "", added)
}

// ---------------------------------------------------------------------
// 7. The loop
// ---------------------------------------------------------------------

section(s"2/4  PHASE 1+2 PER TABLE  (mode=$MODE)")

case class Outcome(table: String, renamed: Int, merged: Int, filesMoved: Int,
                   repointed: Int, recreated: Boolean, recreateWhy: String,
                   partitions: Int, failed: Int, nested: Int)

val outcomes = ArrayBuffer[Outcome]()

targets.foreach { t =>
  val fq = s"$DB.${t.name}"
  subsection(s"$fq   (${t.tableType}, location ${t.location})")

  var ren = RenameResult(0, 0, 0, 0, 0, 0, Seq.empty, Seq.empty)
  var rc = RecreateResult(false, "DO_RECREATE=false", 0)
  var hardFail = false

  if (DO_RENAME) {
    Try(ren = renameOneTable(t)).failed.foreach { e =>
      hardFail = true
      log("ERROR", s"$fq : phase 1 failed: ${e.getMessage}")
    }
  }

  // Read the nesting off DISK, after phase 1 and whether or not phase 1 ran.
  // A table still nested is not ready for phase 2: its partitions would be
  // re-registered at <root>/runId=<X> while the data sits a level deeper.
  val nestedNow = nestedDirsOf(t)

  if (!hardFail && nestedNow.nonEmpty) {
    log("WARN", s"$fq : ${nestedNow.length} nested dir(s) - phase 2 skipped, " +
                "run flatten_nested_runid_partitions.scala on this table first")
    nestedNow.foreach(n => log("WARN", s"  nested: $n"))
    rc = RecreateResult(false, "nested runid= dirs present - flatten it first", 0)
  } else if (!hardFail && ren.failed > 0) {
    // Half-fixed on disk. Recreating now would bake that half-state into a new
    // definition, so it waits for a clean phase 1.
    log("WARN", s"$fq : phase 1 had ${ren.failed} failure(s) - phase 2 skipped, " +
                "fix those and re-run")
    rc = RecreateResult(false, s"phase 1 had ${ren.failed} failure(s) - not recreated", 0)
  } else if (!hardFail && DO_RECREATE) {
    Try(rc = recreateOneTable(t)).failed.foreach { e =>
      // sys.error from inside recreateOneTable is the catastrophic case and
      // must not be swallowed: re-throw it and stop the batch.
      if (String.valueOf(e.getMessage).startsWith("ABORT:")) throw e
      hardFail = true
      log("ERROR", s"$fq : phase 2 failed: ${e.getMessage}")
    }
  }

  outcomes += Outcome(t.name, ren.renamed, ren.merged, ren.filesMoved, ren.repointed,
                      rc.done, rc.why, rc.partitions,
                      ren.failed + (if (hardFail) 1 else 0) + (if (rc.failed) 1 else 0),
                      nestedNow.length)

  if (STOP_ON_FAILURE && (hardFail || ren.failed > 0 || rc.failed))
    sys.error(s"ABORT: $fq failed and STOP_ON_FAILURE=true. " +
              s"${outcomes.length} table(s) had been processed.")
}

// ---------------------------------------------------------------------
// 8. Verification
// ---------------------------------------------------------------------

section(s"3/4  VERIFICATION  (mode=$MODE)")

if (DRY_RUN) {
  log("INFO", "DRY_RUN=true -> nothing was executed, so there is nothing to verify")
} else {
  targets.foreach { t =>
    val fq = s"$DB.${t.name}"
    val cols = Try(spark.table(fq).schema.fieldNames.toSeq).getOrElse(Seq.empty)
    if (cols.isEmpty)
      log("ERROR", s"$fq : the table cannot be read back")
    else if (cols.contains(PARTITION_COL))
      log("OK", s"$fq : Spark's schema carries '$PARTITION_COL'")
    else
      log("WARN", s"$fq : Spark's schema still has ${cols.mkString(", ")}")

    val fs = Try(FileSystem.get(new URI(t.location), conf)).toOption
    fs.foreach { f =>
      val wrong = Try(ls(f, new Path(t.location)).filter { st =>
        st.isDirectory && {
          val (k, v) = splitKey(st.getPath.getName)
          v.isDefined && k.toLowerCase == HIVE_PARTITION_COL && k != PARTITION_COL
        }
      }.length).getOrElse(-1)
      if (wrong > 0)
        log("WARN", s"$fq : $wrong director(ies) still use the wrong case on disk")
      else if (wrong == 0)
        log("OK", s"$fq : every partition directory uses '$PARTITION_COL='")
    }
  }
}

// ---------------------------------------------------------------------
// 9. Report
// ---------------------------------------------------------------------

section(s"4/4  REPORT  (mode=$MODE)")

println(s"database              : $DB")
println(s"mode                  : $MODE")
println(s"tables in $DB         : ${allTables.length}")
println(s"in scope              : ${targets.length}")
println(s"out of scope          : ${outOf.length}")
println(s"phases                : rename=$DO_RENAME recreate=$DO_RECREATE")
println(s"dirs renamed          : ${outcomes.map(_.renamed).sum}")
println(s"dirs merged           : ${outcomes.map(_.merged).sum}")
println(s"files moved           : ${outcomes.map(_.filesMoved).sum}")
println(s"partitions re-pointed : ${outcomes.map(_.repointed).sum}")
println(s"tables recreated      : ${outcomes.count(_.recreated)}")
println(s"tables with failures  : ${outcomes.count(_.failed > 0)}")
println(s"tables still nested   : ${outcomes.count(_.nested > 0)}")
println(s"artefacts             : $OUTPUT_DIR")

println("")
println(f"${"table"}%-46s ${"renamed"}%7s ${"merged"}%6s ${"repoint"}%7s " +
        f"${"recreated"}%9s  note")
println("-" * 110)
outcomes.foreach { o =>
  val note = if (o.nested > 0) s"NESTED x${o.nested} - flatten first"
             else if (o.failed > 0) "FAILURES - see the log above"
             else o.recreateWhy
  println(f"  ${o.table}%-44s ${o.renamed}%7d ${o.merged}%6d ${o.repointed}%7d " +
          f"${o.recreated}%9s  $note")
}

val nested = outcomes.filter(_.nested > 0).map(_.table)
if (nested.nonEmpty) {
  println("")
  println("STILL NESTED -- run flatten_nested_runid_partitions.scala on these, then re-run")
  println("-" * 100)
  nested.foreach(t => println(s"  $DB.$t"))
}

println("")
if (DRY_RUN)
  println(">>> DRY RUN: nothing was renamed, re-pointed or recreated. Review the list " +
          s"above and the .sql files in $OUTPUT_DIR, then set DRY_RUN = false.")
else
  println(s">>> APPLIED to ${targets.length} table(s). Re-run with DRY_RUN = true: it " +
          "must report 0 to rename and every table already at " + PARTITION_COL + ".")
