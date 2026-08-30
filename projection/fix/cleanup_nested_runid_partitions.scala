// =====================================================================
//  Cleanup of the spurious nested "runid=" partition directories
//  located UNDER the legitimate "runId=" partition directories of
//  dbprojection.term_structure
//
//  Observed (wrong) layout:
//    .../term_structure/runId=<uuid>/runid=<uuid>/part-*.orc
//
//  Expected (correct) layout:
//    .../term_structure/runId=<uuid>/part-*.orc
//
//  Run in a Dataiku Scala/Spark notebook. `spark` is already available.
//  Start with dryRun = true, read the report, then set dryRun = false.
//
//  The script is idempotent: if a nesting deeper than one level shows up
//  in the VERIFY section, simply run it again.
// =====================================================================

import java.net.URI
import scala.util.Try
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus, Trash}

// ---------------------------------------------------------------------
// 1. Configuration
// ---------------------------------------------------------------------
val tablePath =
  "hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/term_structure"

val dryRun          = true  // true  -> print only, change NOTHING on HDFS
val promoteFiles    = true  // true  -> move the content up into runId=... before deleting
                            // false -> delete the nested dir AND its content
val allowMismatched = false // true  -> also process nested dirs whose uuid differs from
                            //          the parent one. DANGEROUS: it merges run A into run B.
val useTrash        = true  // true  -> move to HDFS Trash (recoverable) instead of hard delete

// ---------------------------------------------------------------------
// 2. FileSystem handle
//    NB: do NOT call fs.close() - the handle comes from the shared cache
//        and is used by the Spark session itself.
// ---------------------------------------------------------------------
val conf = spark.sparkContext.hadoopConfiguration
val fs   = FileSystem.get(new URI(tablePath), conf)
val root = new Path(tablePath)

require(fs.exists(root), s"Table path does not exist: $tablePath")

// ---------------------------------------------------------------------
// 3. Detect the anomaly
//    NB: startsWith is case-sensitive in Scala, so "runid=" will NEVER
//        match "runId=". That is exactly what we rely on here.
// ---------------------------------------------------------------------
val rootEntries: Array[FileStatus] = fs.listStatus(root)

val parentPartitions: Array[FileStatus] = rootEntries
  .filter(_.isDirectory)
  .filter(_.getPath.getName.startsWith("runId="))
  .sortBy(_.getPath.getName)

println(s"[INFO] runId= partitions found : ${parentPartitions.length}")

// Anything at the root that is neither a runId= partition nor a hidden marker
// is worth a look (stray runid= siblings, leftover .hive-staging_*, ...).
val strayAtRoot = rootEntries.filterNot { s =>
  val n = s.getPath.getName
  n.startsWith("runId=") || n.startsWith("_") || n.startsWith(".")
}
if (strayAtRoot.nonEmpty) {
  println(s"[WARN] ${strayAtRoot.length} unexpected entrie(s) directly under the table path :")
  strayAtRoot.foreach(s => println(s"  ${if (s.isDirectory) "dir " else "file"} ${s.getPath.getName}"))
  println("[WARN] Not handled by this script - review them manually.")
}

case class Anomaly(parent: Path, nested: Path, files: Long, bytes: Long, valueMatches: Boolean)

val anomalies: Array[Anomaly] = parentPartitions.flatMap { parent =>
  val parentPath  = parent.getPath
  val parentValue = parentPath.getName.stripPrefix("runId=")

  fs.listStatus(parentPath)
    .filter(_.isDirectory)
    .filter(_.getPath.getName.startsWith("runid="))   // lowercase 'i' == the wrong one
    .map { nested =>
      val cs = fs.getContentSummary(nested.getPath)
      Anomaly(
        parent       = parentPath,
        nested       = nested.getPath,
        files        = cs.getFileCount,
        bytes        = cs.getLength,
        valueMatches = nested.getPath.getName.stripPrefix("runid=") == parentValue
      )
    }
}

println(s"[INFO] nested runid= directories found : ${anomalies.length}")
println("-" * 100)
anomalies.foreach { a =>
  println(f"  parent : ${a.parent.getName}")
  println(f"  nested : ${a.nested.getName}   files=${a.files}%5d  size=${a.bytes / 1024.0 / 1024.0}%10.2f MB  sameUuid=${a.valueMatches}")
  println("-" * 100)
}

val totalFiles = anomalies.map(_.files).sum
val totalBytes = anomalies.map(_.bytes).sum
println(f"[INFO] TOTAL : ${anomalies.length} nested dirs, $totalFiles files, ${totalBytes / 1024.0 / 1024.0 / 1024.0}%.2f GB")

if (anomalies.isEmpty) {
  println("[INFO] Nothing to do - the layout is already clean.")
}

// A nested uuid that does not match its parent uuid means the data of run A
// is physically stored under run B. Promoting it would merge the two runs.
val (matched, mismatched) = anomalies.partition(_.valueMatches)

if (mismatched.nonEmpty) {
  println("[WARN] ***** UUID MISMATCH between parent and nested directory *****")
  mismatched.foreach(a => println(s"  ${a.parent} -> ${a.nested}"))
  if (allowMismatched) {
    println("[WARN] allowMismatched = true : they WILL be processed. You asked for it.")
  } else if (!dryRun) {
    sys.error(
      s"${mismatched.length} nested dir(s) with a foreign uuid - aborting before any change. " +
      "Review them manually, then set allowMismatched = true if the merge is really intended."
    )
  } else {
    println("[WARN] They are excluded from the fix (allowMismatched = false).")
  }
}

val toFix: Array[Anomaly] = if (allowMismatched) anomalies else matched

// ---------------------------------------------------------------------
// 4. Fix
// ---------------------------------------------------------------------

/** Trash first (recoverable during the HDFS retention), hard delete as a fallback
  * when the trash is disabled (fs.trash.interval = 0). */
def removeDir(p: Path): Boolean = {
  val trashed = useTrash && Try(Trash.moveToAppropriateTrash(fs, p, conf)).getOrElse(false)
  if (trashed) {
    println(s"[OK] moved to trash : $p")
    true
  } else {
    val ok = fs.delete(p, true)
    if (ok) println(s"[OK] deleted : $p") else println(s"[ERROR] delete FAILED : $p")
    ok
  }
}

var movedCount   = 0
var failedCount  = 0
var deletedCount = 0
var keptCount    = 0

if (dryRun) {
  println(s"\n[DRY RUN] Nothing was modified. ${toFix.length} dir(s) would be processed. Set dryRun = false to apply.")
  toFix.foreach { a =>
    if (promoteFiles) println(s"[DRY RUN] would MOVE ${a.files} entries : ${a.nested} -> ${a.parent}")
    println(s"[DRY RUN] would ${if (useTrash) "TRASH" else "DELETE"} ${a.nested}")
  }
} else {
  toFix.foreach { a =>

    var failedHere = 0

    if (promoteFiles) {
      fs.listStatus(a.nested).foreach { entry =>
        val src  = entry.getPath
        val name = src.getName

        // _SUCCESS / _temporary / .crc are markers, not data. Promoting them under a
        // collision-avoiding name would strip the leading '_' and Spark would then try
        // to read them as ORC. Drop them instead.
        if (name.startsWith("_") || name.startsWith(".")) {
          fs.delete(src, true)
          println(s"[INFO] dropped marker : $src")
        } else {
          if (entry.isDirectory) {
            println(s"[WARN] promoting a DIRECTORY (deeper nesting?) : $src - re-run the script afterwards")
          }
          // Never overwrite, and never let rename() fall back to "move INTO the target dir".
          var target = new Path(a.parent, name)
          var i      = 0
          while (fs.exists(target)) {
            i += 1
            target = new Path(a.parent, s"nested${i}__$name")
          }
          val ok = fs.rename(src, target)   // metadata-only, no data copy
          if (ok) {
            movedCount += 1
          } else {
            failedHere  += 1
            failedCount += 1
            println(s"[ERROR] rename FAILED : $src -> $target")
          }
        }
      }
    }

    // Only remove the nested dir once we are sure nothing is left behind.
    if (failedHere > 0) {
      keptCount += 1
      println(s"[SKIP] $failedHere rename(s) failed, nested dir KEPT (no data loss) : ${a.nested}")
    } else if (promoteFiles && fs.listStatus(a.nested).nonEmpty) {
      keptCount += 1
      println(s"[SKIP] nested dir is not empty after promotion, KEPT : ${a.nested}")
    } else {
      if (removeDir(a.nested)) deletedCount += 1 else keptCount += 1
    }
  }
  println(s"\n[DONE] moved entries = $movedCount, failed renames = $failedCount, " +
          s"removed nested dirs = $deletedCount, kept for review = $keptCount")
}

// ---------------------------------------------------------------------
// 5. Verification
// ---------------------------------------------------------------------
if (!dryRun) {
  println("\n[VERIFY] remaining nested runid= directories :")
  val remaining = fs.listStatus(root)
    .filter(_.isDirectory)
    .filter(_.getPath.getName.startsWith("runId="))
    .flatMap(p => fs.listStatus(p.getPath).filter(_.isDirectory).filter(_.getPath.getName.startsWith("runid=")))

  if (remaining.isEmpty) println("  none - layout is clean")
  else {
    remaining.foreach(r => println("  " + r.getPath))
    println("  -> re-run the script if these come from a deeper nesting level")
  }

  // Spark caches the file index of the table: without this the next read still
  // sees the old layout.
  spark.catalog.refreshByPath(tablePath)
  println("[INFO] Spark file index refreshed.")
  println("[INFO] Also check the metastore: SHOW PARTITIONS dbprojection.term_structure")
  println("       If any 'runid=' partition is registered there, drop it with")
  println("       ALTER TABLE dbprojection.term_structure DROP PARTITION (runid='<uuid>')")
}

// Sanity read (only meaningful once the cleanup has been applied)
// val df = spark.read.orc(tablePath)
// df.printSchema()
// df.groupBy("runId").count().show(50, truncate = false)
