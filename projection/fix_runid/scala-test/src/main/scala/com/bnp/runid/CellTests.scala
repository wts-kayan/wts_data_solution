package com.bnp.runid

import java.io.File
import java.nio.file.{Files, Path => JPath}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession

/**
  * Runtime tests for the notebook-cell scripts in projection/fix_runid.
  *
  * Runs a REAL local SparkSession with Hive support (a throwaway derby
  * metastore under the temp dir), writes REAL ORC files, registers REAL
  * external partitions, then invokes the generated cell wrappers and asserts
  * on the resulting directory tree and metastore state.
  *
  * The local filesystem stands in for HDFS. That is faithful for what the
  * cells do -- they only ever go through the Hadoop FileSystem API, the same
  * code path for file:// and hdfs://. Case sensitivity is the one thing it
  * does not reproduce on Windows/macOS, where `runId=x` and `runid=x` collapse
  * into one directory. Tests that need both casings to coexist are SKIPPED
  * there rather than asserting something false -- see `caseSensitiveFs`.
  *
  * Plain `main` rather than JUnit on purpose: the surefire JUnit provider is
  * not in the local ~/.m2, and this harness must run fully offline.
  */
object CellTests {

  // ------------------------------------------------------------------
  // Tiny assertion harness
  // ------------------------------------------------------------------

  private val failures = ArrayBuffer[(String, String)]()
  private var skipped = 0
  private var passed = 0

  private var nativeSkips = 0

  /** Hadoop's local FileSystem calls NativeIO.POSIX.stat for permission info.
    * On Windows without winutils.exe / hadoop.dll that link fails. It is an
    * environment limitation, not a defect in the cell under test, so those
    * runs are reported as SKIP -- never silently as PASS. */
  private def isMissingHadoopNative(t: Throwable): Boolean = {
    var c = t
    while (c != null) {
      if (c.isInstanceOf[UnsatisfiedLinkError] &&
          String.valueOf(c.getMessage).contains("nativeio")) return true
      c = c.getCause
    }
    false
  }

  private def check(name: String)(body: => Unit): Unit = {
    print("  %-52s ".format(name))
    try {
      body
      passed += 1
      println("PASS")
    } catch {
      case Skip(why) =>
        skipped += 1
        println("SKIP (" + why + ")")
      case e: Throwable if isMissingHadoopNative(e) =>
        skipped += 1
        nativeSkips += 1
        println("SKIP (Hadoop native IO unavailable: Windows without winutils)")
      case e: Throwable =>
        failures += ((name, String.valueOf(e.getMessage)))
        println("FAIL")
        println("      " + e.toString.take(400).replace("\n", "\n      "))
    }
  }

  private case class Skip(why: String) extends RuntimeException(why)

  private def assertTrue(msg: String, cond: Boolean): Unit =
    if (!cond) throw new AssertionError(msg)

  private def assertFalse(msg: String, cond: Boolean): Unit =
    if (cond) throw new AssertionError(msg)

  private def assertEquals(msg: String, expected: Any, actual: Any): Unit =
    if (expected != actual)
      throw new AssertionError(s"$msg -- expected [$expected] but was [$actual]")

  // ------------------------------------------------------------------
  // Spark + fixtures
  // ------------------------------------------------------------------

  private var spark: SparkSession = _

  private def startSpark(): Unit = {
    val wh = Files.createTempDirectory("fixrunid-wh-")
    val derby = new File(wh.toFile, "metastore_db").getAbsolutePath
    spark = SparkSession.builder()
      .appName("fix-runid-cell-tests")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", wh.toUri.toString)
      .config("javax.jdo.option.ConnectionURL",
              s"jdbc:derby:;databaseName=$derby;create=true")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  private def uri(p: JPath): String = p.toUri.toString.stripSuffix("/")

  /** Relative paths of every file under `root`, sorted -- the tree snapshot. */
  private def tree(root: JPath): Seq[String] = {
    if (!Files.exists(root)) return Nil
    val s = Files.walk(root)
    try s.iterator().asScala
        .filter(Files.isRegularFile(_))
        .map(p => root.relativize(p).toString.replace('\\', '/'))
        .toVector.sorted
    finally s.close()
  }

  /** Does this filesystem distinguish runId= from runid=? HDFS does; Windows
    * and default macOS do not, so case-mixing tests must be skipped there. */
  private lazy val caseSensitiveFs: Boolean = {
    val probe = Files.createTempDirectory("casetest-")
    Files.createDirectory(probe.resolve("runId=probe"))
    !Files.exists(probe.resolve("runid=probe"))
  }

  /** Write a real ORC partition directory: <root>/<key>=<value>/part-*.orc */
  private def writeOrcPartition(root: JPath, key: String, value: String, rows: Int): Unit = {
    val dir = root.resolve(s"$key=$value")
    Files.createDirectories(dir)
    val tmp = Files.createTempDirectory("orcgen-")
    spark.range(rows.toLong)
      .selectExpr(
        "concat('mig', cast(id as string)) as matrixMigrationName",
        "'2026-01-01' as asOfDate",
        "'base' as scenario",
        "concat('AA', cast(id as string)) as notationCode")
      .repartition(1)
      .write.mode("overwrite").orc(tmp.resolve("out").toUri.toString)
    val src = tmp.resolve("out")
    val s = Files.list(src)
    try s.iterator().asScala
        .filter(_.getFileName.toString.endsWith(".orc"))
        .foreach(p => Files.copy(p, dir.resolve(p.getFileName.toString)))
    finally s.close()
  }

  private def createTable(db: String, table: String, root: JPath): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
    spark.sql(s"DROP TABLE IF EXISTS $db.$table")
    spark.sql(
      s"""CREATE EXTERNAL TABLE $db.$table (
         |  `matrixMigrationName` string,
         |  `asOfDate` string,
         |  `scenario` string,
         |  `notationCode` string)
         |PARTITIONED BY (`runid` string)
         |STORED AS ORC
         |LOCATION '${uri(root)}'""".stripMargin)
  }

  private def addPartition(db: String, table: String, value: String, loc: String): Unit =
    spark.sql(s"ALTER TABLE $db.$table ADD IF NOT EXISTS PARTITION (runid='$value') " +
              s"LOCATION '$loc'")

  private def partitionLocations(db: String, table: String): Map[String, String] =
    spark.sql(s"SHOW PARTITIONS $db.$table").collect().map(_.getString(0)).map { spec =>
      val v = spec.substring(spec.indexOf('=') + 1)
      val loc = spark.sql(s"DESCRIBE FORMATTED $db.$table PARTITION (runid='$v')")
        .collect()
        .find(r => Option(r.get(0)).map(_.toString.trim).contains("Location"))
        .map(_.get(1).toString.trim).getOrElse("")
      v -> loc
    }.toMap

  private def tblProps(t: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $t").collect()
      .map(r => r.getString(0) -> r.getString(1)).toMap

  private def newRoot(name: String): JPath = Files.createTempDirectory(s"fixrunid-$name-")

  private def ddl(root: JPath, n: String) = uri(root.getParent.resolve(n))

  // ------------------------------------------------------------------
  // The tests
  // ------------------------------------------------------------------

  private def flattenTests(): Unit = {
    println("\nflatten_nested_runid_partitions.scala")

    check("DRY_RUN changes nothing on disk") {
      val root = newRoot("flatten-dry")
      writeOrcPartition(root, "runId", "aaa1", 2)
      val wrapper = root.resolve("runId=bbb2")
      Files.createDirectories(wrapper)
      writeOrcPartition(wrapper, "runid", "bbb2", 2)
      createTable("dbtest", "flat_dry", root)
      val before = tree(root)
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> "dbtest.flat_dry",
        "DRY_RUN" -> "true", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-dry.sql")))
      assertEquals("tree must be untouched", before, tree(root))
    }

    check("runId=X/runid=X collapses to runId=X in one pass") {
      val root = newRoot("flatten-apply")
      writeOrcPartition(root, "runId", "aaa1", 2)
      val wrapper = root.resolve("runId=bbb2")
      Files.createDirectories(wrapper)
      writeOrcPartition(wrapper, "runid", "bbb2", 3)
      createTable("dbtest", "flat_apply", root)
      addPartition("dbtest", "flat_apply", "aaa1", uri(root) + "/runId=aaa1")
      addPartition("dbtest", "flat_apply", "bbb2", uri(root) + "/runId=bbb2/runid=bbb2")
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> "dbtest.flat_apply",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-apply.sql")))
      assertFalse("nested wrapper dir must be gone",
                  Files.exists(wrapper.resolve("runid=bbb2")))
      val files = tree(root).filter(_.endsWith(".orc"))
      assertTrue("data must sit directly under runId=bbb2/, got " + files,
                 files.exists(f => f.startsWith("runId=bbb2/") &&
                                   !f.startsWith("runId=bbb2/runid=")))
      val locs = partitionLocations("dbtest", "flat_apply")
      assertTrue("metastore must point at the flattened dir, got " + locs("bbb2"),
                 locs("bbb2").endsWith("runId=bbb2"))
    }

    check("second apply is a no-op (idempotent)") {
      val root = newRoot("flatten-idem")
      val wrapper = root.resolve("runId=ccc3")
      Files.createDirectories(wrapper)
      writeOrcPartition(wrapper, "runid", "ccc3", 2)
      createTable("dbtest", "flat_idem", root)
      val cfg = Map("TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> "dbtest.flat_idem",
                    "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-idem.sql"))
      generated.FlattenCell.run(spark, cfg)
      val afterFirst = tree(root)
      generated.FlattenCell.run(spark, cfg)
      assertEquals("second apply must change nothing", afterFirst, tree(root))
    }

    check("UUID mismatch is skipped, never merged") {
      val root = newRoot("flatten-mismatch")
      val wrapper = root.resolve("runId=ddd4")
      Files.createDirectories(wrapper)
      writeOrcPartition(wrapper, "runid", "eee5", 2)
      createTable("dbtest", "flat_mm", root)
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> "dbtest.flat_mm",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-mm.sql")))
      assertTrue("mismatched nested dir must be left untouched",
                 Files.exists(wrapper.resolve("runid=eee5")))
    }
  }

  private def renameTests(): Unit = {
    println("\nrename_partitions_to_runId.scala")

    check("runid=X is renamed to runId=X and re-pointed") {
      if (!caseSensitiveFs) throw Skip("filesystem is not case sensitive")
      val root = newRoot("rename")
      writeOrcPartition(root, "runid", "aaa1", 2)
      createTable("dbtest", "ren", root)
      addPartition("dbtest", "ren", "aaa1", uri(root) + "/runid=aaa1")
      generated.RenameCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> "dbtest.ren",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-ren.sql")))
      assertTrue("runId=aaa1 must exist", Files.exists(root.resolve("runId=aaa1")))
      assertFalse("runid=aaa1 must be gone", Files.exists(root.resolve("runid=aaa1")))
      assertTrue("metastore must be re-pointed",
                 partitionLocations("dbtest", "ren")("aaa1").endsWith("runId=aaa1"))
    }

    check("refuses a dir that still holds a nested runid=") {
      val root = newRoot("rename-nested")
      val outer = root.resolve("runid=fff6")
      Files.createDirectories(outer)
      writeOrcPartition(outer, "runid", "fff6", 2)
      createTable("dbtest", "ren_nested", root)
      generated.RenameCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> "dbtest.ren_nested",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-rn.sql")))
      assertTrue("still-nested dir must be left alone",
                 Files.exists(outer.resolve("runid=fff6")))
    }
  }

  private def recreateTests(): Unit = {
    println("\nrecreate_table_partcol_runid.scala")

    // Spark rejects any table property key starting with "spark.sql.", so
    // alter mode CANNOT be executed from Spark. The cell must refuse it
    // cleanly and leave the table alone, pointing at the beeline route.
    check("alter mode refuses to execute, leaves the table alone") {
      val root = newRoot("recreate-alter")
      writeOrcPartition(root, "runId", "aaa1", 2)
      createTable("dbtest", "rec_alter", root)
      addPartition("dbtest", "rec_alter", "aaa1", uri(root) + "/runId=aaa1")
      val before = tblProps("dbtest.rec_alter")
      generated.RecreateCell.run(spark, Map(
        "HIVE_TABLE" -> "dbtest.rec_alter", "TABLE_ROOT" -> uri(root),
        "DRY_RUN" -> "false", "FIX_MODE" -> "alter",
        "DDL_OUTPUT_PATH" -> ddl(root, "rec-alter.sql"),
        "BACKUP_OUTPUT_PATH" -> ddl(root, "rec-alter-backup.sql")))
      assertEquals("alter must not change the table properties",
                   before, tblProps("dbtest.rec_alter"))
      assertEquals("alter must not drop partitions", 1L,
                   spark.sql("SHOW PARTITIONS dbtest.rec_alter").count())
      assertTrue("the DDL file must still be written for the beeline replay",
                 Files.exists(new File(new java.net.URI(
                   ddl(root, "rec-alter.sql"))).toPath))
    }

    /** The path that actually works: a datasource table, whose properties
      * Spark writes itself with the casing taken from the column list. */
    check("recreate mode really sets partCol.0 = runId") {
      val root = newRoot("recreate-real")
      writeOrcPartition(root, "runId", "aaa1", 2)
      createTable("dbtest", "rec_real", root)
      addPartition("dbtest", "rec_real", "aaa1", uri(root) + "/runId=aaa1")
      generated.RecreateCell.run(spark, Map(
        "HIVE_TABLE" -> "dbtest.rec_real", "TABLE_ROOT" -> uri(root),
        "DRY_RUN" -> "false", "FIX_MODE" -> "recreate",
        "DDL_OUTPUT_PATH" -> ddl(root, "rec-real.sql"),
        "BACKUP_OUTPUT_PATH" -> ddl(root, "rec-real-backup.sql")))
      val cols = spark.table("dbtest.rec_real").schema.fieldNames
      assertTrue("Spark's schema must carry the camelCase partition column, got " +
                 cols.mkString(","), cols.contains("runId"))
      assertTrue("the camelCase data columns must survive, got " + cols.mkString(","),
                 cols.contains("matrixMigrationName"))
      assertEquals("the partition must be re-registered", 1L,
                   spark.sql("SHOW PARTITIONS dbtest.rec_real").count())
    }

    check("DRY_RUN leaves the metastore untouched") {
      val root = newRoot("recreate-dry")
      writeOrcPartition(root, "runId", "aaa1", 2)
      createTable("dbtest", "rec_dry", root)
      addPartition("dbtest", "rec_dry", "aaa1", uri(root) + "/runId=aaa1")
      val before = tblProps("dbtest.rec_dry").get("spark.sql.sources.schema.partCol.0")
      generated.RecreateCell.run(spark, Map(
        "HIVE_TABLE" -> "dbtest.rec_dry", "TABLE_ROOT" -> uri(root),
        "DRY_RUN" -> "true", "FIX_MODE" -> "recreate",
        "DDL_OUTPUT_PATH" -> ddl(root, "rec-dry.sql"),
        "BACKUP_OUTPUT_PATH" -> ddl(root, "rec-dry-backup.sql")))
      assertEquals("partCol.0 must be unchanged", before,
                   tblProps("dbtest.rec_dry").get("spark.sql.sources.schema.partCol.0"))
      assertEquals("dry run must not drop partitions", 1L,
                   spark.sql("SHOW PARTITIONS dbtest.rec_dry").count())
    }

    check("aborts on a MANAGED table without touching it") {
      spark.sql("CREATE DATABASE IF NOT EXISTS dbtest")
      spark.sql("DROP TABLE IF EXISTS dbtest.rec_managed")
      spark.sql("CREATE TABLE dbtest.rec_managed (a string) " +
                "PARTITIONED BY (runid string) STORED AS ORC")
      var msg = ""
      try {
        generated.RecreateCell.run(spark, Map(
          "HIVE_TABLE" -> "dbtest.rec_managed", "DRY_RUN" -> "false",
          "FIX_MODE" -> "recreate"))
      } catch { case e: RuntimeException => msg = String.valueOf(e.getMessage) }
      assertTrue("must abort naming MANAGED, got: " + msg, msg.contains("MANAGED"))
      assertTrue("the managed table must still exist",
                 spark.catalog.tableExists("dbtest.rec_managed"))
    }

    check("aborts once, with diagnostics, on an invisible table") {
      var msg = ""
      try {
        generated.RecreateCell.run(spark, Map(
          "HIVE_TABLE" -> "dbtest.does_not_exist_at_all",
          "DRY_RUN" -> "true", "FIX_MODE" -> "recreate"))
      } catch { case e: RuntimeException => msg = String.valueOf(e.getMessage) }
      assertTrue("must name the invisible table, got: " + msg,
                 msg.contains("not visible to this Spark session"))
      assertTrue("must carry the catalog diagnostics, got: " + msg,
                 msg.contains("catalogImplementation"))
    }
  }

  // ------------------------------------------------------------------

  def main(args: Array[String]): Unit = {
    println("=" * 78)
    println("fix_runid Scala cell tests -- local Spark + Hive, temp filesystem")
    println("=" * 78)
    startSpark()
    println("case-sensitive filesystem: " + caseSensitiveFs +
            (if (caseSensitiveFs) "" else "  (case-mixing tests will be skipped)"))
    try {
      flattenTests()
      renameTests()
      recreateTests()
    } finally {
      if (spark != null) spark.stop()
    }

    println("")
    println("=" * 78)
    println(s"passed: $passed   failed: ${failures.size}   skipped: $skipped")
    if (nativeSkips > 0) {
      println("")
      println(s"  $nativeSkips test(s) skipped because Hadoop's native IO is unavailable.")
      println("  That is a Windows-without-winutils limitation of THIS machine, not a")
      println("  defect in the scripts: Hive's directory operations call")
      println("  NativeIO.POSIX.stat for permission info. To run them here, point")
      println("  HADOOP_HOME at a winutils distribution matching Hadoop 3.x and put")
      println("  %HADOOP_HOME%\\bin on PATH. On Linux (and on the cluster) they run.")
    }
    if (!caseSensitiveFs) {
      println("")
      println("  Case-mixing tests were skipped: this filesystem does not distinguish")
      println("  runId= from runid=. HDFS does, so those paths are only exercised on a")
      println("  case-sensitive filesystem (any Linux box).")
    }
    if (failures.nonEmpty) {
      println("")
      failures.foreach { case (n, m) => println(s"  FAILED  $n\n          $m") }
      println("=" * 78)
      System.exit(1)
    }
    println("=" * 78)
  }
}
