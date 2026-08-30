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
  * code path for file:// and hdfs://.
  *
  * Case sensitivity matters here, because the whole point is telling runId=
  * from runid=. NTFS supports it per directory, so the temp roots turn it on
  * (see `enableCaseSensitivity`) and the case-mixing tests run on Windows too.
  * If that ever fails, `caseSensitiveFs` reports false and those tests SKIP
  * rather than assert something false.
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

  /** Hadoop's local FileSystem reaches for NativeIO.POSIX.stat, a symbol the
    * Windows hadoop.dll does not export. The session config avoids every path
    * that needs it, but if a new one appears it is an environment limitation
    * rather than a defect in the cell, so it is reported as SKIP -- never
    * silently as PASS. */
  private def isMissingHadoopNative(t: Throwable): Boolean = {
    var c = t
    while (c != null) {
      if (c.isInstanceOf[UnsatisfiedLinkError] &&
          String.valueOf(c.getMessage).contains("nativeio")) return true
      c = c.getCause
    }
    false
  }

  /** The verdict is printed AFTER the body, with the name repeated: each cell
    * writes a full report to stdout as it runs, so a name printed up front
    * ends up hundreds of lines away from its own result. */
  private def check(name: String)(body: => Unit): Unit = {
    def verdict(r: String): Unit = println("  %-6s %s".format(r, name))
    try {
      body
      passed += 1
      verdict("PASS")
    } catch {
      case Skip(why) =>
        skipped += 1
        verdict("SKIP")
        println("         reason: " + why)
      case e: Throwable if isMissingHadoopNative(e) =>
        skipped += 1
        nativeSkips += 1
        verdict("SKIP")
        println("         reason: Hadoop native IO, NativeIO$POSIX.stat unavailable here")
        if (nativeSkips == 1) {
          // print the call site once, so the cause is diagnosable rather than
          // guessed at
          println("         first occurrence, call site:")
          e.getStackTrace.take(12).foreach(f => println("           " + f))
        }
      case e: Throwable =>
        failures += ((name, String.valueOf(e.getMessage)))
        verdict("FAIL")
        println("         " + e.toString.take(400).replace("\n", "\n         "))
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
      // Hadoop's Windows hadoop.dll does not export the POSIX stat symbol that
      // RawLocalFileSystem reaches for when native IO reports itself available,
      // which surfaces as UnsatisfiedLinkError: NativeIO$POSIX.stat. Forcing
      // the pure-Java fallback keeps the local filesystem usable. Irrelevant on
      // Linux and on the cluster, where the native path works.
      .config("spark.hadoop.io.native.lib.available", "false")
      // Hive inherits permissions onto a directory it creates in the warehouse
      // via HdfsUtils.setFullFileStatus -> getGroup() -> NativeIO$POSIX.stat,
      // a symbol the Windows hadoop.dll does not export. Turning the
      // inheritance off skips that call entirely. No effect on Linux.
      .config("spark.hadoop.hive.warehouse.subdir.inherit.perms", "false")
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

  /** NTFS supports case sensitivity per directory. Turning it on lets runId=
    * and runid= coexist the way they do on HDFS, so the case-mixing tests can
    * run on Windows too. Child directories inherit the flag. No-op elsewhere. */
  private def enableCaseSensitivity(dir: JPath): Unit =
    if (System.getProperty("os.name", "").toLowerCase.contains("win")) {
      try {
        val p = new ProcessBuilder("fsutil", "file", "setCaseSensitiveInfo",
                                   dir.toString, "enable")
          .redirectErrorStream(true).start()
        p.getInputStream.close()
        p.waitFor()
      } catch { case _: Throwable => () }   // best effort; probe decides
    }

  private lazy val caseSensitiveFs: Boolean = {
    val probe = Files.createTempDirectory("casetest-")
    enableCaseSensitivity(probe)
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

  /** The `default` database on purpose: CREATE DATABASE goes through
    * Warehouse.mkdirs -> HdfsUtils.setFullFileStatus -> getGroup(), which
    * needs the NativeIO POSIX stat symbol the Windows hadoop.dll lacks. */
  private val DB = "default"

  private def createTable(db: String, table: String, root: JPath): Unit = {
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

  /** The REAL term_structure shape, nested column included. The production
    * table carries `matrix array<array<double>>`, and the recreate cell has to
    * round-trip that through the Spark schema JSON, where a nested type must be
    * a nested JSON object and not the DDL string. Every other fixture here is
    * four plain strings, which would never exercise that. */
  private def createNestedTable(db: String, table: String, root: JPath): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $db.$table")
    spark.sql(
      s"""CREATE EXTERNAL TABLE $db.$table (
         |  `matrixMigrationName` string,
         |  `asOfDate` string,
         |  `scenario` string,
         |  `notationCode` string,
         |  `matrix` array<array<double>>)
         |PARTITIONED BY (`runid` string)
         |STORED AS ORC
         |LOCATION '${uri(root)}'""".stripMargin)
  }

  /** An ORC partition carrying the nested `matrix` column, so the recreated
    * table can be read back and the nested values actually compared. */
  private def writeNestedPartition(root: JPath, value: String): Unit = {
    val dir = root.resolve(s"runId=$value")
    Files.createDirectories(dir)
    val tmp = Files.createTempDirectory("orcgen-")
    spark.range(2L)
      .selectExpr(
        "concat('mig', cast(id as string)) as matrixMigrationName",
        "'2026-01-01' as asOfDate",
        "'base' as scenario",
        "concat('AA', cast(id as string)) as notationCode",
        "array(array(cast(id as double), 1.5d), array(2.5d)) as matrix")
      .repartition(1)
      .write.mode("overwrite").orc(tmp.resolve("out").toUri.toString)
    val s = Files.list(tmp.resolve("out"))
    try s.iterator().asScala
        .filter(_.getFileName.toString.endsWith(".orc"))
        .foreach(p => Files.copy(p, dir.resolve(p.getFileName.toString)))
    finally s.close()
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

  private def newRoot(name: String): JPath = {
    val p = Files.createTempDirectory(s"fixrunid-$name-")
    enableCaseSensitivity(p)
    p
  }

  private def ddl(root: JPath, n: String) = uri(root.getParent.resolve(n))

  /** Run `body` with stdout captured. The cells report through `println`, which
    * is Scala's Console.out, not System.out -- Console.out is bound once, on
    * first use, so System.setOut alone would capture nothing here. Console
    * .withOut is the redirect that actually works; System.setOut is set too,
    * for anything that prints through the Java stream directly. */
  private def capturingStdout(body: => Unit): String = {
    val buf = new java.io.ByteArrayOutputStream()
    val ps  = new java.io.PrintStream(buf, true, "UTF-8")
    val old = System.out
    try {
      System.setOut(ps)
      Console.withOut(ps) { body }
    } finally System.setOut(old)
    new String(buf.toByteArray, "UTF-8")
  }

  /** One REAL ORC file, under a name the test chooses, carrying `tag` in its
    * first column. The merge paths key on the file name -- a collision is only
    * reachable when two directories hold the same name -- and Spark picks its
    * own part-<uuid> names, so those paths need a file placed by hand. It has
    * to be real ORC, not a stub: the cells re-read the table to verify, and a
    * stub would make that verification fail for a reason the test did not
    * intend. The tag is what tells two same-named files apart afterwards. */
  private def writeNamedOrc(dir: JPath, name: String, tag: String): Unit = {
    Files.createDirectories(dir)
    val tmp = Files.createTempDirectory("orcgen-")
    spark.range(1L)
      .selectExpr(
        s"'$tag' as matrixMigrationName",
        "'2026-01-01' as asOfDate",
        "'base' as scenario",
        "'AA0' as notationCode")
      .repartition(1)
      .write.mode("overwrite").orc(tmp.resolve("out").toUri.toString)
    val src = Files.list(tmp.resolve("out"))
    try {
      val one = src.iterator().asScala
        .find(_.getFileName.toString.endsWith(".orc"))
        .getOrElse(throw new IllegalStateException("no orc file was produced"))
      Files.copy(one, dir.resolve(name))
    } finally src.close()
  }

  private def writeMarker(dir: JPath, name: String): Unit = {
    Files.createDirectories(dir)
    Files.write(dir.resolve(name), Array.empty[Byte])
  }

  /** Every ORC file directly under `dir`, as name -> the tag it carries. Lets a
    * merge assert that no file was lost AND that each one is still readable,
    * rather than merely that some count matched. */
  private def orcTags(dir: JPath): Map[String, String] = {
    val s = Files.list(dir)
    try s.iterator().asScala
        .filter(Files.isRegularFile(_))
        .filter(_.getFileName.toString.endsWith(".orc"))
        .map(p => p.getFileName.toString ->
                  spark.read.orc(p.toUri.toString).collect()(0).getString(0))
        .toMap
    finally s.close()
  }

  // ------------------------------------------------------------------
  // Fixtures for unpurge_and_drop_dbprojection
  // ------------------------------------------------------------------

  /** A throwaway database with an explicit LOCATION. Explicit on purpose: it
    * keeps the database out of the warehouse, whose creation path is the one
    * that needs the NativeIO POSIX symbol Windows lacks. */
  private def makeDb(name: String): JPath = {
    val loc = Files.createTempDirectory(s"dropdb-$name-")
    spark.sql(s"DROP DATABASE IF EXISTS $name CASCADE")
    spark.sql(s"CREATE DATABASE $name LOCATION '${uri(loc)}'")
    loc
  }

  /** EXTERNAL, partitioned, with one registered partition and real files --
    * the shape the script is built for. `purge` mirrors CDP's
    * TRANSLATED_TO_EXTERNAL tables, where DROP would delete the data. */
  private def extPartTable(db: String, t: String, purge: Boolean): JPath = {
    val root = newRoot(s"dropdata-$t-")
    writeOrcPartition(root, "runid", "aaa1", 1)
    spark.sql(s"DROP TABLE IF EXISTS $db.$t")
    spark.sql(
      s"""CREATE EXTERNAL TABLE $db.$t (`a` string)
         |PARTITIONED BY (`runid` string)
         |STORED AS ORC
         |LOCATION '${uri(root)}'
         |TBLPROPERTIES ('external.table.purge'='${purge.toString}',
         |               'TRANSLATED_TO_EXTERNAL'='TRUE')""".stripMargin)
    spark.sql(s"ALTER TABLE $db.$t ADD IF NOT EXISTS PARTITION (runid='aaa1') " +
              s"LOCATION '${uri(root)}/runid=aaa1'")
    root
  }

  /** EXTERNAL but NOT partitioned -- must never be altered and never dropped. */
  private def extPlainTable(db: String, t: String): JPath = {
    val root = newRoot(s"dropdata-$t-")
    spark.sql(s"DROP TABLE IF EXISTS $db.$t")
    spark.sql(
      s"""CREATE EXTERNAL TABLE $db.$t (`a` string)
         |STORED AS ORC
         |LOCATION '${uri(root)}'
         |TBLPROPERTIES ('external.table.purge'='true')""".stripMargin)
    root
  }

  private def tableNames(db: String): Set[String] =
    spark.sql(s"SHOW TABLES IN $db").collect().map(_.getAs[String]("tableName")).toSet

  private def purgeOf(db: String, t: String): String =
    spark.sessionState.catalog
      .getTableMetadata(new org.apache.spark.sql.catalyst.TableIdentifier(t, Some(db)))
      .properties.getOrElse("external.table.purge", "")

  /** EXTERNAL, partitioned by `runid`, with the partition directories still
    * in the WRONG case on disk -- the state the batch cell exists to fix. */
  private def runidTable(db: String, t: String, values: Seq[String]): JPath = {
    val root = newRoot(s"fixall-$t-")
    values.foreach(v => writeOrcPartition(root, "runid", v, 1))
    spark.sql(s"DROP TABLE IF EXISTS $db.$t")
    spark.sql(
      s"""CREATE EXTERNAL TABLE $db.$t (
         |  `matrixMigrationName` string,
         |  `asOfDate` string,
         |  `scenario` string,
         |  `notationCode` string)
         |PARTITIONED BY (`runid` string)
         |STORED AS ORC
         |LOCATION '${uri(root)}'""".stripMargin)
    values.foreach(v =>
      spark.sql(s"ALTER TABLE $db.$t ADD IF NOT EXISTS PARTITION (runid='$v') " +
                s"LOCATION '${uri(root)}/runid=$v'"))
    root
  }

  /** Directory names directly under a table root, sorted. */
  private def dirNames(root: JPath): Seq[String] = {
    val st = Files.list(root)
    try st.iterator().asScala.filter(Files.isDirectory(_))
         .map(_.getFileName.toString).toVector.sorted
    finally st.close()
  }

  private def readArtifact(dir: JPath, name: String): String =
    new String(Files.readAllBytes(dir.resolve(name)), "UTF-8")

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
      createTable(DB, "flat_dry", root)
      val before = tree(root)
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_dry",
        "DRY_RUN" -> "true", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-dry.sql")))
      assertEquals("tree must be untouched", before, tree(root))
    }

    check("runId=X/runid=X collapses to runId=X in one pass") {
      val root = newRoot("flatten-apply")
      writeOrcPartition(root, "runId", "aaa1", 2)
      val wrapper = root.resolve("runId=bbb2")
      Files.createDirectories(wrapper)
      writeOrcPartition(wrapper, "runid", "bbb2", 3)
      createTable(DB, "flat_apply", root)
      addPartition(DB, "flat_apply", "aaa1", uri(root) + "/runId=aaa1")
      addPartition(DB, "flat_apply", "bbb2", uri(root) + "/runId=bbb2/runid=bbb2")
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_apply",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-apply.sql")))
      assertFalse("nested wrapper dir must be gone",
                  Files.exists(wrapper.resolve("runid=bbb2")))
      val files = tree(root).filter(_.endsWith(".orc"))
      assertTrue("data must sit directly under runId=bbb2/, got " + files,
                 files.exists(f => f.startsWith("runId=bbb2/") &&
                                   !f.startsWith("runId=bbb2/runid=")))
      val locs = partitionLocations(DB, "flat_apply")
      assertTrue("metastore must point at the flattened dir, got " + locs("bbb2"),
                 locs("bbb2").endsWith("runId=bbb2"))
    }

    check("second apply is a no-op (idempotent)") {
      val root = newRoot("flatten-idem")
      val wrapper = root.resolve("runId=ccc3")
      Files.createDirectories(wrapper)
      writeOrcPartition(wrapper, "runid", "ccc3", 2)
      createTable(DB, "flat_idem", root)
      val cfg = Map("TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_idem",
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
      createTable(DB, "flat_mm", root)
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_mm",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-mm.sql")))
      assertTrue("mismatched nested dir must be left untouched",
                 Files.exists(wrapper.resolve("runid=eee5")))
    }

    // The merge path is where data can actually be lost: files are moved into a
    // directory that already holds files, and HDFS rename onto an existing name
    // fails. The cell renames the incoming file instead. If that ever regresses,
    // a partition silently loses a file -- so assert on contents, not counts.
    check("a name collision on merge keeps both files") {
      if (!caseSensitiveFs) throw Skip("filesystem is not case sensitive")
      val root = newRoot("flatten-collide")
      // already-correct partition, and a nested leftover for the SAME run whose
      // file carries the SAME name -- the state a half-finished rename leaves
      writeNamedOrc(root.resolve("runId=ggg7"), "part-0.orc", "from-target")
      writeNamedOrc(root.resolve("runid=ggg7").resolve("runid=ggg7"),
                    "part-0.orc", "from-nested")
      createTable(DB, "flat_collide", root)
      addPartition(DB, "flat_collide", "ggg7", uri(root) + "/runid=ggg7/runid=ggg7")
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_collide",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-collide.sql")))

      val body = orcTags(root.resolve("runId=ggg7"))
      assertEquals("both files must land in runId=ggg7, got " + body.keys.toSeq.sorted,
                   2, body.size)
      assertTrue("the file already there must keep its name and content",
                 body.get("part-0.orc").contains("from-target"))
      assertTrue("the incoming file must survive under a merged_ name, got " +
                 body.keys.toSeq.sorted,
                 body.exists { case (n, c) =>
                   n.startsWith("merged_") && n.endsWith("_part-0.orc") &&
                   c == "from-nested" })
      assertFalse("the emptied wrapper must be gone",
                  Files.exists(root.resolve("runid=ggg7")))
      assertTrue("the metastore must point at the flattened dir",
                 partitionLocations(DB, "flat_collide")("ggg7").endsWith("runId=ggg7"))
    }

    // _SUCCESS and friends are never moved: they describe the write that made
    // the directory, and moving them would relabel a different directory. The
    // cell keeps them, and therefore must NOT delete the dir they sit in.
    check("markers stay put and their dir survives the merge") {
      val root = newRoot("flatten-markers")
      val inner = root.resolve("runId=hhh8").resolve("runid=hhh8")
      writeNamedOrc(inner, "part-0.orc", "data")
      writeMarker(inner, "_SUCCESS")
      createTable(DB, "flat_mark", root)
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_mark",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-mark.sql")))
      assertTrue("the data file must be promoted",
                 Files.exists(root.resolve("runId=hhh8").resolve("part-0.orc")))
      assertTrue("the marker must be left where it was",
                 Files.exists(inner.resolve("_SUCCESS")))
      assertFalse("the marker must not have been promoted too",
                  Files.exists(root.resolve("runId=hhh8").resolve("_SUCCESS")))
    }

    // A nested dir holding ONLY markers never reaches the merge path -- there
    // is no data to promote -- so it used to survive every re-run and be left
    // for a human. It is still a runid= dir under a runId= one, which is the
    // layout this script exists to remove.
    check("a marker-only nested dir is left, and named, by default") {
      val root = newRoot("flatten-markeronly")
      val inner = root.resolve("runId=mmm1").resolve("runid=mmm1")
      writeNamedOrc(root.resolve("runId=mmm1"), "part-0.orc", "data")
      writeMarker(inner, "_SUCCESS")
      createTable(DB, "flat_mo", root)
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_mo",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-mo.sql")))
      assertTrue("the default must not delete anything holding a marker",
                 Files.exists(inner))
    }

    check("DELETE_MARKERS_ON_MERGE=true removes a marker-only nested dir") {
      val root = newRoot("flatten-markeronly-del")
      val inner = root.resolve("runId=nnn2").resolve("runid=nnn2")
      writeNamedOrc(root.resolve("runId=nnn2"), "part-0.orc", "data")
      writeMarker(inner, "_SUCCESS")
      createTable(DB, "flat_mo_del", root)
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_mo_del",
        "DRY_RUN" -> "false", "DELETE_MARKERS_ON_MERGE" -> "true",
        "DDL_OUTPUT_PATH" -> ddl(root, "ddl-mo-del.sql")))
      assertFalse("the marker-only nested dir must be gone", Files.exists(inner))
      assertTrue("the partition's own data must be untouched",
                 Files.exists(root.resolve("runId=nnn2").resolve("part-0.orc")))
    }

    // Guard on the one branch that deletes a non-empty directory: if real data
    // turned up in it since the scan, it must not be deleted.
    check("a marker-only drop is abandoned if data appears in the dir") {
      val root = newRoot("flatten-markeronly-race")
      val inner = root.resolve("runId=ooo3").resolve("runid=ooo3")
      writeNamedOrc(root.resolve("runId=ooo3"), "part-0.orc", "data")
      writeMarker(inner, "_SUCCESS")
      createTable(DB, "flat_mo_race", root)
      // stand in for the race: the cell re-lists the dir before deleting, so a
      // file present at that moment is what the re-check sees
      writeNamedOrc(inner, "late-arrival.orc", "late")
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_mo_race",
        "DRY_RUN" -> "false", "DELETE_MARKERS_ON_MERGE" -> "true",
        "DDL_OUTPUT_PATH" -> ddl(root, "ddl-mo-race.sql")))
      assertTrue("a dir holding real data must never be dropped as marker-only",
                 Files.exists(inner.resolve("late-arrival.orc")) ||
                 Files.exists(root.resolve("runId=ooo3").resolve("late-arrival.orc")))
    }

    // The state a real run lands in, and the way out of it. This is the
    // sequence to actually run against the cluster when a runid= dir is found
    // still sitting under a runId= one.
    check("a leftover nested dir is cleared by a re-run with the flag on") {
      val root = newRoot("flatten-recovery")
      val inner = root.resolve("runId=ppp4").resolve("runid=ppp4")
      writeNamedOrc(inner, "part-0.orc", "data")
      writeMarker(inner, "_SUCCESS")
      createTable(DB, "flat_rec", root)
      addPartition(DB, "flat_rec", "ppp4", uri(root) + "/runId=ppp4/runid=ppp4")
      val cfg = Map("TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_rec",
                    "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-rec.sql"))

      // pass 1, default flag: data is promoted, the marker dir survives
      generated.FlattenCell.run(spark, cfg)
      assertTrue("pass 1 must promote the data",
                 Files.exists(root.resolve("runId=ppp4").resolve("part-0.orc")))
      assertTrue("pass 1 leaves the marker dir behind", Files.exists(inner))
      assertTrue("but the metastore must already be re-pointed",
                 partitionLocations(DB, "flat_rec")("ppp4").endsWith("runId=ppp4"))

      // pass 2, flag on: the leftover goes, the data does not
      generated.FlattenCell.run(spark, cfg + ("DELETE_MARKERS_ON_MERGE" -> "true"))
      assertFalse("pass 2 must clear the leftover", Files.exists(inner))
      assertTrue("and must not touch the promoted data",
                 Files.exists(root.resolve("runId=ppp4").resolve("part-0.orc")))
      assertEquals("the partition must still read back", 1L,
                   spark.table(s"$DB.flat_rec").count())
    }

    // A leftover that is never mentioned is the real danger: the run reads as
    // a success while the nested layout it exists to remove is still there.
    check("a surviving nested dir is named in the report, not hidden") {
      val root = newRoot("flatten-report")
      val inner = root.resolve("runId=qqq5").resolve("runid=qqq5")
      writeNamedOrc(inner, "part-0.orc", "data")
      writeMarker(inner, "_SUCCESS")
      createTable(DB, "flat_report", root)
      val out = capturingStdout {
        generated.FlattenCell.run(spark, Map(
          "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_report",
          "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-report.sql")))
      }
      assertTrue("the leftover must survive for this test to mean anything",
                 Files.exists(inner))
      assertTrue("the run must name the surviving nested dir",
                 out.contains("runid=qqq5") &&
                 out.contains("nested 'runId=' dir(s) still under a partition dir"))
      assertFalse("and must not claim the tree is clean",
                  out.contains("no nested 'runId=' directory left"))
    }

    check("DELETE_MARKERS_ON_MERGE=true clears the nested dir") {
      val root = newRoot("flatten-markers-del")
      val inner = root.resolve("runId=iii9").resolve("runid=iii9")
      writeNamedOrc(inner, "part-0.orc", "data")
      writeMarker(inner, "_SUCCESS")
      createTable(DB, "flat_mark_del", root)
      generated.FlattenCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.flat_mark_del",
        "DRY_RUN" -> "false", "DELETE_MARKERS_ON_MERGE" -> "true",
        "DDL_OUTPUT_PATH" -> ddl(root, "ddl-mark-del.sql")))
      assertTrue("the data file must still be promoted",
                 Files.exists(root.resolve("runId=iii9").resolve("part-0.orc")))
      assertFalse("the emptied nested dir must be gone", Files.exists(inner))
    }
  }

  private def renameTests(): Unit = {
    println("\nrename_partitions_to_runId.scala")

    check("runid=X is renamed to runId=X and re-pointed") {
      if (!caseSensitiveFs) throw Skip("filesystem is not case sensitive")
      val root = newRoot("rename")
      writeOrcPartition(root, "runid", "aaa1", 2)
      createTable(DB, "ren", root)
      addPartition(DB, "ren", "aaa1", uri(root) + "/runid=aaa1")
      generated.RenameCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.ren",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-ren.sql")))
      assertTrue("runId=aaa1 must exist", Files.exists(root.resolve("runId=aaa1")))
      assertFalse("runid=aaa1 must be gone", Files.exists(root.resolve("runid=aaa1")))
      assertTrue("metastore must be re-pointed",
                 partitionLocations(DB, "ren")("aaa1").endsWith("runId=aaa1"))
    }

    check("refuses a dir that still holds a nested runid=") {
      val root = newRoot("rename-nested")
      val outer = root.resolve("runid=fff6")
      Files.createDirectories(outer)
      writeOrcPartition(outer, "runid", "fff6", 2)
      createTable(DB, "ren_nested", root)
      generated.RenameCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.ren_nested",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> ddl(root, "ddl-rn.sql")))
      assertTrue("still-nested dir must be left alone",
                 Files.exists(outer.resolve("runid=fff6")))
    }

    // MERGE_ON_COLLISION is the guard between "tidy the casing" and "move data
    // between two directories". With it off the cell must report and stop, so
    // that a partly-renamed table can be inspected before anything moves.
    check("MERGE_ON_COLLISION=false refuses a colliding target") {
      if (!caseSensitiveFs) throw Skip("filesystem is not case sensitive")
      val root = newRoot("rename-nomerge")
      writeNamedOrc(root.resolve("runId=jjj1"), "part-0.orc", "from-target")
      writeNamedOrc(root.resolve("runid=jjj1"), "part-1.orc", "from-source")
      createTable(DB, "ren_nomerge", root)
      val before = tree(root)
      generated.RenameCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.ren_nomerge",
        "DRY_RUN" -> "false", "MERGE_ON_COLLISION" -> "false",
        "DDL_OUTPUT_PATH" -> ddl(root, "ddl-nomerge.sql")))
      assertEquals("nothing may move when merging is refused", before, tree(root))
    }

    check("MERGE_ON_COLLISION=true loses no file, colliding or not") {
      if (!caseSensitiveFs) throw Skip("filesystem is not case sensitive")
      val root = newRoot("rename-merge")
      writeNamedOrc(root.resolve("runId=kkk2"), "part-0.orc", "from-target")
      writeNamedOrc(root.resolve("runid=kkk2"), "part-0.orc", "from-source")
      writeNamedOrc(root.resolve("runid=kkk2"), "part-9.orc", "only-in-source")
      createTable(DB, "ren_merge", root)
      addPartition(DB, "ren_merge", "kkk2", uri(root) + "/runid=kkk2")
      generated.RenameCell.run(spark, Map(
        "TABLE_ROOT" -> uri(root), "HIVE_TABLE" -> s"$DB.ren_merge",
        "DRY_RUN" -> "false", "MERGE_ON_COLLISION" -> "true",
        "DDL_OUTPUT_PATH" -> ddl(root, "ddl-merge.sql")))

      val body = orcTags(root.resolve("runId=kkk2"))
      assertEquals("all three files must end up in runId=kkk2, got " +
                   body.keys.toSeq.sorted, 3, body.size)
      assertTrue("the file already there must be untouched",
                 body.get("part-0.orc").contains("from-target"))
      assertTrue("the non-colliding file must move under its own name",
                 body.get("part-9.orc").contains("only-in-source"))
      assertTrue("the colliding file must survive renamed, got " +
                 body.keys.toSeq.sorted,
                 body.exists { case (n, c) =>
                   n.startsWith("merged_") && c == "from-source" })
      assertFalse("the emptied source dir must be gone",
                  Files.exists(root.resolve("runid=kkk2")))
      assertTrue("the metastore must be re-pointed",
                 partitionLocations(DB, "ren_merge")("kkk2").endsWith("runId=kkk2"))
      assertEquals("and all three rows must read back through the table", 3L,
                   spark.table(s"$DB.ren_merge").count())
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
      createTable(DB, "rec_alter", root)
      addPartition(DB, "rec_alter", "aaa1", uri(root) + "/runId=aaa1")
      val before = tblProps(s"$DB.rec_alter")
      generated.RecreateCell.run(spark, Map(
        "HIVE_TABLE" -> s"$DB.rec_alter", "TABLE_ROOT" -> uri(root),
        "DRY_RUN" -> "false", "FIX_MODE" -> "alter",
        "DDL_OUTPUT_PATH" -> ddl(root, "rec-alter.sql"),
        "BACKUP_OUTPUT_PATH" -> ddl(root, "rec-alter-backup.sql")))
      assertEquals("alter must not change the table properties",
                   before, tblProps(s"$DB.rec_alter"))
      assertEquals("alter must not drop partitions", 1L,
                   spark.sql(s"SHOW PARTITIONS $DB.rec_alter").count())
      assertTrue("the DDL file must still be written for the beeline replay",
                 Files.exists(new File(new java.net.URI(
                   ddl(root, "rec-alter.sql"))).toPath))
    }

    /** The path that actually works: a datasource table, whose properties
      * Spark writes itself with the casing taken from the column list. */
    check("recreate mode really sets partCol.0 = runId") {
      val root = newRoot("recreate-real")
      writeOrcPartition(root, "runId", "aaa1", 2)
      createTable(DB, "rec_real", root)
      addPartition(DB, "rec_real", "aaa1", uri(root) + "/runId=aaa1")
      generated.RecreateCell.run(spark, Map(
        "HIVE_TABLE" -> s"$DB.rec_real", "TABLE_ROOT" -> uri(root),
        "DRY_RUN" -> "false", "FIX_MODE" -> "recreate",
        "DDL_OUTPUT_PATH" -> ddl(root, "rec-real.sql"),
        "BACKUP_OUTPUT_PATH" -> ddl(root, "rec-real-backup.sql")))
      val cols = spark.table(s"$DB.rec_real").schema.fieldNames
      assertTrue("Spark's schema must carry the camelCase partition column, got " +
                 cols.mkString(","), cols.contains("runId"))
      assertTrue("the camelCase data columns must survive, got " + cols.mkString(","),
                 cols.contains("matrixMigrationName"))
      assertEquals("the partition must be re-registered", 1L,
                   spark.sql(s"SHOW PARTITIONS $DB.rec_real").count())
    }

    check("DRY_RUN leaves the metastore untouched") {
      val root = newRoot("recreate-dry")
      writeOrcPartition(root, "runId", "aaa1", 2)
      createTable(DB, "rec_dry", root)
      addPartition(DB, "rec_dry", "aaa1", uri(root) + "/runId=aaa1")
      val before = tblProps(s"$DB.rec_dry").get("spark.sql.sources.schema.partCol.0")
      generated.RecreateCell.run(spark, Map(
        "HIVE_TABLE" -> s"$DB.rec_dry", "TABLE_ROOT" -> uri(root),
        "DRY_RUN" -> "true", "FIX_MODE" -> "recreate",
        "DDL_OUTPUT_PATH" -> ddl(root, "rec-dry.sql"),
        "BACKUP_OUTPUT_PATH" -> ddl(root, "rec-dry-backup.sql")))
      assertEquals("partCol.0 must be unchanged", before,
                   tblProps(s"$DB.rec_dry").get("spark.sql.sources.schema.partCol.0"))
      assertEquals("dry run must not drop partitions", 1L,
                   spark.sql(s"SHOW PARTITIONS $DB.rec_dry").count())
    }

    check("aborts on a MANAGED table without touching it") {
      s"DROP TABLE IF EXISTS $DB.rec_managed"
      spark.sql(s"CREATE TABLE $DB.rec_managed (a string) " +
                "PARTITIONED BY (runid string) STORED AS ORC")
      var msg = ""
      try {
        generated.RecreateCell.run(spark, Map(
          "HIVE_TABLE" -> s"$DB.rec_managed", "DRY_RUN" -> "false",
          "FIX_MODE" -> "recreate"))
      } catch { case e: RuntimeException => msg = String.valueOf(e.getMessage) }
      assertTrue("must abort naming MANAGED, got: " + msg, msg.contains("MANAGED"))
      assertTrue("the managed table must still exist",
                 spark.catalog.tableExists(s"$DB.rec_managed"))
    }

    check("aborts once, with diagnostics, on an invisible table") {
      var msg = ""
      try {
        generated.RecreateCell.run(spark, Map(
          "HIVE_TABLE" -> s"$DB.does_not_exist_at_all",
          "DRY_RUN" -> "true", "FIX_MODE" -> "recreate"))
      } catch { case e: RuntimeException => msg = String.valueOf(e.getMessage) }
      assertTrue("must name the invisible table, got: " + msg,
                 msg.contains("not visible to this Spark session"))
      assertTrue("must carry the catalog diagnostics, got: " + msg,
                 msg.contains("catalogImplementation"))
    }

    // recreate mode DROPs before it CREATEs. The backup file is the only way
    // back if the CREATE fails, so it has to be readable and complete BEFORE
    // the drop -- an empty or partial one is worse than none, because it looks
    // like a safety net.
    check("the backup replays the pre-drop definition") {
      val root = newRoot("recreate-backup")
      writeOrcPartition(root, "runId", "aaa1", 1)
      writeOrcPartition(root, "runId", "bbb2", 1)
      writeOrcPartition(root, "runId", "ccc3", 1)
      createTable(DB, "rec_backup", root)
      Seq("aaa1", "bbb2", "ccc3").foreach(v =>
        addPartition(DB, "rec_backup", v, uri(root) + s"/runId=$v"))
      val backupPath = ddl(root, "rec-backup-backup.sql")
      generated.RecreateCell.run(spark, Map(
        "HIVE_TABLE" -> s"$DB.rec_backup", "TABLE_ROOT" -> uri(root),
        "DRY_RUN" -> "false", "FIX_MODE" -> "recreate",
        "DDL_OUTPUT_PATH" -> ddl(root, "rec-backup.sql"),
        "BACKUP_OUTPUT_PATH" -> backupPath))

      val backup = new String(Files.readAllBytes(
        new File(new java.net.URI(backupPath)).toPath), "UTF-8")
      assertTrue("the backup must carry the original CREATE, got: " + backup.take(300),
                 backup.contains("CREATE") && backup.contains("rec_backup"))
      Seq("aaa1", "bbb2", "ccc3").foreach { v =>
        assertTrue(s"the backup must be able to re-add partition $v, got: " + backup,
                   backup.contains(s"ADD IF NOT EXISTS PARTITION") && backup.contains(v))
      }
    }

    // One partition proves the re-registration runs at all; three prove it
    // does not stop after the first. A partial replay leaves data on disk that
    // the table can no longer see.
    check("recreate re-registers every partition, not just one") {
      val root = newRoot("recreate-many")
      Seq("aaa1", "bbb2", "ccc3").foreach(v => writeOrcPartition(root, "runId", v, 1))
      createTable(DB, "rec_many", root)
      Seq("aaa1", "bbb2", "ccc3").foreach(v =>
        addPartition(DB, "rec_many", v, uri(root) + s"/runId=$v"))
      generated.RecreateCell.run(spark, Map(
        "HIVE_TABLE" -> s"$DB.rec_many", "TABLE_ROOT" -> uri(root),
        "DRY_RUN" -> "false", "FIX_MODE" -> "recreate",
        "DDL_OUTPUT_PATH" -> ddl(root, "rec-many.sql"),
        "BACKUP_OUTPUT_PATH" -> ddl(root, "rec-many-backup.sql")))
      assertEquals("every partition must come back", 3L,
                   spark.sql(s"SHOW PARTITIONS $DB.rec_many").count())
      assertEquals("and the rows must still be readable through the table", 3L,
                   spark.table(s"$DB.rec_many").count())
    }

    // The production table is not four strings: it carries
    // `matrix array<array<double>>`. recreate rebuilds the table from a schema
    // JSON, where a nested type has to be a nested JSON object rather than the
    // DDL string, so this is the one column shape that can break the CREATE
    // after the DROP has already happened.
    check("a nested array<array<double>> column survives the recreate") {
      val root = newRoot("recreate-nested")
      writeNestedPartition(root, "aaa1")
      writeNestedPartition(root, "bbb2")
      createNestedTable(DB, "rec_nested", root)
      Seq("aaa1", "bbb2").foreach(v =>
        addPartition(DB, "rec_nested", v, uri(root) + s"/runId=$v"))
      generated.RecreateCell.run(spark, Map(
        "HIVE_TABLE" -> s"$DB.rec_nested", "TABLE_ROOT" -> uri(root),
        "DRY_RUN" -> "false", "FIX_MODE" -> "recreate",
        "DDL_OUTPUT_PATH" -> ddl(root, "rec-nested.sql"),
        "BACKUP_OUTPUT_PATH" -> ddl(root, "rec-nested-backup.sql")))

      val schema = spark.table(s"$DB.rec_nested").schema
      assertTrue("the partition column must be camelCase, got " +
                 schema.fieldNames.mkString(","),
                 schema.fieldNames.contains("runId"))
      val matrix = schema.fields.find(_.name == "matrix")
        .getOrElse(throw new AssertionError("the matrix column is gone, got " +
                                            schema.fieldNames.mkString(",")))
      assertEquals("the nested type must round-trip intact",
                   "array<array<double>>", matrix.dataType.simpleString)
      assertEquals("every partition must come back", 2L,
                   spark.sql(s"SHOW PARTITIONS $DB.rec_nested").count())
      // and the nested values must still READ, not merely typecheck
      val rows = spark.sql(
        s"SELECT matrix FROM $DB.rec_nested WHERE runId = 'aaa1' " +
        "ORDER BY notationCode").collect()
      assertEquals("both rows of the partition must read back", 2, rows.length)
      assertEquals("the nested value must survive",
                   Seq(Seq(0.0d, 1.5d), Seq(2.5d)),
                   rows(0).getSeq[Seq[Double]](0).map(_.toSeq).toSeq)
    }
  }

  private def dropTablesTests(): Unit = {
    println("")
    println("unpurge_and_drop_dbprojection.scala")

    def cfg(db: String, out: JPath, dry: Boolean) = Map(
      "DB" -> db, "OUTPUT_DIR" -> out.toUri.toString,
      "DRY_RUN" -> dry.toString, "CHECK_INVENTORY" -> "false")

    check("DRY_RUN alters nothing and drops nothing") {
      val db = "dropdb_dry"
      makeDb(db)
      extPartTable(db, "part_purge", purge = true)
      extPlainTable(db, "plain_one")
      val out = Files.createTempDirectory("dropout-dry-")
      generated.UnpurgeDropCell.run(spark, cfg(db, out, dry = true))
      assertEquals("both tables must still exist",
                   Set("part_purge", "plain_one"), tableNames(db))
      assertEquals("the purge flag must be untouched", "true",
                   purgeOf(db, "part_purge"))
    }

    // The acceptance criterion that protects the other 60-odd tables: a table
    // with no partition column is never altered and never dropped.
    check("a non-partitioned table is never altered nor dropped") {
      val db = "dropdb_scope"
      makeDb(db)
      extPartTable(db, "part_purge", purge = true)
      extPlainTable(db, "plain_one")
      val out = Files.createTempDirectory("dropout-scope-")
      generated.UnpurgeDropCell.run(spark, cfg(db, out, dry = false))
      assertTrue("the non-partitioned table must survive",
                 tableNames(db).contains("plain_one"))
      assertEquals("and its purge flag must be untouched", "true",
                   purgeOf(db, "plain_one"))
      assertFalse("while the partitioned one is dropped",
                  tableNames(db).contains("part_purge"))
    }

    check("a MANAGED table is reported and never dropped") {
      val db = "dropdb_managed"
      makeDb(db)
      spark.sql(s"CREATE TABLE $db.man_one (a string) PARTITIONED BY (runid string) " +
                "STORED AS ORC")
      val out = Files.createTempDirectory("dropout-managed-")
      generated.UnpurgeDropCell.run(spark, cfg(db, out, dry = false))
      assertTrue("the MANAGED table must still exist",
                 tableNames(db).contains("man_one"))
      assertTrue("and must be named in the report",
                 readArtifact(out, "dbprojection_preflight_report.csv").contains("MANAGED"))
    }

    check("purge is flipped to false before the table is dropped") {
      val db = "dropdb_flip"
      makeDb(db)
      val data = extPartTable(db, "part_purge", purge = true)
      val out = Files.createTempDirectory("dropout-flip-")
      val out2 = capturingStdout {
        generated.UnpurgeDropCell.run(spark, cfg(db, out, dry = false))
      }
      assertFalse("the table must be dropped", tableNames(db).contains("part_purge"))
      assertTrue("the data directory must survive the drop", Files.exists(data))
      assertTrue("the partition data must still be there",
                 Files.exists(data.resolve("runid=aaa1")))
      val alterAt = out2.indexOf("ALTER  " + db + ".part_purge")
      val dropAt  = out2.indexOf("DROP   " + db + ".part_purge")
      assertTrue("the ALTER must be logged, got none", alterAt >= 0)
      assertTrue("the DROP must be logged, got none", dropAt >= 0)
      assertTrue("the ALTER must come BEFORE the DROP", alterAt < dropAt)
      assertTrue("the verification must run between them",
                 out2.indexOf("VERIFY " + db + ".part_purge") > alterAt)
    }

    check("a table already at purge=false is dropped without an ALTER") {
      val db = "dropdb_noalter"
      makeDb(db)
      extPartTable(db, "part_plain", purge = false)
      val out = Files.createTempDirectory("dropout-noalter-")
      val out2 = capturingStdout {
        generated.UnpurgeDropCell.run(spark, cfg(db, out, dry = false))
      }
      assertFalse("it must still be dropped", tableNames(db).contains("part_plain"))
      assertTrue("and the ALTER must be skipped, not issued",
                 out2.contains("SKIP ALTER " + db + ".part_plain"))
    }

    check("the artefacts name every table on the right side") {
      val db = "dropdb_art"
      makeDb(db)
      extPartTable(db, "part_purge", purge = true)
      extPlainTable(db, "plain_one")
      val out = Files.createTempDirectory("dropout-art-")
      generated.UnpurgeDropCell.run(spark, cfg(db, out, dry = true))

      val ddl = readArtifact(out, "dbprojection_ddl_backup.sql")
      assertTrue("the DDL backup must cover the in-scope table",
                 ddl.contains("part_purge"))
      assertTrue("and the out-of-scope one too - it is deliberately wider",
                 ddl.contains("plain_one"))

      val alterSql = readArtifact(out, "dbprojection_alter_purge.sql")
      assertTrue("the ALTER file must carry the in-scope table",
                 alterSql.contains("ALTER TABLE " + db + ".part_purge"))
      assertFalse("and must never carry the non-partitioned one",
                  alterSql.contains("plain_one"))

      val dropText = readArtifact(out, "dbprojection_drop_tables.sql")
      assertTrue("the DROP file must carry the in-scope table",
                 dropText.contains("DROP TABLE IF EXISTS " + db + ".part_purge;"))
      assertFalse("and must never carry the non-partitioned one",
                  dropText.contains("plain_one"))
      assertTrue("and must warn about running it before the ALTER",
                 dropText.contains("NEVER run this file before"))

      val oos = readArtifact(out, "dbprojection_out_of_scope.txt")
      assertTrue("the exclusion must be auditable", oos.contains("plain_one"))
      assertFalse("and must not list an in-scope table", oos.contains("part_purge"))

      val locs = readArtifact(out, "dbprojection_locations.txt")
      assertTrue("locations must be captured before any drop",
                 locs.contains("part_purge") && locs.contains("plain_one"))
    }

    // Re-running after a partial run is the realistic case: 85 tables, one
    // fails, you fix it and go again.
    check("a second run is a clean no-op") {
      val db = "dropdb_idem"
      makeDb(db)
      extPartTable(db, "part_purge", purge = true)
      extPlainTable(db, "plain_one")
      val out = Files.createTempDirectory("dropout-idem-")
      generated.UnpurgeDropCell.run(spark, cfg(db, out, dry = false))
      val after = tableNames(db)
      generated.UnpurgeDropCell.run(spark, cfg(db, out, dry = false))
      assertEquals("the second run must change nothing", after, tableNames(db))
      assertTrue("and the non-partitioned table must still be there",
                 tableNames(db).contains("plain_one"))
    }

    // The backup is the only way back from a DROP, so an unwritable one has to
    // stop the run before anything is altered.
    check("an unwritable DDL backup aborts before any change") {
      val db = "dropdb_nobackup"
      makeDb(db)
      extPartTable(db, "part_purge", purge = true)
      var msg = ""
      try {
        // a FILE where the output directory has to go, so the create cannot work
        val clash = Files.createTempFile("dropout-clash-", ".txt")
        generated.UnpurgeDropCell.run(spark, cfg(db, clash, dry = false))
      } catch { case e: RuntimeException => msg = String.valueOf(e.getMessage) }
      assertTrue("must abort naming the backup, got: " + msg,
                 msg.contains("DDL backup") || msg.contains("Refusing"))
      assertTrue("the table must be untouched", tableNames(db).contains("part_purge"))
      assertEquals("and its purge flag unchanged", "true", purgeOf(db, "part_purge"))
    }
  }

  private def fixAllTests(): Unit = {
    println("")
    println("fix_all_runid_tables.scala")

    def cfg(db: String, out: JPath, dry: Boolean) = Map(
      "DB" -> db, "OUTPUT_DIR" -> out.toUri.toString,
      "DRY_RUN" -> dry.toString)

    check("only tables partitioned by runid are in scope") {
      val db = "fixall_scope"
      makeDb(db)
      val hit = runidTable(db, "in_scope", Seq("aaa1"))
      extPlainTable(db, "no_parts")
      spark.sql(s"DROP TABLE IF EXISTS $db.other_key")
      spark.sql(s"CREATE EXTERNAL TABLE $db.other_key (a string) " +
                s"PARTITIONED BY (asofdate string) STORED AS ORC " +
                s"LOCATION '${uri(newRoot("fixall-otherkey-"))}'")
      spark.sql(s"DROP TABLE IF EXISTS $db.man_one")
      spark.sql(s"CREATE TABLE $db.man_one (a string) PARTITIONED BY (runid string) " +
                "STORED AS ORC")
      val out = Files.createTempDirectory("fixall-scope-")
      val log = capturingStdout {
        generated.FixAllCell.run(spark, cfg(db, out, dry = true))
      }
      assertTrue("the runid table must be in scope", log.contains("IN SCOPE  " + db + ".in_scope"))
      assertFalse("a non-partitioned table must not be", log.contains("IN SCOPE  " + db + ".no_parts"))
      assertFalse("nor one partitioned by another column",
                  log.contains("IN SCOPE  " + db + ".other_key"))
      assertFalse("nor a MANAGED one", log.contains("IN SCOPE  " + db + ".man_one"))
      assertTrue("and the MANAGED one must be named as out of scope",
                 log.contains("man_one") && log.contains("MANAGED"))
      assertTrue("the fixture must still be untouched by a dry run",
                 dirNames(hit) == Seq("runid=aaa1"))
    }

    check("DRY_RUN renames nothing and recreates nothing") {
      val db = "fixall_dry"
      makeDb(db)
      val root = runidTable(db, "t_one", Seq("aaa1", "bbb2"))
      val before = tree(root)
      val out = Files.createTempDirectory("fixall-dry-")
      generated.FixAllCell.run(spark, cfg(db, out, dry = true))
      assertEquals("the tree must be untouched", before, tree(root))
      assertFalse("and Spark must not yet carry the camelCase",
                  spark.table(s"$db.t_one").schema.fieldNames.contains("runId"))
    }

    check("both phases run, over every in-scope table") {
      if (!caseSensitiveFs) throw Skip("filesystem is not case sensitive")
      val db = "fixall_apply"
      makeDb(db)
      val r1 = runidTable(db, "t_one", Seq("aaa1", "bbb2"))
      val r2 = runidTable(db, "t_two", Seq("ccc3"))
      val out = Files.createTempDirectory("fixall-apply-")
      generated.FixAllCell.run(spark, cfg(db, out, dry = false))

      Seq(("t_one", r1, Seq("runId=aaa1", "runId=bbb2")),
          ("t_two", r2, Seq("runId=ccc3"))).foreach { case (t, root, want) =>
        assertEquals(s"$t : every dir must be re-cased on disk", want, dirNames(root))
        val cols = spark.table(s"$db.$t").schema.fieldNames
        assertTrue(s"$t : Spark's schema must carry runId, got " + cols.mkString(","),
                   cols.contains("runId"))
        assertEquals(s"$t : every partition must be registered",
                     want.length.toLong, spark.sql(s"SHOW PARTITIONS $db.$t").count())
        assertEquals(s"$t : the rows must still read back",
                     want.length.toLong, spark.table(s"$db.$t").count())
      }
    }

    check("a nested table is skipped by both phases and reported") {
      val db = "fixall_nested"
      makeDb(db)
      val root = runidTable(db, "t_nested", Seq("aaa1"))
      // turn runid=aaa1 into the double-nested defect
      val inner = root.resolve("runid=aaa1").resolve("runid=aaa1")
      Files.createDirectories(inner)
      val st = Files.list(root.resolve("runid=aaa1"))
      val moved = try st.iterator().asScala.filter(Files.isRegularFile(_)).toVector
                  finally st.close()
      moved.foreach(f => Files.move(f, inner.resolve(f.getFileName.toString)))
      val out = Files.createTempDirectory("fixall-nested-")
      val log = capturingStdout {
        generated.FixAllCell.run(spark, cfg(db, out, dry = false))
      }
      assertTrue("the nested dir must be left alone", Files.exists(inner))
      assertTrue("the run must say to flatten it first",
                 log.contains("flatten_nested_runid_partitions.scala"))
      assertTrue("and must list the table as still nested",
                 log.contains("STILL NESTED"))
      assertFalse("phase 2 must not have recreated it",
                  spark.table(s"$db.t_nested").schema.fieldNames.contains("runId"))
    }

    check("a second run is a clean no-op") {
      if (!caseSensitiveFs) throw Skip("filesystem is not case sensitive")
      val db = "fixall_idem"
      makeDb(db)
      val root = runidTable(db, "t_one", Seq("aaa1"))
      val out = Files.createTempDirectory("fixall-idem-")
      generated.FixAllCell.run(spark, cfg(db, out, dry = false))
      val afterFirst = tree(root)
      generated.FixAllCell.run(spark, cfg(db, out, dry = false))
      assertEquals("the second run must change nothing on disk", afterFirst, tree(root))
      assertTrue("and must leave Spark's schema correct",
                 spark.table(s"$db.t_one").schema.fieldNames.contains("runId"))
      assertEquals("with the partition still registered", 1L,
                   spark.sql(s"SHOW PARTITIONS $db.t_one").count())
    }

    check("ONLY_TABLES limits the batch to the named tables") {
      if (!caseSensitiveFs) throw Skip("filesystem is not case sensitive")
      val db = "fixall_only"
      makeDb(db)
      val r1 = runidTable(db, "t_one", Seq("aaa1"))
      val r2 = runidTable(db, "t_two", Seq("bbb2"))
      val out = Files.createTempDirectory("fixall-only-")
      generated.FixAllCell.run(spark,
        cfg(db, out, dry = false) + ("ONLY_TABLES" -> "t_one"))
      assertEquals("the named table must be fixed", Seq("runId=aaa1"), dirNames(r1))
      assertEquals("the other must be untouched", Seq("runid=bbb2"), dirNames(r2))
    }

    // The batch duplicates the two single-table algorithms, so the thing worth
    // pinning is that it still lands in the SAME state they do.
    check("the batch lands where the two single-table cells land") {
      if (!caseSensitiveFs) throw Skip("filesystem is not case sensitive")
      val db = "fixall_equiv"
      makeDb(db)
      val batchRoot  = runidTable(db, "t_batch", Seq("aaa1", "bbb2"))
      val singleRoot = runidTable(db, "t_single", Seq("aaa1", "bbb2"))

      val out = Files.createTempDirectory("fixall-equiv-")
      generated.FixAllCell.run(spark,
        cfg(db, out, dry = false) + ("ONLY_TABLES" -> "t_batch"))

      generated.RenameCell.run(spark, Map(
        "TABLE_ROOT" -> uri(singleRoot), "HIVE_TABLE" -> s"$db.t_single",
        "DRY_RUN" -> "false", "DDL_OUTPUT_PATH" -> (uri(out) + "/single-rename.sql")))
      generated.RecreateCell.run(spark, Map(
        "HIVE_TABLE" -> s"$db.t_single", "TABLE_ROOT" -> uri(singleRoot),
        "DRY_RUN" -> "false", "FIX_MODE" -> "recreate",
        "DDL_OUTPUT_PATH" -> (uri(out) + "/single-recreate.sql"),
        "BACKUP_OUTPUT_PATH" -> (uri(out) + "/single-backup.sql")))

      assertEquals("the on-disk layout must match",
                   dirNames(singleRoot), dirNames(batchRoot))
      // Spark names each ORC file part-<random uuid>, so the two fixtures can
      // never share file NAMES. What must match is the shape: which directory
      // each file ends up in, and how many.
      def shape(root: JPath) = tree(root).map(_.replaceAll("part-[^/]*$", "part-*"))
      assertEquals("the file layout must match", shape(singleRoot), shape(batchRoot))
      assertEquals("Spark's schema must match",
                   spark.table(s"$db.t_single").schema.fieldNames.toSeq,
                   spark.table(s"$db.t_batch").schema.fieldNames.toSeq)
      assertEquals("the partition count must match",
                   spark.sql(s"SHOW PARTITIONS $db.t_single").count(),
                   spark.sql(s"SHOW PARTITIONS $db.t_batch").count())
      assertEquals("and the rows must match",
                   spark.table(s"$db.t_single").count(),
                   spark.table(s"$db.t_batch").count())
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
      fixAllTests()
      dropTablesTests()
    } finally {
      if (spark != null) spark.stop()
    }

    println("")
    println("=" * 78)
    println(s"passed: $passed   failed: ${failures.size}   skipped: $skipped")
    if (nativeSkips > 0) {
      println("")
      println(s"  $nativeSkips test(s) skipped on Hadoop's native IO. This is a Windows")
      println("  limitation of THIS machine, not a defect in the scripts, and having")
      println("  winutils installed does not fix it:")
      println("    Hive creating a directory in the warehouse calls")
      println("    HdfsUtils.setFullFileStatus -> getGroup() -> NativeIO$POSIX.stat,")
      println("    and the Windows hadoop.dll does not export that POSIX symbol.")
      println("  hadoop.dll cannot simply be unloaded either: without it even")
      println("  RawLocalFileSystem.listStatus fails in NativeIO$Windows.access0.")
      println("  Everything that does not create a warehouse directory runs fine, and")
      println("  on Linux (and on the cluster) all of it runs.")
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
