# =====================================================================
#  recreate_table_partcol_runid.py               (PySpark -- Dataiku DSS)
#
#  README -- HOW TO RUN
#  --------------------
#    1. Set HIVE_TABLE / TABLE_ROOT and pick a FIX_MODE (see below).
#    2. Run with DRY_RUN = True (the default). NOTHING is modified; the
#       full DDL is printed and written to a .sql file.
#    3. Review the .sql AND the backup file, then set DRY_RUN = False.
#
#  GOAL
#  ----
#  Guarantee the table carries
#      'spark.sql.sources.schema.numPartCols' = '1'
#      'spark.sql.sources.schema.partCol.0'   = 'runId'
#  and that the field for the partition column inside the
#  'spark.sql.sources.schema' JSON is named `runId` too, so that Spark's view
#  of the table uses the camelCase name -- including when it creates
#  partition directories.
#
#  WHAT CANNOT BE CHANGED (and why the two cases coexist)
#  -----------------------------------------------------
#  The Hive metastore LOWERCASES every column name. There is no DDL that
#  makes it store `runId`:
#      SHOW CREATE TABLE  -> PARTITIONED BY (runid string)     always
#      SHOW PARTITIONS    -> runid=<uuid>                      always
#  So the Hive column stays `runid` and only Spark's schema properties carry
#  `runId`. That is not a defect, it is how Spark preserves column case on
#  Hive -- the same mechanism already keeps the camelCase data columns
#  (matrixMigrationName, asOfDate, notationCode) readable.
#
#  Consequence, and the reason this matters: Spark names the partition
#  DIRECTORIES after its own schema, so once partCol.0 is `runId` a Spark
#  write produces runId=<uuid>/ rather than runid=<uuid>/. That is what makes
#  the camelCase directory layout self-sustaining instead of something a
#  rename has to keep fixing up.
#
#  The script therefore RENAMES the partition field inside the schema JSON to
#  PARTITION_COL. Data columns are never re-cased.
#
#  TWO MODES -- read this before choosing
#  --------------------------------------
#  FIX_MODE = "alter"     (SAFE, recommended, no drop)
#      Rewrites only the TBLPROPERTIES in place:
#          ALTER TABLE ... SET TBLPROPERTIES (...)
#      Partitions, data and locations are untouched: nothing to re-register,
#      nothing to lose. Use this whenever partCol.0 is the only problem.
#
#  FIX_MODE = "recreate"  (DROP + CREATE, use only when the table
#                          definition itself is wrong)
#      Captures the schema, the location and EVERY partition location, drops
#      the table definition, recreates it, then re-adds each partition.
#      The data is NOT touched because the table is EXTERNAL -- but the
#      metastore entry is destroyed and rebuilt, so if the session dies
#      between DROP and the last ADD PARTITION you must replay the generated
#      .sql by hand. That file is written BEFORE the drop, on purpose.
#
#  SAFETY GUARDS (both modes abort rather than risk data)
#  ------------------------------------------------------
#    * the table must be EXTERNAL -- DROP TABLE on a MANAGED table deletes
#      the data directory;
#    * 'external.table.purge' must NOT be 'true' -- with that property set,
#      DROP TABLE deletes the data even for an EXTERNAL table;
#    * in "recreate" mode the partition inventory must be non-empty and
#      fully captured, otherwise the script refuses to drop.
# =====================================================================

from __future__ import print_function

import datetime
import json

# ---------------------------------------------------------------------
# 1. CONFIG
# ---------------------------------------------------------------------

HIVE_TABLE = "dbprojection.term_structure"
TABLE_ROOT = "/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/term_structure"
DRY_RUN    = True                # must stay True until the DDL is reviewed

FIX_MODE   = "recreate"          # "alter" (safe, in place) | "recreate" (drop + create)

# The partition column AS SPARK SEES IT. This is the case written into
# spark.sql.sources.schema.partCol.0 and into the schema JSON field name, and
# it is the case Spark uses when it creates partition directories.
#
# NOTE: the HIVE column is ALWAYS lowercase. The metastore lowercases every
# column name, so SHOW CREATE TABLE keeps showing PARTITIONED BY (runid string)
# and SHOW PARTITIONS keeps returning runid=<uuid> whatever you put here. Only
# Spark's own view of the table can carry the camelCase.
PARTITION_COL  = "runId"
PARTITION_TYPE = "string"
HIVE_PARTITION_COL = PARTITION_COL.lower()   # what the metastore actually stores

# Directory key used by the partition LOCATIONs when they have to be rebuilt.
# Set to "runId" if you ran rename_partitions_to_runId.py, "runid" otherwise.
# Only used in "recreate" mode for partitions whose location cannot be read
# back from the metastore.
ON_DISK_KEY = "runId"

DDL_OUTPUT_PATH    = "/Projects/STCreditRisk_UAT/tmp/recreate_table_ddl.sql"
BACKUP_OUTPUT_PATH = "/Projects/STCreditRisk_UAT/tmp/table_definition_backup.sql"

# ---------------------------------------------------------------------
# 2. Logging helpers
# ---------------------------------------------------------------------

def log(level, msg):
    print("[%s] %-7s %s" % (datetime.datetime.now().strftime("%H:%M:%S"), level, msg))


def section(title):
    print("")
    print("=" * 100)
    print(title)
    print("=" * 100)


MODE = "DRY-RUN" if DRY_RUN else "APPLY"

if FIX_MODE not in ("alter", "recreate"):
    raise ValueError("FIX_MODE must be 'alter' or 'recreate', got %r" % FIX_MODE)

# ---------------------------------------------------------------------
# 3. Spark bootstrap + FS handle (for writing the .sql artefacts)
# ---------------------------------------------------------------------

try:
    spark
except NameError:
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    sc    = SparkContext.getOrCreate()
    # enableHiveSupport() is REQUIRED: without it the session falls back to the
    # in-memory catalog and no Hive table is visible at all -- every metastore
    # call fails with TABLE_OR_VIEW_NOT_FOUND. Note that getOrCreate() returns
    # an ALREADY-EXISTING session unchanged, so if a non-Hive session is live
    # in this JVM you must restart the kernel for this to take effect.
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()


# ---------------------------------------------------------------------
# Catalog pre-flight
#   A missing table otherwise produces one stack trace per metastore call.
#   Check it once, up front, and report what the session can actually see.
# ---------------------------------------------------------------------

def table_exists(name):
    try:
        return bool(spark.catalog.tableExists(name))
    except Exception:                                           # noqa: BLE001
        try:
            spark.sql("DESCRIBE TABLE %s" % name).collect()
            return True
        except Exception:                                       # noqa: BLE001
            return False


def catalog_diagnostics(name):
    """Why can't we see the table? Returns a list of report lines."""
    out = []
    impl = "<unknown>"
    try:
        impl = spark.conf.get("spark.sql.catalogImplementation", "<unset>")
    except Exception:                                           # noqa: BLE001
        pass
    out.append("spark.sql.catalogImplementation = %s" % impl)

    db = name.split(".")[0] if "." in name else None
    try:
        dbs = sorted(str(r[0]) for r in spark.sql("SHOW DATABASES").collect())
        out.append("databases visible (%d): %s"
                   % (len(dbs), ", ".join(dbs[:25]) + (" ..." if len(dbs) > 25 else "")))
        if db and db not in dbs:
            out.append("-> database %r is NOT in that list" % db)
        elif db:
            try:
                tbs = sorted(str(r[1]) for r in spark.sql("SHOW TABLES IN %s" % db).collect())
                out.append("tables in %s (%d): %s"
                           % (db, len(tbs), ", ".join(tbs[:25]) + (" ..." if len(tbs) > 25 else "")))
            except Exception as exc:                            # noqa: BLE001
                out.append("SHOW TABLES IN %s failed: %s" % (db, exc))
    except Exception as exc:                                    # noqa: BLE001
        out.append("SHOW DATABASES failed: %s" % exc)

    if impl != "hive":
        out.append("-> this session has NO Hive support, so no Hive table can be found.")
        out.append("   Restart the kernel and re-run: the bootstrap builds the session")
        out.append("   with enableHiveSupport(), but getOrCreate() reuses an existing")
        out.append("   non-Hive session unchanged.")
    else:
        out.append("-> Hive support is on, so check the database/table name against the")
        out.append("   listing above (and current_catalog()/current_schema()).")
    return out

jvm  = spark._jvm
conf = spark._jsc.hadoopConfiguration()
Path       = jvm.org.apache.hadoop.fs.Path
FileSystem = jvm.org.apache.hadoop.fs.FileSystem
URI        = jvm.java.net.URI


def write_text(path, text):
    """Write a text artefact through the Hadoop FS API (no local FS assumption)."""
    if not path:
        return
    try:
        wfs = FileSystem.get(URI.create(path), conf)
        p   = Path(path)
        parent = p.getParent()
        if parent is not None and not wfs.exists(parent):
            wfs.mkdirs(parent)
        out = wfs.create(p, True)
        out.write(bytearray(text.encode("utf-8")))
        out.close()
        log("OK", "written %s" % path)
        return True
    except Exception as exc:                                    # noqa: BLE001
        log("ERROR", "could not write %s : %s" % (path, exc))
        return False


def describe_rows(sql):
    return [(str(r[0] or ""), str(r[1] or ""), str(r[2] or "") if len(r) > 2 else "")
            for r in spark.sql(sql).collect()]


# ---------------------------------------------------------------------
# 4. Capture the current definition
# ---------------------------------------------------------------------

section("1/5  CAPTURE  (mode=%s, fix_mode=%s)  table=%s" % (MODE, FIX_MODE, HIVE_TABLE))

# Everything below reads the table definition. If the table is not visible at
# all, fail once with a useful message instead of one stack trace per query.
if not table_exists(HIVE_TABLE):
    raise RuntimeError(
        "ABORT: table %s is not visible to this Spark session, so its definition "
        "cannot be captured. Nothing was modified.\n  %s"
        % (HIVE_TABLE, "\n  ".join(catalog_diagnostics(HIVE_TABLE))))
log("OK", "table %s is visible to this session" % HIVE_TABLE)

show_create = ""
try:
    show_create = "\n".join(str(r[0]) for r in
                            spark.sql("SHOW CREATE TABLE %s" % HIVE_TABLE).collect())
    print(show_create)
except Exception as exc:                                        # noqa: BLE001
    log("WARN", "SHOW CREATE TABLE failed: %s" % exc)

table_type = None
location   = None
data_cols  = []              # [(name, type)] excluding the partition column
part_cols  = []              # [(name, type)]
detail     = {}              # the '# Detailed Table Information' key/value block

# DESCRIBE FORMATTED emits three blocks in order:
#   1. the data columns
#   2. '# Partition Information' -> the partition columns
#   3. '# Detailed Table Information' (and '# Storage Information') -> key/value
# Tracking the block explicitly matters: without it the detail rows
# (Database, Owner, Provider, ...) get mistaken for partition columns.
block = "cols"
try:
    for col, typ, _cmt in describe_rows("DESCRIBE FORMATTED %s" % HIVE_TABLE):
        c = col.strip()
        if c.startswith("# Partition Information"):
            block = "partcols"
            continue
        if c.startswith("# Detailed Table Information") or c.startswith("# Storage Information"):
            block = "detail"
            continue
        if c.startswith("#") or c == "":          # '# col_name' headers, blank rows
            continue
        t = typ.strip()
        if block == "cols" and t:
            data_cols.append((c, t))
        elif block == "partcols" and t:
            part_cols.append((c, t))
        elif block == "detail":
            detail[c.rstrip(":")] = t
except Exception as exc:                                        # noqa: BLE001
    log("WARN", "DESCRIBE FORMATTED failed: %s" % exc)

for k in ("Table Type", "Type"):
    if k in detail:
        v = detail[k].upper()
        table_type = "EXTERNAL" if "EXTERNAL" in v else ("MANAGED" if "MANAGED" in v else None)
        break
location = detail.get("Location") or None

# The partition column is also listed among the data columns; drop it there.
part_names = set(n.lower() for n, _ in part_cols) or {HIVE_PARTITION_COL}
data_cols  = [(n, t) for n, t in data_cols if n.lower() not in part_names]

if not location:
    location = TABLE_ROOT
    log("WARN", "no Location read from the metastore, falling back to TABLE_ROOT")

# external.table.purge decides whether DROP TABLE deletes the data, so it has
# to be read reliably -- not guessed.
tblprops = {}
tblprops_read_ok = False
try:
    tblprops = dict((str(r[0]), str(r[1]))
                    for r in spark.sql("SHOW TBLPROPERTIES %s" % HIVE_TABLE).collect())
    tblprops_read_ok = True
except Exception as exc:                                        # noqa: BLE001
    log("WARN", "SHOW TBLPROPERTIES failed: %s" % exc)

purge_flag = tblprops.get("external.table.purge")
if purge_flag is None:
    # Fallback: the same properties appear as a blob in DESCRIBE FORMATTED.
    blob = detail.get("Table Properties", "")
    if "external.table.purge=true" in blob.replace(" ", "").lower():
        purge_flag = "true"
    elif tblprops_read_ok:
        purge_flag = "false"

log("INFO", "table type        : %s" % (table_type or "UNKNOWN"))
log("INFO", "external.table.purge : %s" % purge_flag)
log("INFO", "location          : %s" % location)
log("INFO", "data columns      : %d  %s" % (len(data_cols),
                                            ", ".join("%s %s" % c for c in data_cols)))
log("INFO", "partition columns : %s" % (", ".join("%s %s" % c for c in part_cols)
                                        or "(none read)"))
log("INFO", "tblproperties     : %d" % len(tblprops))

# ---------------------------------------------------------------------
# 5. Guards
# ---------------------------------------------------------------------

section("2/5  GUARDS")

if table_type == "MANAGED":
    raise RuntimeError(
        "ABORT: %s is a MANAGED_TABLE. DROP TABLE would DELETE the data directory %s. "
        "Nothing was modified." % (HIVE_TABLE, location))
if table_type != "EXTERNAL":
    raise RuntimeError(
        "ABORT: could not confirm %s is EXTERNAL (got %r). Refusing to touch the table "
        "definition. Nothing was modified." % (HIVE_TABLE, table_type))
log("OK", "table is EXTERNAL")

if str(purge_flag).strip().lower() == "true":
    raise RuntimeError(
        "ABORT: %s has external.table.purge=true. DROP TABLE would DELETE the data even "
        "though the table is EXTERNAL. Unset it first:\n"
        "  ALTER TABLE %s UNSET TBLPROPERTIES ('external.table.purge');\n"
        "Nothing was modified." % (HIVE_TABLE, HIVE_TABLE))
if purge_flag is None and FIX_MODE == "recreate":
    raise RuntimeError(
        "ABORT: could not read the TBLPROPERTIES of %s, so external.table.purge cannot "
        "be ruled out. If it were true, DROP TABLE would DELETE the data. Refusing to "
        "recreate. Use FIX_MODE='alter', or fix the metastore access first. Nothing was "
        "modified." % HIVE_TABLE)
log("OK", "external.table.purge is not set -> DROP TABLE is metastore-only")

if not data_cols:
    raise RuntimeError(
        "ABORT: no data columns could be read from DESCRIBE FORMATTED %s. Recreating the "
        "table from an empty schema would destroy the definition. Nothing was modified."
        % HIVE_TABLE)

# --- partition inventory (needed to rebuild the table) ----------------
partitions = []          # [(value, location)]
try:
    specs = [str(r[0]) for r in spark.sql("SHOW PARTITIONS %s" % HIVE_TABLE).collect()]
    for spec in specs:
        pkey, _, pval = spec.partition("=")
        loc = None
        try:
            for col, typ, _c in describe_rows("DESCRIBE FORMATTED %s PARTITION (%s='%s')"
                                              % (HIVE_TABLE, pkey, pval)):
                if col.strip().rstrip(":") == "Location":
                    loc = typ.strip()
                    break
        except Exception as exc:                                # noqa: BLE001
            log("WARN", "DESCRIBE failed for %s : %s" % (spec, exc))
        if loc is None:
            loc = "%s/%s=%s" % (location.rstrip("/"), ON_DISK_KEY, pval)
            log("WARN", "no location for %s, assuming %s" % (spec, loc))
        partitions.append((pval, loc))
    log("INFO", "partitions captured: %d" % len(partitions))
except Exception as exc:                                        # noqa: BLE001
    log("WARN", "SHOW PARTITIONS failed: %s" % exc)

if FIX_MODE == "recreate" and not partitions:
    raise RuntimeError(
        "ABORT: no partition could be captured for %s. Dropping the table now would lose "
        "every partition registration with no way to rebuild it. Nothing was modified."
        % HIVE_TABLE)

# ---------------------------------------------------------------------
# 6. Build the DDL
# ---------------------------------------------------------------------

section("3/5  GENERATED DDL")

# --- the corrected schema JSON ----------------------------------------
# It must be the JSON shape Spark itself produces: DataType.fromJson only
# accepts primitive names as bare strings, so a nested type such as
# array<array<double>> has to be a nested JSON object, NOT the DDL string.
# Reading it back from Spark also preserves the existing column-name casing
# (the camelCase data columns are normal Spark-on-Hive behaviour and must
# not be "fixed" here).
schema_json   = None
schema_source = None

try:
    schema_json   = spark.table(HIVE_TABLE).schema.json()
    schema_source = "spark.table(%s).schema" % HIVE_TABLE
except Exception as exc:                                        # noqa: BLE001
    log("WARN", "could not read the schema through spark.table(): %s" % exc)

if schema_json is None:
    # Fallback: rebuild it from DESCRIBE FORMATTED via Spark's own DDL parser,
    # so nested types still come out in the correct nested-object form.
    try:
        from pyspark.sql.types import _parse_datatype_string
        ddl = ", ".join("`%s` %s" % (n, t)
                        for n, t in data_cols + [(PARTITION_COL, PARTITION_TYPE)])
        schema_json   = _parse_datatype_string(ddl).json()
        schema_source = "parsed from DESCRIBE FORMATTED"
    except Exception as exc:                                    # noqa: BLE001
        log("WARN", "could not parse the schema from DESCRIBE FORMATTED: %s" % exc)

if schema_json is None:
    raise RuntimeError(
        "ABORT: the schema of %s could not be captured in Spark's JSON form. Writing "
        "spark.sql.sources.schema by hand would risk an unreadable table definition. "
        "Nothing was modified." % HIVE_TABLE)

# The partition column must be present in the schema JSON, and last.
parsed = json.loads(schema_json)
names  = [f["name"] for f in parsed["fields"]]
hit    = [i for i, n in enumerate(names) if n.lower() == HIVE_PARTITION_COL]
if not hit:
    parsed["fields"].append({"name": PARTITION_COL, "type": PARTITION_TYPE,
                             "nullable": True, "metadata": {}})
    names.append(PARTITION_COL)
    schema_json = json.dumps(parsed)
    log("WARN", "partition column %r was missing from the schema JSON - appended"
                % PARTITION_COL)
elif names[hit[0]] != PARTITION_COL:
    # This rename is the whole point: Hive cannot store the camelCase, so the
    # schema JSON is the only place the wanted case can live.
    was = names[hit[0]]
    parsed["fields"][hit[0]]["name"] = PARTITION_COL
    names[hit[0]] = PARTITION_COL
    schema_json = json.dumps(parsed)
    log("INFO", "partition column re-cased in the schema JSON: %r -> %r"
                % (was, PARTITION_COL))

log("INFO", "schema source     : %s" % schema_source)
log("INFO", "schema fields     : %s" % ", ".join(names))

# Properties Spark/Hive regenerate on their own -- never replay them.
VOLATILE = ("transient_lastDdlTime", "totalSize", "numFiles", "numRows",
            "rawDataSize", "numPartitions", "COLUMN_STATS_ACCURATE",
            "last_modified_time", "last_modified_by", "spark.sql.create.version")

preserved = dict((k, v) for k, v in tblprops.items()
                 if k not in VOLATILE and not k.startswith("spark.sql.sources.schema"))
preserved["spark.sql.sources.schema"] = schema_json
preserved["spark.sql.sources.schema.numPartCols"] = "1"
preserved["spark.sql.sources.schema.partCol.0"] = PARTITION_COL
preserved.setdefault("spark.sql.partitionProvider", "catalog")


def props_block(d, indent="  "):
    items = ["%s'%s'='%s'" % (indent, k, str(v).replace("'", "\\'"))
             for k, v in sorted(d.items())]
    return ",\n".join(items)


statements = []

if FIX_MODE == "alter":
    statements.append(
        "ALTER TABLE %s SET TBLPROPERTIES (\n%s\n);"
        % (HIVE_TABLE,
           props_block({"spark.sql.sources.schema": schema_json,
                        "spark.sql.sources.schema.numPartCols": "1",
                        "spark.sql.sources.schema.partCol.0": PARTITION_COL})))
else:
    cols_ddl = ",\n".join("  `%s` %s" % (n, t) for n, t in data_cols)
    statements.append("DROP TABLE IF EXISTS %s;" % HIVE_TABLE)
    statements.append(
        "CREATE EXTERNAL TABLE %s (\n%s\n)\n"
        "PARTITIONED BY (\n  `%s` %s)\n"
        "ROW FORMAT SERDE\n  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'\n"
        "STORED AS INPUTFORMAT\n  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'\n"
        "OUTPUTFORMAT\n  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'\n"
        "LOCATION\n  '%s'\n"
        "TBLPROPERTIES (\n%s\n);"
        % (HIVE_TABLE, cols_ddl, PARTITION_COL, PARTITION_TYPE, location,
           props_block(preserved)))
    # CREATE may normalise the schema properties away; re-assert them.
    statements.append(
        "ALTER TABLE %s SET TBLPROPERTIES (\n%s\n);"
        % (HIVE_TABLE,
           props_block({"spark.sql.sources.schema": schema_json,
                        "spark.sql.sources.schema.numPartCols": "1",
                        "spark.sql.sources.schema.partCol.0": PARTITION_COL})))
    for value, loc in partitions:
        statements.append(
            "ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s='%s') LOCATION '%s';"
            % (HIVE_TABLE, HIVE_PARTITION_COL, value, loc))

header = ("-- generated by recreate_table_partcol_runid.py on %s\n"
          "-- table     : %s   (type: %s, purge: %s)\n"
          "-- fix mode  : %s\n"
          "-- run mode  : %s\n"
          "-- partitions: %d re-registered\n"
          % (datetime.datetime.now().isoformat(), HIVE_TABLE, table_type, purge_flag,
             FIX_MODE, MODE, len(partitions) if FIX_MODE == "recreate" else 0))
ddl_text = header + "\n" + "\n\n".join(statements) + "\n"
print(ddl_text)

# ---------------------------------------------------------------------
# 7. Backup + execution
# ---------------------------------------------------------------------

section("4/5  BACKUP & EXECUTION  (mode=%s)" % MODE)

backup_text = (
    "-- BACKUP of %s taken on %s\n"
    "-- Replay this file to restore the definition if the recreate goes wrong.\n\n"
    "-- ===== SHOW CREATE TABLE =====\n%s\n\n"
    "-- ===== TBLPROPERTIES =====\n%s\n\n"
    "-- ===== PARTITIONS (%d) =====\n%s\n"
    % (HIVE_TABLE, datetime.datetime.now().isoformat(),
       show_create or "-- (unavailable)",
       "\n".join("-- %s = %s" % (k, v) for k, v in sorted(tblprops.items())) or "-- (none)",
       len(partitions),
       "\n".join("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s='%s') LOCATION '%s';"
                 % (HIVE_TABLE, HIVE_PARTITION_COL, v, l) for v, l in partitions) or "-- (none)"))

backup_ok = write_text(BACKUP_OUTPUT_PATH, backup_text)
write_text(DDL_OUTPUT_PATH, ddl_text)

if DRY_RUN:
    log("INFO", "DRY_RUN=True -> nothing executed. Review %s and %s."
                % (DDL_OUTPUT_PATH, BACKUP_OUTPUT_PATH))
elif FIX_MODE == "recreate" and not backup_ok:
    raise RuntimeError(
        "ABORT: the backup file could not be written to %s. Refusing to DROP the table "
        "without a replayable backup. Nothing was modified." % BACKUP_OUTPUT_PATH)
else:
    failures = 0
    for stmt in statements:
        sql = stmt.strip().rstrip(";")
        if not sql or sql.startswith("--"):
            continue
        try:
            spark.sql(sql)
            log("OK", "EXEC   %s" % sql.replace("\n", " ")[:160])
        except Exception as exc:                                # noqa: BLE001
            failures += 1
            log("ERROR", "SQL FAILED (%s): %s" % (sql.replace("\n", " ")[:160], exc))
            if sql.upper().startswith("DROP TABLE"):
                raise RuntimeError("DROP TABLE failed, aborting before CREATE: %s" % exc)
    if failures:
        log("ERROR", "%d statement(s) failed -- replay %s by hand"
                     % (failures, DDL_OUTPUT_PATH))

# ---------------------------------------------------------------------
# 8. Verification
# ---------------------------------------------------------------------

section("5/5  VERIFICATION")

if DRY_RUN:
    log("INFO", "DRY_RUN=True -> verification skipped")
else:
    try:
        after = dict((str(r[0]), str(r[1]))
                     for r in spark.sql("SHOW TBLPROPERTIES %s" % HIVE_TABLE).collect())
        for k in ("spark.sql.sources.schema.numPartCols",
                  "spark.sql.sources.schema.partCol.0"):
            log("OK" if after.get(k) else "ERROR", "%s = %s" % (k, after.get(k, "<missing>")))
        if after.get("spark.sql.sources.schema.partCol.0") != PARTITION_COL:
            log("ERROR", "partCol.0 is %r, expected %r"
                         % (after.get("spark.sql.sources.schema.partCol.0"), PARTITION_COL))
    except Exception as exc:                                    # noqa: BLE001
        log("ERROR", "SHOW TBLPROPERTIES failed: %s" % exc)

    try:
        n = len(spark.sql("SHOW PARTITIONS %s" % HIVE_TABLE).collect())
        log("OK" if n == len(partitions) or FIX_MODE == "alter" else "ERROR",
            "partitions after: %d (captured before: %d)" % (n, len(partitions)))
    except Exception as exc:                                    # noqa: BLE001
        log("ERROR", "SHOW PARTITIONS failed: %s" % exc)

    try:
        spark.sql("SELECT %s, count(*) AS n FROM %s GROUP BY %s"
                  % (PARTITION_COL, HIVE_TABLE, PARTITION_COL)).show(100, False)
    except Exception as exc:                                    # noqa: BLE001
        log("ERROR", "count per %s failed: %s" % (PARTITION_COL, exc))

print("")
print("table            : %s" % HIVE_TABLE)
print("fix mode         : %s" % FIX_MODE)
print("table type       : %s   (purge: %s)" % (table_type, purge_flag))
print("location         : %s" % location)
print("data columns     : %d" % len(data_cols))
print("partitions       : %d" % len(partitions))
print("statements       : %d %s" % (len(statements),
                                    "(printed only, DRY_RUN)" if DRY_RUN else "(executed)"))
print("ddl file         : %s" % DDL_OUTPUT_PATH)
print("backup file      : %s" % BACKUP_OUTPUT_PATH)
print("")
if DRY_RUN:
    print(">>> DRY RUN: nothing was changed. Review the DDL and the backup, then set "
          "DRY_RUN = False.")
elif FIX_MODE == "recreate":
    print(">>> RECREATED. If anything failed above, replay %s by hand -- the pre-drop "
          "definition is in %s." % (DDL_OUTPUT_PATH, BACKUP_OUTPUT_PATH))
else:
    print(">>> TBLPROPERTIES updated in place. No partition was touched.")
