# =====================================================================
#  rename_partitions_to_runId.py                 (PySpark -- Dataiku DSS)
#
#  README -- HOW TO RUN
#  --------------------
#    1. Set TABLE_ROOT / HIVE_TABLE in the CONFIG block below.
#    2. Run with DRY_RUN = True (the default). NOTHING is modified.
#    3. Review the plan and the generated .sql, then set DRY_RUN = False.
#    4. Re-run once more: idempotent, the second run must report 0 rename.
#
#  WHAT IT DOES
#  ------------
#  Makes the on-disk partition directory casing uniformly `runId=<uuid>`
#  (camelCase I), and re-points the metastore at the new locations:
#
#      runid=<uuid>/   ->   runId=<uuid>/          (HDFS, atomic rename)
#      ALTER TABLE ... DROP PARTITION (runid='<uuid>');
#      ALTER TABLE ... ADD  PARTITION (runid='<uuid>')
#                     LOCATION '<root>/runId=<uuid>';
#
#  The METASTORE COLUMN STAYS LOWERCASE `runid`. Only the directory name
#  changes. That combination is legal because the table is registered with
#  spark.sql.partitionProvider=catalog: Spark reads partitions from the
#  metastore with an explicit LOCATION per partition, so the directory name
#  does not have to match the column name.
#
#  READ THIS BEFORE RUNNING
#  ------------------------
#  `runId=` on disk is only stable while EVERY writer builds its paths by
#  hand. Anything that writes through the Hive table -- INSERT INTO,
#  df.write.saveAsTable(...).partitionBy("runid"), dynamic partition
#  overwrite -- names the directory after the CATALOG column, i.e.
#  lowercase `runid=`. One such write re-introduces the mixed casing.
#  If that is a risk in your pipelines, prefer the opposite direction:
#  flatten_nested_runid_partitions.py normalises everything to lowercase
#  `runid=`, which is what Spark itself produces.
#
#  PRE-REQUISITE
#  -------------
#  The tree must already be FLAT. If any first-level directory still holds
#  a nested runid= directory (the double-nesting defect), this script
#  refuses to touch it and tells you to run
#  flatten_nested_runid_partitions.py first. Renaming a wrapper would just
#  move the broken nesting under a new name.
#
#  SAFETY
#  ------
#  UAT data with no backup: HDFS renames are metadata-only (no file is
#  read, rewritten or recompressed), every action is logged with its full
#  path, and every ambiguous case is skipped and reported.
#  The table must be EXTERNAL -- DROP PARTITION on a MANAGED table would
#  delete the data. The script aborts if it is not.
# =====================================================================

from __future__ import print_function

import datetime
import uuid as _uuid

# ---------------------------------------------------------------------
# 1. CONFIG
# ---------------------------------------------------------------------

TABLE_ROOT = "/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/term_structure"
DRY_RUN    = True          # must stay True until the plan has been reviewed
HIVE_TABLE = "dbprojection.term_structure"
EMIT_HIVE_DDL = True

# Desired on-disk directory key (case-sensitive) and the metastore column
# name (Hive stores partition column names lowercase -- do NOT change it).
TARGET_KEY  = "runId"
CATALOG_KEY = "runid"

DDL_OUTPUT_PATH = "/Projects/STCreditRisk_UAT/tmp/generated_rename_partition_ddl.sql"

# When both runid=<X> and runId=<X> exist, move the files of the lowercase
# one into the camelCase one (renaming on collision) instead of skipping.
MERGE_ON_COLLISION = True

# Markers are moved with the directory on the atomic-rename path. On the
# merge path they are left in place unless this is True.
DELETE_MARKERS_ON_MERGE = False

METASTORE_PREFLIGHT = True

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

# ---------------------------------------------------------------------
# 3. Spark bootstrap
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

# ---------------------------------------------------------------------
# 4. Hadoop FileSystem handle (JVM gateway; every result is a Java proxy)
# ---------------------------------------------------------------------

jvm  = spark._jvm
jsc  = spark._jsc
conf = jsc.hadoopConfiguration()

Path       = jvm.org.apache.hadoop.fs.Path
FileSystem = jvm.org.apache.hadoop.fs.FileSystem
URI        = jvm.java.net.URI

fs   = FileSystem.get(URI.create(TABLE_ROOT), conf)
root = Path(TABLE_ROOT)


def pstr(p):
    return str(p.toString())


def pname(p):
    return str(p.getName())


if not fs.exists(root):
    raise RuntimeError("Table root does not exist: %s" % TABLE_ROOT)


def ls(path):
    return sorted(list(fs.listStatus(path)), key=lambda s: pname(s.getPath()))


def is_protected(name):
    return name.startswith("_") or name.startswith(".")


def split_key(name):
    name = str(name)
    if "=" not in name:
        return name, None
    k, v = name.split("=", 1)
    return k, v


def path_only(uri):
    """Strip scheme and authority so two locations can be compared.

    Handles both URI shapes Hive hands back:
        hdfs://nameservice/a/b   (scheme + authority)
        file:/a/b                (scheme, NO authority)
    Without the second case a location the metastore reports as file:/x is
    never recognised as living under a table root written file:///x."""
    uri = str(uri).rstrip("/")
    i = uri.find("://")
    if i >= 0:
        j = uri.find("/", i + 3)
        return uri[j:] if j >= 0 else "/"
    c = uri.find(":/")
    # the scheme must be longer than one char, so a Windows drive letter
    # (C:/...) is not mistaken for a URI scheme
    if c > 1 and "/" not in uri[:c]:
        return uri[c + 1:]
    return uri


ROOT_PATH_ONLY = path_only(TABLE_ROOT)


def human(nbytes):
    v = float(nbytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if v < 1024.0:
            return "%.2f %s" % (v, unit)
        v /= 1024.0
    return "%.2f PB" % v


# ---------------------------------------------------------------------
# 5. Discovery (read-only)
# ---------------------------------------------------------------------

section("1/6  DISCOVERY  (mode=%s)  root=%s" % (MODE, TABLE_ROOT))
log("INFO", "target on-disk key = '%s='   metastore column = '%s'"
            % (TARGET_KEY, CATALOG_KEY))

plans = []          # dirs to rename / merge
already = []        # already named TARGET_KEY=
skipped = []        # (path, reason)
strays = []         # (path, reason)
protected_seen = []

for st in ls(root):
    name = pname(st.getPath())
    full = pstr(st.getPath())

    if is_protected(name):
        protected_seen.append((full, "protected entry at the table root - left untouched"))
        continue

    if not st.isDirectory():
        strays.append((full, "file directly under the table root"))
        continue

    key, value = split_key(name)
    if value is None:
        strays.append((full, "directory without a 'key=value' name"))
        continue

    if key.lower() != CATALOG_KEY:
        strays.append((full, "unrelated partition key '%s=' - not touched" % key))
        continue

    # Refuse to rename anything that still carries the double-nesting defect:
    # renaming it would only move the broken tree under a new name.
    children  = ls(st.getPath())
    child_dirs = [c for c in children
                  if c.isDirectory() and not is_protected(pname(c.getPath()))]
    nested = [c for c in child_dirs
              if split_key(pname(c.getPath()))[0].lower() == CATALOG_KEY]
    if nested:
        skipped.append((full, "still holds a nested '%s' dir - run "
                              "flatten_nested_runid_partitions.py FIRST, then re-run "
                              "this script" % pname(nested[0].getPath())))
        continue
    if child_dirs:
        skipped.append((full, "contains sub-directories (%s) - unexpected layout, "
                              "review manually"
                              % ", ".join(pname(c.getPath()) for c in child_dirs)))
        continue

    if key == TARGET_KEY:
        already.append((full, value))
        continue

    target = Path(root, "%s=%s" % (TARGET_KEY, value))
    target_exists = fs.exists(target)

    data  = [c for c in children if not is_protected(pname(c.getPath()))]
    prot  = [c for c in children if is_protected(pname(c.getPath()))]
    nbytes = sum(c.getLen() for c in data if not c.isDirectory())

    if target_exists and not MERGE_ON_COLLISION:
        skipped.append((full, "target %s already exists and MERGE_ON_COLLISION=False"
                              % pstr(target)))
        continue

    for c in prot:
        protected_seen.append((pstr(c.getPath()), "marker inside %s" % name))

    plans.append({
        "kind": "merge" if target_exists else "rename",
        "src": st.getPath(),
        "target": target,
        "value": value,
        "src_key": key,
        "files": len(data),
        "bytes": nbytes,
        "protected": [pname(c.getPath()) for c in prot],
    })

log("INFO", "already '%s=' partitions : %d" % (TARGET_KEY, len(already)))
log("INFO", "to rename                : %d" % len([p for p in plans if p["kind"] == "rename"]))
log("INFO", "to merge (both cases)    : %d" % len([p for p in plans if p["kind"] == "merge"]))
log("INFO", "skipped                  : %d" % len(skipped))
log("INFO", "stray entries            : %d" % len(strays))

# ---------------------------------------------------------------------
# 6. Metastore pre-flight
# ---------------------------------------------------------------------

section("2/6  METASTORE PRE-FLIGHT  (table=%s)" % HIVE_TABLE)

table_type = None
registered = {}          # value -> location
ddl_execution_allowed = True


def describe_rows(sql):
    return [(str(r[0] or ""), str(r[1] or "")) for r in spark.sql(sql).collect()]


if METASTORE_PREFLIGHT and not table_exists(HIVE_TABLE):
    ddl_execution_allowed = False
    log("ERROR", "table %s is NOT visible to this Spark session" % HIVE_TABLE)
    for _line in catalog_diagnostics(HIVE_TABLE):
        log("ERROR", "  %s" % _line)
    log("ERROR", "-> the HDFS renames below can still run (they need no metastore),")
    log("ERROR", "   but NO partition DDL will be executed. Fix the table name or the")
    log("ERROR", "   Hive support first if you want the metastore re-pointed.")
elif METASTORE_PREFLIGHT:
    try:
        for col, val in describe_rows("DESCRIBE FORMATTED %s" % HIVE_TABLE):
            if col.strip().rstrip(":") in ("Table Type", "Type"):
                v = val.strip().upper()
                table_type = "EXTERNAL" if "EXTERNAL" in v else (
                             "MANAGED" if "MANAGED" in v else None)
                break
    except Exception as exc:                                    # noqa: BLE001
        log("WARN", "DESCRIBE FORMATTED %s failed: %s" % (HIVE_TABLE, exc))

    if table_type == "MANAGED":
        raise RuntimeError(
            "ABORT: %s is a MANAGED_TABLE. ALTER TABLE ... DROP PARTITION would DELETE "
            "the underlying HDFS data. Nothing was modified." % HIVE_TABLE)
    if table_type == "EXTERNAL":
        log("OK", "table type confirmed EXTERNAL -> DROP PARTITION is metastore-only")
    else:
        ddl_execution_allowed = False
        log("ERROR", "could not confirm the table type is EXTERNAL. The HDFS renames "
                     "will still run, but NO DDL will be executed.")

    try:
        specs = [str(r[0]) for r in spark.sql("SHOW PARTITIONS %s" % HIVE_TABLE).collect()]
        for spec in specs:
            pkey, _, pval = spec.partition("=")
            try:
                rows = describe_rows("DESCRIBE FORMATTED %s PARTITION (%s='%s')"
                                     % (HIVE_TABLE, pkey, pval))
            except Exception as exc:                            # noqa: BLE001
                log("WARN", "DESCRIBE failed for %s : %s" % (spec, exc))
                continue
            for col, val in rows:
                if col.strip().rstrip(":") == "Location":
                    registered[pval] = val.strip()
                    break
        log("INFO", "registered partitions: %d" % len(registered))
    except Exception as exc:                                    # noqa: BLE001
        log("WARN", "SHOW PARTITIONS failed: %s" % exc)

    renaming_values = set(p["value"] for p in plans)
    not_registered = sorted(v for v in renaming_values if v not in registered)
    if not_registered:
        log("WARN", "%d director(ies) being renamed have NO metastore partition; they "
                    "will be ADDed as new" % len(not_registered))
        for v in not_registered:
            log("WARN", "  unregistered %s=%s" % (CATALOG_KEY, v))
else:
    ddl_execution_allowed = False
    log("WARN", "METASTORE_PREFLIGHT=False -> no table-type guard, DDL not executed")

# ---------------------------------------------------------------------
# 7. Plan
# ---------------------------------------------------------------------

section("3/6  PLANNED ACTIONS")

if not plans:
    log("INFO", "nothing to rename - every partition dir already uses '%s=' "
                "(idempotent no-op)" % TARGET_KEY)

for p in plans:
    if p["kind"] == "rename":
        log("PLAN", "RENAME %s -> %s   (%d file(s), %s)"
                    % (pstr(p["src"]), pstr(p["target"]), p["files"], human(p["bytes"])))
    else:
        log("PLAN", "MERGE  %d file(s) (%s) %s -> %s   [both casings exist]"
                    % (p["files"], human(p["bytes"]), pstr(p["src"]), pstr(p["target"])))
        log("PLAN", "DELETE %s   (emptied source dir)" % pstr(p["src"]))
        if p["protected"]:
            log("PLAN", "KEEP   protected entries in %s : %s"
                        % (pstr(p["src"]), ", ".join(p["protected"])))

# ---------------------------------------------------------------------
# 8. Execution
# ---------------------------------------------------------------------

section("4/6  EXECUTION  (mode=%s)" % MODE)

renamed_values = []
counts = {"rename": 0, "merge": 0, "files_moved": 0, "dirs_deleted": 0, "failed": 0}


def delete_if_empty(path, label):
    remaining = list(fs.listStatus(path))
    if remaining:
        log("KEEP", "%s NOT deleted, still contains [%s] -- %s"
                    % (label, ", ".join(pname(s.getPath()) for s in remaining), pstr(path)))
        return False
    if fs.delete(path, True):
        log("OK", "DELETE %s   (%s)" % (pstr(path), label))
        return True
    log("ERROR", "DELETE FAILED %s   (%s)" % (pstr(path), label))
    return False


if DRY_RUN:
    log("INFO", "DRY_RUN=True -> zero mutation performed under %s" % TABLE_ROOT)
    renamed_values = [p["value"] for p in plans]
else:
    for p in plans:
        src, target = p["src"], p["target"]
        ok = True

        if p["kind"] == "rename":
            # fs.rename into an EXISTING directory would move src INSIDE it,
            # so the existence check is mandatory, not an optimisation.
            if fs.exists(target):
                log("WARN", "target appeared since the scan, falling back to MERGE: %s"
                            % pstr(target))
                p["kind"] = "merge"
            elif fs.rename(src, target):
                log("OK", "RENAME %s -> %s" % (pstr(src), pstr(target)))
                counts["rename"] += 1
                counts["files_moved"] += p["files"]
            else:
                ok = False
                counts["failed"] += 1
                log("ERROR", "RENAME FAILED %s -> %s -- source KEPT, no data lost"
                             % (pstr(src), pstr(target)))

        if p["kind"] == "merge" and ok:
            moved = 0
            for st in ls(src):
                s = st.getPath()
                name = pname(s)
                if is_protected(name):
                    if DELETE_MARKERS_ON_MERGE:
                        if fs.delete(s, True):
                            log("OK", "DELETE marker %s" % pstr(s))
                        else:
                            log("ERROR", "DELETE marker FAILED %s" % pstr(s))
                    else:
                        log("KEEP", "protected entry left in place: %s" % pstr(s))
                    continue
                dst = Path(target, name)
                if fs.exists(dst):
                    dst = Path(target, "merged_%s_%s" % (_uuid.uuid4().hex[:8], name))
                    log("WARN", "collision on %s -> renamed to %s" % (name, pname(dst)))
                if fs.rename(s, dst):
                    log("OK", "MOVE   %s -> %s" % (pstr(s), pstr(dst)))
                    moved += 1
                else:
                    ok = False
                    counts["failed"] += 1
                    log("ERROR", "MOVE FAILED %s -> %s" % (pstr(s), pstr(dst)))
            counts["merge"] += 1
            counts["files_moved"] += moved
            if ok and delete_if_empty(src, "emptied source dir"):
                counts["dirs_deleted"] += 1

        if ok:
            renamed_values.append(p["value"])
        else:
            skipped.append((pstr(src), "at least one rename failed - kept as is"))

    log("INFO", "renamed=%(rename)d merged=%(merge)d files_moved=%(files_moved)d "
                "dirs_deleted=%(dirs_deleted)d failures=%(failed)d" % counts)
    spark.catalog.refreshByPath(TABLE_ROOT)
    log("INFO", "Spark file index refreshed for %s" % TABLE_ROOT)

# ---------------------------------------------------------------------
# 9. Hive re-registration DDL
# ---------------------------------------------------------------------

section("5/6  HIVE DDL  (table=%s)" % HIVE_TABLE)

ddl_statements = []

if EMIT_HIVE_DDL:
    # Every partition must end up pointing at <root>/runId=<X>, including the
    # ones that were already named correctly on disk but whose metastore
    # LOCATION still says otherwise.
    to_register = {}
    for v in renamed_values:
        to_register[v] = True
    for full, v in already:
        loc = registered.get(v)
        if loc is None or path_only(loc) != "%s/%s=%s" % (ROOT_PATH_ONLY, TARGET_KEY, v):
            to_register[v] = True

    for value in sorted(to_register):
        loc = "%s/%s=%s" % (TABLE_ROOT.rstrip("/"), TARGET_KEY, value)
        ddl_statements.append("ALTER TABLE %s\n  DROP IF EXISTS PARTITION (%s='%s');"
                              % (HIVE_TABLE, CATALOG_KEY, value))
        ddl_statements.append("ALTER TABLE %s\n  ADD IF NOT EXISTS PARTITION (%s='%s')\n"
                              "  LOCATION '%s';"
                              % (HIVE_TABLE, CATALOG_KEY, value, loc))

    header = ("-- generated by rename_partitions_to_runId.py on %s\n"
              "-- table      : %s   (type: %s)\n"
              "-- table root : %s\n"
              "-- mode       : %s\n"
              "-- on-disk key: %s=   metastore column: %s\n"
              "-- partitions : %d re-pointed\n"
              % (datetime.datetime.now().isoformat(), HIVE_TABLE,
                 table_type or "UNKNOWN", TABLE_ROOT, MODE, TARGET_KEY, CATALOG_KEY,
                 len(to_register)))
    ddl_text = header + "\n" + "\n\n".join(ddl_statements) + ("\n" if ddl_statements else "")
    print(ddl_text)

    if DDL_OUTPUT_PATH:
        try:
            ddl_fs   = FileSystem.get(URI.create(DDL_OUTPUT_PATH), conf)
            ddl_path = Path(DDL_OUTPUT_PATH)
            parent   = ddl_path.getParent()
            if parent is not None and not ddl_fs.exists(parent):
                ddl_fs.mkdirs(parent)
            out = ddl_fs.create(ddl_path, True)
            out.write(bytearray(ddl_text.encode("utf-8")))
            out.close()
            log("OK", "DDL written to %s" % DDL_OUTPUT_PATH)
        except Exception as exc:                                # noqa: BLE001
            log("WARN", "could not write the DDL file: %s" % exc)

    if DRY_RUN:
        log("INFO", "DRY_RUN=True -> the DDL above was NOT executed")
    elif not ddl_execution_allowed:
        log("ERROR", "DDL NOT executed (table type not confirmed EXTERNAL). Replay %s "
                     "by hand." % DDL_OUTPUT_PATH)
    else:
        for stmt in ddl_statements:
            sql = "\n".join(l for l in stmt.split("\n") if not l.startswith("--")).strip()
            sql = sql.rstrip(";")
            if not sql:
                continue
            try:
                spark.sql(sql)
                log("OK", "EXEC   %s" % sql.replace("\n", " "))
            except Exception as exc:                            # noqa: BLE001
                counts["failed"] += 1
                log("ERROR", "SQL FAILED (%s): %s" % (sql.replace("\n", " "), exc))

# ---------------------------------------------------------------------
# 10. Validation + report
# ---------------------------------------------------------------------

section("6/6  VALIDATION & REPORT  (mode=%s)" % MODE)

wrong_case = []
for st in ls(root):
    n = pname(st.getPath())
    if not st.isDirectory() or is_protected(n):
        continue
    k, v = split_key(n)
    if v is not None and k.lower() == CATALOG_KEY and k != TARGET_KEY:
        wrong_case.append(pstr(st.getPath()))

planned_paths = set(pstr(p["src"]) for p in plans)
unresolved = [w for w in wrong_case if w in planned_paths and not DRY_RUN]

if not wrong_case:
    log("OK", "every partition directory now uses '%s='" % TARGET_KEY)
else:
    log("WARN", "%d director(ies) still not named '%s='" % (len(wrong_case), TARGET_KEY))
    for w in wrong_case:
        log("WARN", "  %s" % w)

if not DRY_RUN:
    try:
        spark.sql("SELECT %s, count(*) AS n FROM %s GROUP BY %s"
                  % (CATALOG_KEY, HIVE_TABLE, CATALOG_KEY)).show(100, False)
    except Exception as exc:                                    # noqa: BLE001
        log("ERROR", "count per %s failed: %s" % (CATALOG_KEY, exc))

print("")
print("table root            : %s" % TABLE_ROOT)
print("hive table            : %s  (type: %s)" % (HIVE_TABLE, table_type or "UNKNOWN"))
print("on-disk key           : %s=   (metastore column: %s)" % (TARGET_KEY, CATALOG_KEY))
print("already correct       : %d" % len(already))
print("renamed               : %d" % (len([p for p in plans if p["kind"] == "rename"])
                                      if DRY_RUN else counts["rename"]))
print("merged                : %d" % (len([p for p in plans if p["kind"] == "merge"])
                                      if DRY_RUN else counts["merge"]))
print("files moved           : %d" % counts["files_moved"])
print("failures              : %d" % counts["failed"])
print("skipped               : %d" % len(skipped))
print("stray entries         : %d" % len(strays))
print("DDL statements        : %d %s" % (len(ddl_statements),
                                         "(printed only, DRY_RUN)" if DRY_RUN else "(executed)"))

if skipped:
    print("")
    print("SKIPPED -- nothing was modified, these need a human decision")
    print("-" * 100)
    for sp, reason in skipped:
        print("  %s\n      reason: %s" % (sp, reason))

if strays:
    print("")
    print("STRAY / UNRELATED ENTRIES -- not touched")
    print("-" * 100)
    for sp, reason in strays:
        print("  %s\n      %s" % (sp, reason))

if protected_seen:
    print("")
    print("PROTECTED ENTRIES -- never moved file-by-file, never deleted")
    print("-" * 100)
    for sp, reason in protected_seen:
        print("  %s\n      %s" % (sp, reason))

print("")
if DRY_RUN:
    print(">>> DRY RUN: nothing was changed. Review the plan and the DDL, then set "
          "DRY_RUN = False and run again.")
else:
    print(">>> APPLIED. Re-run with DRY_RUN = True: it must report 0 rename "
          "(idempotency check).")
    print(">>> REMINDER: a later INSERT INTO / saveAsTable through the Hive table will "
          "create lowercase 'runid=' dirs again -- keep the writers building paths by hand.")

assert not unresolved, (
    "%d director(ies) were planned for rename but are still wrong-cased: %s"
    % (len(unresolved), unresolved))
