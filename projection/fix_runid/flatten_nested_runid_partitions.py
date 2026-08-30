# =====================================================================
#  flatten_nested_runid_partitions.py            (PySpark -- primary)
#
#  README -- HOW TO RUN
#  --------------------
#  Dataiku DSS, Python (PySpark) notebook or recipe, Spark 3.5.x, Python 3.
#  No `dataiku` / `dkuspark` import is needed: nothing here reads a DSS
#  dataset. No third-party dependency.
#
#    1. Set TABLE_ROOT / HIVE_TABLE in the CONFIG block below.
#       Run it once for dbprojection.term_structure, then once for
#       dbprojection.term_structure_detailed.
#    2. Run with DRY_RUN = True (the default). NOTHING is modified: the
#       script prints the metastore pre-flight, then the planned actions
#       (RENAME / MERGE / DELETE) and the DDL it would execute.
#    3. Read the report and the generated .sql file carefully.
#    4. Flip DRY_RUN = False and run again to apply.
#    5. Re-run once more: the script is idempotent, the second run must
#       report 0 wrapper found.
#
#  The Spark-Scala twin `flatten_nested_runid_partitions.scala` implements
#  the identical logic and produces the same report and the same .sql.
#
#  WHAT IT FIXES
#  -------------
#  Two layouts coexist under the table root:
#
#    correct : term_structure/runid=<uuid>/part-*.orc
#    broken  : term_structure/runId=<uuid>/runid=<uuid>/part-*.orc
#                             ^^^^^ uppercase I wrapper, holds no data
#
#  `PARTITIONED BY (runid string)` with numPartCols=1 / partCol.0=runid, so
#  lowercase `runid` is the canonical form and `runId=` on disk is
#  unambiguously the writer defect -- flatten toward lowercase.
#
#  The data files only live in the INNER directory, so the wrapper must be
#  FLATTENED, never deleted outright:
#     runId=<X>/runid=<X>/*  ->  runid=<X>/*      then  rmdir runId=<X>
#
#  WHAT IT RELIES ON (from SHOW CREATE TABLE)
#  ------------------------------------------
#  * EXTERNAL table -> DROP PARTITION removes the metastore entry only and
#    leaves the HDFS files intact. Re-checked at runtime: the script ABORTS
#    if the table turns out to be MANAGED_TABLE.
#  * spark.sql.partitionProvider = catalog -> Spark does NOT discover
#    partitions from the directory tree, it reads them from the metastore.
#    Explicit ALTER TABLE ... ADD PARTITION ... LOCATION is therefore
#    mandatory; MSCK REPAIR / filesystem discovery are not options.
#  * `matrix array<array<double>>` -> no read or rewrite of file contents.
#    The remediation is pure HDFS metadata movement (rename) plus metastore
#    DDL. No Spark job ever opens these ORC files.
#  * The camelCase data columns in spark.sql.sources.schema are normal
#    Spark-on-Hive behaviour. The script touches partitions ONLY, never the
#    table schema or TBLPROPERTIES.
#
#  SAFETY
#  ------
#  UAT data with no backup: every action is logged with its full path,
#  every ambiguous case is SKIPPED and reported instead of guessed, and a
#  wrapper is deleted only after it has been verified empty.
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

# Canonical (lowercase) partition key -- PARTITIONED BY (runid string).
# Any first-level directory whose key is not EXACTLY this one is a wrapper.
PARTITION_KEY = "runid"

# Where the generated DDL is written. Any Hadoop-addressable path
# (hdfs://..., file:/...). Set to None to skip the file and only print.
# NOTE: this file is written in dry-run too -- it is the review artefact.
# It lives outside TABLE_ROOT, so the table itself stays untouched in dry-run.
DDL_OUTPUT_PATH = "/Projects/STCreditRisk_UAT/tmp/generated_partition_ddl.sql"

# A merge (target partition already exists) never moves _SUCCESS / _temporary
# / .hive-staging* / dot-files: they are listed in the report and left in
# place, which keeps the inner directory non-empty and therefore keeps the
# wrapper alive. Set to True to let the script delete those markers so the
# wrapper can be removed. Data files are NEVER deleted, whatever the value.
DELETE_MARKERS_ON_MERGE = False

# Metastore pre-flight: one DESCRIBE FORMATTED per registered partition
# -> can be slow on a table with thousands of partitions. Turning it off
# also disables ORPHAN detection and the disk/metastore cross-check.
METASTORE_PREFLIGHT = True

# ---------------------------------------------------------------------
# 2. Logging helpers
# ---------------------------------------------------------------------

_LOG = []


def log(level, msg):
    line = "[%s] %-7s %s" % (datetime.datetime.now().strftime("%H:%M:%S"), level, msg)
    _LOG.append(line)
    print(line)


def section(title):
    print("")
    print("=" * 100)
    print(title)
    print("=" * 100)


MODE = "DRY-RUN" if DRY_RUN else "APPLY"

# ---------------------------------------------------------------------
# 3. Spark bootstrap
#    In a Dataiku Python notebook `spark` is usually already bound; create
#    it only when it is not.
# ---------------------------------------------------------------------

try:
    spark
except NameError:
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    sc    = SparkContext.getOrCreate()
    spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------
# 4. Hadoop FileSystem handle through the JVM gateway
#    Do NOT call fs.close(): the handle comes from the shared cache and is
#    used by the Spark session itself.
#    Every object coming back from Py4J is a Java proxy -> always wrap the
#    result of getName()/toString() in str() before comparing it to a
#    Python string.
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
    """Full path of a Path proxy, as a Python str."""
    return str(p.toString())


def pname(p):
    """Last segment of a Path proxy, as a Python str."""
    return str(p.getName())


if not fs.exists(root):
    raise RuntimeError("Table root does not exist: %s" % TABLE_ROOT)
if not fs.getFileStatus(root).isDirectory():
    raise RuntimeError("Table root is not a directory: %s" % TABLE_ROOT)


def ls(path):
    """listStatus as a plain python list, sorted by name."""
    return sorted(list(fs.listStatus(path)), key=lambda s: pname(s.getPath()))


def is_protected(name):
    """Markers / staging / hidden entries: _SUCCESS, _temporary,
    .hive-staging*, and anything starting with '.'.
    Never moved file-by-file, never deleted (unless DELETE_MARKERS_ON_MERGE),
    always reported."""
    return name.startswith("_") or name.startswith(".")


def split_key(name):
    """'runId=abc' -> ('runId', 'abc'); 'foo' -> ('foo', None)."""
    name = str(name)
    if "=" not in name:
        return name, None
    k, v = name.split("=", 1)
    return k, v


def path_only(uri):
    """Drop scheme://authority so two locations can be compared even when
    one of them is written without the nameservice."""
    uri = str(uri).rstrip("/")
    i = uri.find("://")
    if i < 0:
        return uri
    j = uri.find("/", i + 3)
    return uri[j:] if j >= 0 else "/"


ROOT_PATH_ONLY = path_only(TABLE_ROOT)


def rel_to_root(uri):
    """Path segments of `uri` relative to the table root, or None if the
    location is not under the table root at all."""
    p = path_only(uri)
    if p == ROOT_PATH_ONLY:
        return []
    if not p.startswith(ROOT_PATH_ONLY + "/"):
        return None
    return [s for s in p[len(ROOT_PATH_ONLY):].split("/") if s]


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

section("1/7  DISCOVERY  (mode=%s)  root=%s" % (MODE, TABLE_ROOT))

plans = []            # what to do, executed in section 8
skipped = []          # (path, reason) -- reported, never touched
protected_seen = []   # (path, reason)
canonical = []        # already-correct first-level partitions
canonical_values = [] # their run ids
strays = []           # anything unexpected at the root

root_entries = ls(root)

for st in root_entries:
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

    is_canonical_key = (key == PARTITION_KEY)   # case-sensitive on purpose

    children    = ls(st.getPath())
    child_dirs  = [c for c in children
                   if c.isDirectory() and not is_protected(pname(c.getPath()))]
    child_files = [c for c in children
                   if not c.isDirectory() and not is_protected(pname(c.getPath()))]

    nested_part_dirs = [c for c in child_dirs
                        if split_key(pname(c.getPath()))[0].lower() == PARTITION_KEY]

    if is_canonical_key and not child_dirs:
        # runid=<X>/part-*.orc -> already flat, nothing to do
        canonical.append(full)
        canonical_values.append(value)
        continue

    if not is_canonical_key and not child_dirs:
        # runId=<X>/part-*.orc -> data sits directly under a non-canonical key.
        # Not the documented defect (no nesting). Renaming the directory would
        # be a partition rename, so report instead of guessing.
        skipped.append((full, "non-canonical key '%s=' holding data files directly "
                              "(no nested %s= dir) - manual decision required"
                              % (key, PARTITION_KEY)))
        continue

    # --- from here on there is at least one child directory ---

    if len(child_dirs) > 1:
        skipped.append((full, "%d child directories (%s) - expected exactly one"
                              % (len(child_dirs),
                                 ", ".join(pname(c.getPath()) for c in child_dirs))))
        continue

    inner = child_dirs[0]
    inner_name = pname(inner.getPath())
    inner_key, inner_value = split_key(inner_name)

    if not nested_part_dirs:
        skipped.append((full, "single child dir '%s' is not a %s= partition dir"
                              % (inner_name, PARTITION_KEY)))
        continue

    if inner_key != PARTITION_KEY:
        skipped.append((full, "nested dir key is '%s=' not '%s=' - unknown nesting, "
                              "review manually" % (inner_key, PARTITION_KEY)))
        continue

    if child_files:
        skipped.append((full, "wrapper holds %d data file(s) AND a nested '%s' dir "
                              "- ambiguous, review manually"
                              % (len(child_files), inner_name)))
        continue

    if value.lower() != (inner_value or "").lower():
        skipped.append((full, "UUID MISMATCH outer='%s' inner='%s' - would merge two "
                              "distinct runs, never guessed" % (value, inner_value)))
        continue

    inner_children = ls(inner.getPath())
    inner_data     = [c for c in inner_children if not is_protected(pname(c.getPath()))]
    inner_prot     = [c for c in inner_children if is_protected(pname(c.getPath()))]
    inner_subdirs  = [c for c in inner_data if c.isDirectory()]

    if inner_subdirs:
        skipped.append((pstr(inner.getPath()),
                        "nested dir contains sub-directories (%s) - deeper nesting, "
                        "flatten manually or re-run after review"
                        % ", ".join(pname(c.getPath()) for c in inner_subdirs)))
        continue

    if not inner_children:
        # empty nested dir: no data at risk, drop the whole wrapper
        plans.append({
            "kind": "drop_empty",
            "wrapper": st.getPath(),
            "inner": inner.getPath(),
            "target": None,
            "value": value,
            "files": 0,
            "bytes": 0,
            "protected": [],
            "self_nested": is_canonical_key,
        })
        continue

    if not inner_data:
        skipped.append((pstr(inner.getPath()),
                        "nested dir only contains protected entries (%s) - no data to "
                        "promote, remove manually"
                        % ", ".join(pname(c.getPath()) for c in inner_children)))
        for c in inner_prot:
            protected_seen.append((pstr(c.getPath()),
                                   "marker inside a data-less nested dir"))
        continue

    target = Path(root, "%s=%s" % (PARTITION_KEY, value))
    target_exists = fs.exists(target)
    # runid=<X>/runid=<X> : the wrapper IS the canonical target, so the atomic
    # directory rename is impossible -> always the file-by-file merge path.
    self_nested = is_canonical_key
    nbytes = sum(c.getLen() for c in inner_data if not c.isDirectory())

    for c in inner_prot:
        protected_seen.append((pstr(c.getPath()), "marker inside the nested dir"))

    plans.append({
        "kind": "merge" if (target_exists or self_nested) else "rename",
        "wrapper": st.getPath(),
        "inner": inner.getPath(),
        "target": target,
        "value": value,
        "files": len(inner_data),
        "bytes": nbytes,
        "protected": [pname(c.getPath()) for c in inner_prot],
        "self_nested": self_nested,
    })

log("INFO", "first-level entries scanned      : %d" % len(root_entries))
log("INFO", "already-canonical %s= partitions : %d" % (PARTITION_KEY, len(canonical)))
log("INFO", "wrappers to flatten              : %d" % len(plans))
log("INFO", "skipped (reported, not touched)  : %d" % len(skipped))
log("INFO", "protected entries seen           : %d" % len(protected_seen))
log("INFO", "stray entries at root            : %d" % len(strays))

# ---------------------------------------------------------------------
# 6. Metastore pre-flight (read-only, BEFORE anything is touched on HDFS)
#
#    Two guards and one inventory:
#      * abort if the table is MANAGED_TABLE (DROP PARTITION would delete
#        the data instead of only the metastore entry);
#      * classify every registered partition as OK / NESTED / ORPHAN;
#      * cross-check the metastore against the directory scan.
# ---------------------------------------------------------------------

section("2/7  METASTORE PRE-FLIGHT  (table=%s)" % HIVE_TABLE)

table_type = None          # 'EXTERNAL' | 'MANAGED' | None (undetermined)
part_ok, part_nested, part_orphan, part_outside = [], [], [], []
registered_values = set()
unregistered = []          # on disk but not in the metastore
ddl_execution_allowed = True


def describe_rows(sql):
    return [(str(r[0] or ""), str(r[1] or "")) for r in spark.sql(sql).collect()]


if METASTORE_PREFLIGHT:
    # --- guard 1: EXTERNAL vs MANAGED ---------------------------------
    try:
        for col, val in describe_rows("DESCRIBE FORMATTED %s" % HIVE_TABLE):
            if col.strip().rstrip(":") in ("Table Type", "Type"):
                v = val.strip().upper()
                if "EXTERNAL" in v:
                    table_type = "EXTERNAL"
                elif "MANAGED" in v:
                    table_type = "MANAGED"
                break
    except Exception as exc:                                    # noqa: BLE001
        log("WARN", "DESCRIBE FORMATTED %s failed: %s" % (HIVE_TABLE, exc))

    if table_type == "MANAGED":
        raise RuntimeError(
            "ABORT: %s is a MANAGED_TABLE. ALTER TABLE ... DROP PARTITION would "
            "DELETE the underlying HDFS data, not just the metastore entry. This "
            "script only supports EXTERNAL tables (SHOW CREATE TABLE reported "
            "CREATE EXTERNAL TABLE). Nothing was modified." % HIVE_TABLE)
    if table_type == "EXTERNAL":
        log("OK", "table type confirmed EXTERNAL -> DROP PARTITION is metastore-only")
    else:
        ddl_execution_allowed = False
        log("ERROR", "could not confirm the table type is EXTERNAL. HDFS flattening "
                     "will still run, but NO DDL will be executed (the .sql file is "
                     "still generated for manual review).")

    # --- inventory: OK / NESTED / ORPHAN ------------------------------
    try:
        specs = [str(r[0]) for r in spark.sql("SHOW PARTITIONS %s" % HIVE_TABLE).collect()]
        log("INFO", "registered partitions: %d" % len(specs))
        for spec in specs:
            pkey, _, pval = spec.partition("=")
            registered_values.add(pval)
            try:
                rows = describe_rows("DESCRIBE FORMATTED %s PARTITION (%s='%s')"
                                     % (HIVE_TABLE, pkey, pval))
            except Exception as exc:                            # noqa: BLE001
                log("WARN", "DESCRIBE failed for %s : %s" % (spec, exc))
                continue
            loc = None
            for col, val in rows:
                if col.strip().rstrip(":") == "Location":
                    loc = val.strip()
                    break
            if not loc:
                log("WARN", "no Location reported for %s" % spec)
                continue

            try:
                exists = FileSystem.get(URI.create(loc), conf).exists(Path(loc))
            except Exception as exc:                            # noqa: BLE001
                log("WARN", "cannot stat %s : %s" % (loc, exc))
                continue

            rel = rel_to_root(loc)
            if not exists:
                part_orphan.append((spec, loc))
            elif rel is None:
                part_outside.append((spec, loc))
            elif len(rel) == 1 and rel[0] == "%s=%s" % (PARTITION_KEY, pval):
                part_ok.append((spec, loc))
            else:
                part_nested.append((spec, loc))
    except Exception as exc:                                    # noqa: BLE001
        log("WARN", "SHOW PARTITIONS failed, inventory skipped: %s" % exc)

    log("INFO", "  OK      (root/%s=<X>, exists)      : %d" % (PARTITION_KEY, len(part_ok)))
    log("INFO", "  NESTED  (points inside a wrapper)  : %d" % len(part_nested))
    log("INFO", "  ORPHAN  (location gone from HDFS)  : %d" % len(part_orphan))
    if part_outside:
        log("WARN", "  OUTSIDE (location not under root)  : %d" % len(part_outside))

    for spec, loc in part_nested:
        log("NESTED", "%s -> %s   (re-pointed after flattening)" % (spec, loc))
    for spec, loc in part_orphan:
        log("ORPHAN", "%s -> %s   (location missing on HDFS)" % (spec, loc))
    for spec, loc in part_outside:
        log("WARN", "%s -> %s   (outside the table root - NOT touched)" % (spec, loc))

    # --- cross-check disk vs metastore --------------------------------
    disk_values = set(canonical_values) | set(p["value"] for p in plans)
    for p in plans:
        # drop_empty carries no data, so "unregistered" would be misleading:
        # there is nothing to ADD PARTITION for it.
        if p["kind"] != "drop_empty" and p["value"] not in registered_values:
            unregistered.append((pstr(p["wrapper"]), p["value"], "wrapper"))
    for v, full in zip(canonical_values, canonical):
        if v not in registered_values:
            unregistered.append((full, v, "canonical dir"))

    log("INFO", "cross-check: %d run id(s) on disk, %d registered in the metastore"
                % (len(disk_values), len(registered_values)))
    if unregistered:
        log("WARN", "*** %d run id(s) exist on HDFS but are NOT registered ***"
                    % len(unregistered))
        for full, v, kind in unregistered:
            log("WARN", "  UNREGISTERED %s %s=%s -> %s  (a run wrote data that was "
                        "never registered)" % (kind, PARTITION_KEY, v, full))
else:
    ddl_execution_allowed = False
    log("WARN", "METASTORE_PREFLIGHT=False -> table-type guard and partition "
                "inventory skipped, DDL will not be executed")

# ---------------------------------------------------------------------
# 7. Plan
# ---------------------------------------------------------------------

section("3/7  PLANNED ACTIONS")

if not plans:
    log("INFO", "nothing to flatten - the layout is already canonical (idempotent no-op)")

for p in plans:
    if p["kind"] == "rename":
        log("PLAN", "RENAME %s -> %s   (%d file(s), %s)"
                    % (pstr(p["inner"]), pstr(p["target"]), p["files"], human(p["bytes"])))
        log("PLAN", "DELETE %s   (empty wrapper)" % pstr(p["wrapper"]))
    elif p["kind"] == "merge":
        log("PLAN", "MERGE  %d file(s) (%s) %s -> %s   [%s]"
                    % (p["files"], human(p["bytes"]), pstr(p["inner"]), pstr(p["target"]),
                       "self-nested" if p["self_nested"] else "target already exists"))
        log("PLAN", "DELETE %s   (emptied nested dir)" % pstr(p["inner"]))
        if not p["self_nested"]:
            log("PLAN", "DELETE %s   (empty wrapper)" % pstr(p["wrapper"]))
        if p["protected"]:
            log("PLAN", "KEEP   protected entries in %s : %s"
                        % (pstr(p["inner"]), ", ".join(p["protected"])))
    elif p["kind"] == "drop_empty":
        log("PLAN", "DELETE %s   (nested dir is empty, no data at risk)" % pstr(p["inner"]))
        if not p["self_nested"]:
            log("PLAN", "DELETE %s   (empty wrapper)" % pstr(p["wrapper"]))

# ---------------------------------------------------------------------
# 8. Execution
# ---------------------------------------------------------------------

section("4/7  EXECUTION  (mode=%s)" % MODE)

flattened = []      # run ids successfully flattened -> feed the DDL
counts = {"rename": 0, "merge": 0, "drop_empty": 0, "files_moved": 0,
          "wrappers_deleted": 0, "failed": 0}


def delete_if_empty(path, label):
    """Delete `path` only after verifying it holds nothing at all."""
    remaining = list(fs.listStatus(path))
    if remaining:
        names = ", ".join(pname(s.getPath()) for s in remaining)
        log("KEEP", "%s NOT deleted, still contains [%s] -- %s" % (label, names, pstr(path)))
        return False
    if fs.delete(path, True):
        log("OK", "DELETE %s   (%s)" % (pstr(path), label))
        return True
    log("ERROR", "DELETE FAILED %s   (%s)" % (pstr(path), label))
    return False


if DRY_RUN:
    log("INFO", "DRY_RUN=True -> zero mutation performed under %s" % TABLE_ROOT)
    log("INFO", "review the plan above, then set DRY_RUN = False and re-run")
    flattened = [p["value"] for p in plans if p["kind"] in ("rename", "merge")]
else:
    for p in plans:
        wrapper = p["wrapper"]
        inner   = p["inner"]
        target  = p["target"]
        ok      = True

        if p["kind"] == "drop_empty":
            counts["drop_empty"] += 1
            delete_if_empty(inner, "empty nested dir")
            if not p["self_nested"] and delete_if_empty(wrapper, "empty wrapper"):
                counts["wrappers_deleted"] += 1
            continue

        if p["kind"] == "rename":
            # Re-check just before mutating: the plan was built earlier.
            if fs.exists(target):
                log("WARN", "target appeared since the scan, falling back to MERGE: %s"
                            % pstr(target))
                p["kind"] = "merge"
            elif fs.rename(inner, target):
                log("OK", "RENAME %s -> %s" % (pstr(inner), pstr(target)))
                counts["rename"] += 1
                counts["files_moved"] += p["files"]
            else:
                ok = False
                counts["failed"] += 1
                log("ERROR", "RENAME FAILED %s -> %s -- wrapper KEPT, no data lost"
                             % (pstr(inner), pstr(target)))

        if p["kind"] == "merge" and ok:
            if not fs.exists(target):
                fs.mkdirs(target)
                log("OK", "MKDIR  %s" % pstr(target))
            moved = 0
            for st in ls(inner):
                src  = st.getPath()
                name = pname(src)
                if is_protected(name):
                    if DELETE_MARKERS_ON_MERGE:
                        if fs.delete(src, True):
                            log("OK", "DELETE marker %s" % pstr(src))
                        else:
                            log("ERROR", "DELETE marker FAILED %s" % pstr(src))
                    else:
                        log("KEEP", "protected entry left in place: %s" % pstr(src))
                    continue
                dst = Path(target, name)
                if fs.exists(dst):
                    dst = Path(target, "merged_%s_%s" % (_uuid.uuid4().hex[:8], name))
                    log("WARN", "collision on %s -> renamed to %s" % (name, pname(dst)))
                if fs.rename(src, dst):
                    log("OK", "MOVE   %s -> %s" % (pstr(src), pstr(dst)))
                    moved += 1
                else:
                    ok = False
                    counts["failed"] += 1
                    log("ERROR", "MOVE FAILED %s -> %s" % (pstr(src), pstr(dst)))
            counts["merge"] += 1
            counts["files_moved"] += moved
            if ok:
                delete_if_empty(inner, "emptied nested dir")

        if not ok:
            skipped.append((pstr(wrapper),
                            "at least one rename failed - wrapper kept as is, no data lost"))
            continue

        if p["self_nested"]:
            flattened.append(p["value"])
        elif delete_if_empty(wrapper, "empty wrapper"):
            counts["wrappers_deleted"] += 1
            flattened.append(p["value"])
        else:
            skipped.append((pstr(wrapper),
                            "wrapper not empty after flattening - review (protected "
                            "entries are kept unless DELETE_MARKERS_ON_MERGE=True)"))

    log("INFO", "renamed=%(rename)d merged=%(merge)d dropped_empty=%(drop_empty)d "
                "files_moved=%(files_moved)d wrappers_deleted=%(wrappers_deleted)d "
                "failures=%(failed)d" % counts)

    # Without this the next spark.read still sees the pre-move file index.
    spark.catalog.refreshByPath(TABLE_ROOT)
    log("INFO", "Spark file index refreshed for %s" % TABLE_ROOT)

# ---------------------------------------------------------------------
# 9. Hive re-registration DDL
#    partitionProvider=catalog -> explicit DDL is mandatory, MSCK REPAIR is
#    not an option.
# ---------------------------------------------------------------------

section("5/7  HIVE DDL  (table=%s)" % HIVE_TABLE)

ddl_statements = []

if EMIT_HIVE_DDL:
    unregistered_values = set(v for _, v, _ in unregistered)

    for value in sorted(set(flattened)):
        loc = "%s/%s=%s" % (TABLE_ROOT.rstrip("/"), PARTITION_KEY, value)
        tag = ("-- NOTE: this run id was NOT registered in the metastore before the fix\n"
               if value in unregistered_values else "")
        ddl_statements.append(
            "%sALTER TABLE %s\n  DROP IF EXISTS PARTITION (%s='%s');"
            % (tag, HIVE_TABLE, PARTITION_KEY, value))
        ddl_statements.append(
            "ALTER TABLE %s\n  ADD IF NOT EXISTS PARTITION (%s='%s')\n  LOCATION '%s';"
            % (HIVE_TABLE, PARTITION_KEY, value, loc))

    for spec, loc in part_orphan:
        pkey, _, pval = spec.partition("=")
        ddl_statements.append(
            "-- ORPHAN: %s no longer exists on HDFS\n"
            "ALTER TABLE %s\n  DROP IF EXISTS PARTITION (%s='%s');"
            % (loc, HIVE_TABLE, pkey, pval))

    # Canonical directories that were never registered: registering them is a
    # judgement call (they may be an aborted run), so they are emitted
    # commented out for a human to enable.
    commented = []
    for full, value, kind in unregistered:
        if kind == "canonical dir":
            commented.append(
                "-- UNREGISTERED canonical dir, review before enabling:\n"
                "-- ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s='%s') LOCATION '%s';"
                % (HIVE_TABLE, PARTITION_KEY, value, full))

    header = ("-- generated by flatten_nested_runid_partitions.py on %s\n"
              "-- table      : %s   (type: %s)\n"
              "-- table root : %s\n"
              "-- mode       : %s\n"
              "-- flattened  : %d run id(s)   orphan : %d   unregistered : %d\n"
              % (datetime.datetime.now().isoformat(), HIVE_TABLE, table_type or "UNKNOWN",
                 TABLE_ROOT, MODE, len(set(flattened)), len(part_orphan),
                 len(unregistered)))
    ddl_text = header + "\n" + "\n\n".join(ddl_statements + commented)
    ddl_text += "\n" if (ddl_statements or commented) else ""

    print(ddl_text)

    if DDL_OUTPUT_PATH:
        try:
            ddl_fs   = FileSystem.get(URI.create(DDL_OUTPUT_PATH), conf)
            ddl_path = Path(DDL_OUTPUT_PATH)
            parent   = ddl_path.getParent()
            if parent is not None and not ddl_fs.exists(parent):
                ddl_fs.mkdirs(parent)
            out = ddl_fs.create(ddl_path, True)      # True = overwrite
            out.write(bytearray(ddl_text.encode("utf-8")))
            out.close()
            log("OK", "DDL written to %s" % DDL_OUTPUT_PATH)
        except Exception as exc:                                # noqa: BLE001
            log("WARN", "could not write the DDL file (%s): %s" % (DDL_OUTPUT_PATH, exc))

    if DRY_RUN:
        log("INFO", "DRY_RUN=True -> the DDL above was NOT executed")
    elif not ddl_execution_allowed:
        log("ERROR", "DDL NOT executed: the table type could not be confirmed EXTERNAL. "
                     "Review %s and replay it by hand." % DDL_OUTPUT_PATH)
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
# 10. Validation
# ---------------------------------------------------------------------

section("6/7  VALIDATION")

remaining_wrappers = []
for st in ls(root):
    n = pname(st.getPath())
    if not st.isDirectory() or is_protected(n):
        continue
    k, v = split_key(n)
    if v is not None and k != PARTITION_KEY:
        remaining_wrappers.append(pstr(st.getPath()))

# A wrapper the script deliberately skipped (ambiguous case) is EXPECTED to
# still be there: it needs a human. A wrapper the script planned to flatten
# and that is still there is a real failure.
planned_paths = set(pstr(p["wrapper"]) for p in plans)
skipped_paths = set(sp for sp, _ in skipped)

unresolved = [w for w in remaining_wrappers if w in planned_paths and not DRY_RUN]
left_by_design = [w for w in remaining_wrappers if w not in unresolved]

if not remaining_wrappers:
    log("OK", "no non-canonical first-level partition directory left under the table root")
else:
    log("WARN", "%d first-level dir(s) still use a non-canonical key" % len(remaining_wrappers))
    for rp in left_by_design:
        why = "planned, dry-run" if DRY_RUN else (
            "deliberately skipped - see the SKIPPED table below"
            if rp in skipped_paths else "not handled - see the report below")
        log("WARN", "  %s   (%s)" % (rp, why))
    for rp in unresolved:
        log("ERROR", "  %s   (was planned for flattening and is STILL there)" % rp)

if DRY_RUN:
    log("INFO", "DRY_RUN=True -> Spark/Hive read validation skipped")
else:
    # These reads legitimately fail while any non-canonical wrapper survives:
    # report the error, never hide the final report behind a traceback.
    try:
        n = spark.read.orc(TABLE_ROOT).select(PARTITION_KEY).distinct().count()
        log("OK", "spark.read.orc(TABLE_ROOT) distinct %s = %d" % (PARTITION_KEY, n))
    except Exception as exc:                                    # noqa: BLE001
        log("ERROR", "spark.read.orc(TABLE_ROOT) failed: %s" % exc)
        if remaining_wrappers:
            log("ERROR", "-> expected while the wrappers listed above survive; "
                         "resolve them manually and re-run")
    try:
        spark.sql("SELECT %s, count(*) AS n FROM %s GROUP BY %s"
                  % (PARTITION_KEY, HIVE_TABLE, PARTITION_KEY)).show(100, False)
    except Exception as exc:                                    # noqa: BLE001
        log("ERROR", "count per %s failed: %s" % (PARTITION_KEY, exc))

# ---------------------------------------------------------------------
# 11. Report
# ---------------------------------------------------------------------

section("7/7  REPORT  (mode=%s)" % MODE)


def _count(kind):
    return len([p for p in plans if p["kind"] == kind])


print("table root            : %s" % TABLE_ROOT)
print("hive table            : %s  (type: %s)" % (HIVE_TABLE, table_type or "UNKNOWN"))
print("canonical partitions  : %d" % len(canonical))
print("wrappers found        : %d" % len(plans))
print("  by RENAME           : %d" % (_count("rename") if DRY_RUN else counts["rename"]))
print("  by MERGE            : %d" % (_count("merge") if DRY_RUN else counts["merge"]))
print("  empty nested dropped: %d" % (_count("drop_empty") if DRY_RUN else counts["drop_empty"]))
print("files moved           : %d" % counts["files_moved"])
print("wrappers deleted      : %d" % counts["wrappers_deleted"])
print("failures              : %d" % counts["failed"])
print("skipped               : %d" % len(skipped))
print("protected entries     : %d" % len(protected_seen))
print("stray root entries    : %d" % len(strays))
print("registered partitions : %d  (OK %d / NESTED %d / ORPHAN %d / OUTSIDE %d)"
      % (len(registered_values), len(part_ok), len(part_nested), len(part_orphan),
         len(part_outside)))
print("unregistered on disk  : %d" % len(unregistered))
print("DDL statements        : %d %s" % (len(ddl_statements),
                                         "(printed only, DRY_RUN)" if DRY_RUN else "(executed)"))

if skipped:
    print("")
    print("SKIPPED -- nothing was modified for these, they need a human decision")
    print("-" * 100)
    for sp, reason in skipped:
        print("  %s\n      reason: %s" % (sp, reason))

if unregistered:
    print("")
    print("UNREGISTERED ON DISK -- data exists on HDFS but no metastore partition")
    print("-" * 100)
    for full, v, kind in unregistered:
        print("  %s=%s  (%s)\n      %s" % (PARTITION_KEY, v, kind, full))

if part_nested:
    print("")
    print("NESTED METASTORE PARTITIONS -- location pointed inside a wrapper")
    print("-" * 100)
    for spec, loc in part_nested:
        print("  %s -> %s" % (spec, loc))

if part_orphan:
    print("")
    print("ORPHAN METASTORE PARTITIONS -- location missing on HDFS")
    print("-" * 100)
    for spec, loc in part_orphan:
        print("  %s -> %s" % (spec, loc))

if part_outside:
    print("")
    print("OUTSIDE METASTORE PARTITIONS -- location not under the table root")
    print("-" * 100)
    for spec, loc in part_outside:
        print("  %s -> %s" % (spec, loc))

if protected_seen:
    print("")
    print("PROTECTED ENTRIES -- never moved file-by-file, never deleted")
    print("-" * 100)
    for sp, reason in protected_seen:
        print("  %s\n      %s" % (sp, reason))

if strays:
    print("")
    print("STRAY ENTRIES AT THE TABLE ROOT -- out of scope, review manually")
    print("-" * 100)
    for sp, reason in strays:
        print("  %s\n      %s" % (sp, reason))

print("")
if DRY_RUN:
    print(">>> DRY RUN: nothing was changed. Review the plan and the DDL, then set "
          "DRY_RUN = False and run again.")
else:
    print(">>> APPLIED. Re-run with DRY_RUN = True: it must report 0 wrapper found "
          "(idempotency check).")

if left_by_design and not DRY_RUN:
    print("")
    print(">>> MANUAL ACTION REQUIRED: %d non-canonical first-level director(ies) are "
          "still present." % len(left_by_design))
    print("    They were skipped on purpose (see the SKIPPED table). Until they are "
          "resolved by hand,")
    print("    spark.read.orc('%s') still sees mixed partition depths." % TABLE_ROOT)

# Raised last, once the whole report has been printed: only for wrappers the
# script actually tried to flatten and failed to.
assert not unresolved, (
    "%d wrapper(s) were planned for flattening but are still present - see the "
    "ERROR lines above: %s" % (len(unresolved), unresolved))
