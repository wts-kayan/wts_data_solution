package str_sicr3_input.utility;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;

import scala.Option;
import scala.Predef;
import scala.collection.JavaConverters;

public class InputUtils {

    private static final Pattern TABLE_PATTERN =
            Pattern.compile("^([\\w]+\\.)?([\\w]+)$");

    /** Canonical partition-column name. The on-disk / catalog casing may differ
     *  (e.g. "runId" before the submit_to_runid migration, "runid" after), so it
     *  is always resolved at runtime rather than assumed. */
    private static final String RUN_ID_KEY = "runId";

    public static String getPathFromTableOrPath(String input, String runId) {

        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("input (table or path) must not be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId must not be null or empty");
        }

        input = input.trim();
        runId = runId.trim();

        Option<SparkSession> activeSession = SparkSession.getActiveSession();
        if (activeSession.isEmpty()) {
            throw new IllegalStateException("No active SparkSession found");
        }
        SparkSession sparkSession = activeSession.get();

        Matcher m = TABLE_PATTERN.matcher(input);
        boolean tabPattern = m.matches();

        TableIdentifier table =
                tabPattern ? new TableIdentifier(m.group(2)) : null;

        SessionCatalog catalog = sparkSession.sessionState().catalog();

        if (tabPattern && catalog.tableExists(table)) {
            return pathFromTable(sparkSession, catalog, table, runId);
        }

        return pathFromRawPath(sparkSession, input, runId);
    }

    /** Resolve the partition location from the catalog, matching the runId
     *  partition column case-insensitively against the table's real schema.
     *
     *  <p>When files are written straight to HDFS (e.g. a Spark write to the
     *  partition path) without an {@code ALTER TABLE ... ADD PARTITION} /
     *  {@code MSCK REPAIR}, the metastore has no partition entry and
     *  {@code getPartition} throws {@link NoSuchPartitionException}. In that
     *  case we fall back to the conventional Hive layout under the table
     *  {@code LOCATION}, after verifying the directory actually exists. */
    private static String pathFromTable(SparkSession sparkSession,
                                        SessionCatalog catalog,
                                        TableIdentifier table,
                                        String runId) {

        CatalogTable metadata = catalog.getTableMetadata(table);
        String runIdColumn = resolveRunIdColumn(metadata);

        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put(runIdColumn, runId);

        try {
            return catalog.getPartition(
                            table,
                            JavaConverters.mapAsScalaMapConverter(partitionSpec)
                                    .asScala()
                                    .toMap(Predef.conforms())
                    )
                    .location()
                    .getPath();
        } catch (Exception e) {
            // getPartition() comes from Scala, so NoSuchPartitionException is not
            // visible as a thrown checked exception to javac; gate on instanceof
            // and rethrow anything that is not a missing-partition error.
            if (!(e instanceof NoSuchPartitionException)) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new RuntimeException(e);
            }
            return partitionPathFromTableLocation(sparkSession, metadata, runIdColumn, runId);
        }
    }

    /** Build the conventional partition path under the table LOCATION
     *  ({@code <location>/<runIdColumn>=<runId>}) for partitions that exist on
     *  the filesystem but are not registered in the metastore. Throws a clear
     *  error when the directory is genuinely absent. */
    private static String partitionPathFromTableLocation(SparkSession sparkSession,
                                                         CatalogTable metadata,
                                                         String runIdColumn,
                                                         String runId) {

        URI tableLocation = metadata.location();
        String base = tableLocation.toString();
        if (!base.endsWith("/")) {
            base = base + "/";
        }
        String partitionPath = base + runIdColumn + "=" + runId;

        try {
            Configuration hadoopConf = sparkSession.sessionState().newHadoopConf();
            Path candidate = new Path(partitionPath);
            FileSystem fs = candidate.getFileSystem(hadoopConf);
            if (!fs.exists(candidate)) {
                throw new IllegalStateException(
                        "Partition not registered in the metastore and no directory found on the "
                                + "filesystem for " + runIdColumn + "=" + runId + " at " + partitionPath);
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to verify fallback partition path on the filesystem: " + partitionPath, e);
        }

        return partitionPath;
    }

    /** Find the actual partition-column name on the table whose name equals
     *  "runId" ignoring case; fall back to the canonical key if absent. */
    private static String resolveRunIdColumn(CatalogTable metadata) {
        List<String> partitionColumns = JavaConverters
                .seqAsJavaListConverter(metadata.partitionColumnNames())
                .asJava();

        return partitionColumns.stream()
                .filter(c -> c.equalsIgnoreCase(RUN_ID_KEY))
                .findFirst()
                .orElse(RUN_ID_KEY);
    }

    /** Build a runId partition directory under a raw path, preferring whichever
     *  casing actually exists on the filesystem ("runId=" or "runid="). When
     *  neither exists yet, default to the post-migration lowercase convention. */
    private static String pathFromRawPath(SparkSession sparkSession, String input, String runId) {

        String base = input.endsWith("/") ? input : input + "/";

        try {
            Configuration hadoopConf = sparkSession.sessionState().newHadoopConf();
            FileSystem fs = new Path(base).getFileSystem(hadoopConf);

            for (String key : new String[] {"runId", "runid"}) {
                Path candidate = new Path(base + key + "=" + runId);
                if (fs.exists(candidate)) {
                    return candidate.toString();
                }
            }
        } catch (Exception e) {
            // Filesystem not reachable: fall through to the default convention.
        }

        // Default to the lowercase convention produced by submit_to_runid.sh.
        return base + "runid=" + runId;
    }
}
