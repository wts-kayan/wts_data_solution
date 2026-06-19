package utility;

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
            return pathFromTable(catalog, table, runId);
        }

        return pathFromRawPath(sparkSession, input, runId);
    }

    /** Resolve the partition location from the catalog, matching the runId
     *  partition column case-insensitively against the table's real schema. */
    private static String pathFromTable(SessionCatalog catalog,
                                        TableIdentifier table,
                                        String runId) {

        String runIdColumn = resolveRunIdColumn(catalog, table);

        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put(runIdColumn, runId);

        return catalog.getPartition(
                        table,
                        JavaConverters.mapAsScalaMapConverter(partitionSpec)
                                .asScala()
                                .toMap(Predef.conforms())
                )
                .location()
                .getPath();
    }

    /** Find the actual partition-column name on the table whose name equals
     *  "runId" ignoring case; fall back to the canonical key if absent. */
    private static String resolveRunIdColumn(SessionCatalog catalog, TableIdentifier table) {
        CatalogTable metadata = catalog.getTableMetadata(table);
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
