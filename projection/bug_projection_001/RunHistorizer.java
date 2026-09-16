/* ⟪CUT⟫ package declaration + import block (lines 1–32) collapsed behind the `import ...` fold in Image 1 — not visible in any screenshot. Package path below reconstructed from the IDE breadcrumb, see anomaly B1. */

package com.bnpp.itg.fresh.str.runHistorizer;

/* ⟪CUT⟫ imports */

@Component
public class RunHistorizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunHistorizer.class);

    @Value(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX + ProjectionConfigurationConstants.PROPERTY_HISTORY_RUN_EXPORT_DIR + PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX)
    private String localHistorizerPathFile;

    @Value(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX + ProjectionConfigurationConstants.PROPERTY_HISTORY_RUN_ACTIVE_CONF_CONTENT_VIEW + PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX)
    private boolean viewConfContent;

    @Value(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX + ProjectionConfigurationConstants.PROPERTY_RUN_HISTORY_TABLE_NAME + PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX)
    private String historyTableName;


    @Value(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX + ProjectionConfigurationConstants.PROPERTY_LAUNCH_TYPE + PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX)
    private String launchType;

    @Value(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX + ProjectionConfigurationConstants.PROPERTY_RUN_PROJECTION_CATEGORY + PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX)
    private String runType;

    @Value(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX + ProjectionConfigurationConstants.PROPERTY_REAL_USER_NAME + PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX)
    private String realUserId;

    @Value(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX + ProjectionConfigurationConstants.PROPERTY_HISTORY_RUN_ACTIVE + PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX)
    private boolean activeRunHistory;


    public void historize(String usedConf, String usedWorkflow, Timestamp startingDate, SparkSession sc, boolean hasTechnicalFailed) throws IOException, URISyntaxException {

        if(!activeRunHistory){
            return;
        }

        LOGGER.info("STARTING HISTORIZATION ON : " + historyTableName);

        List<String> params = getConfParams(sc, usedConf);
        String jarPath = getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        FileSystem fs = FileSystem.get(sc.sparkContext().hadoopConfiguration());


        ModelRun modelRun = new ModelRun();
        modelRun.setRunId(params.get(0));
        modelRun.setUsedJar(getJarName(jarPath));
        modelRun.setUsedConf(viewConfContent ? readData(usedConf, fs) : usedConf);
        modelRun.setCreationDate(startingDate);
        modelRun.setEndDate(new Timestamp(new Date().getTime()));
        modelRun.setUsedWorfklow(usedWorkflow);
        modelRun.setUserLauncher(sc.sparkContext().sparkUser());
        modelRun.setApplicationId(sc.sparkContext().applicationId());
        modelRun.setRunType(runType);
        modelRun.setLaunchType(launchType);
        modelRun.setMotor("projection");
        modelRun.setRealUserId(realUserId == null || realUserId.isEmpty() ? sc.sparkContext().sparkUser() : realUserId);


        long durationMillis = modelRun.getEndDate().getTime() - modelRun.getCreationDate().getTime();
        String duration = convertDuration(durationMillis);
        modelRun.setDuration(duration);


        if (hasTechnicalFailed) {
            modelRun.setStatus(CommonConstants.STATUS_FAILED);
        } else {
            modelRun.setStatus(CommonConstants.STATUS_SUCCEEDED);
        }
        List<ModelRun> modelRunList = Arrays.asList(modelRun);
        Dataset<ModelRun> ds = sc.createDataset(modelRunList, Encoders.bean(ModelRun.class));

        write(ds, getTableName(historyTableName), localHistorizerPathFile, LOCAL_HISTORIZER_FILENAME + "_" + sc.sparkContext().startTime(),
                ModelRun.class, sc.sparkContext().isLocal());
    }
    public static String getJarName(String path) {
        String res=path;
        if (path != null && !path.isEmpty()) {
            int idx = path.lastIndexOf('/');
            if (idx == -1) {
                idx = path.lastIndexOf('\\');
            }
            res = path.substring(idx + 1);
        }
        return res;
    }
    private static void write(Dataset<?> ds, String tableName, String outputDir, String fileName, Class clazz, boolean isLocal) throws IOException {
        if (!isLocal) {
            ds = ds.select(JavaConversions.asScalaBuffer(getFilterColumns(clazz.getDeclaredFields())));
            ds = renameFields(ds, clazz);

            ds.write().format("orc").option("delimiter", ";").mode(SaveMode.Append).option("header", "false").option("compression", "ZLIB").saveAsTable(tableName);
        } else {
            String outputFileName = fileName;
            String outputPath = outputDir + File.separator + "tmp" + outputFileName + Math.random();
            ds = ds.select(JavaConversions.asScalaBuffer(getFilterColumns(clazz.getDeclaredFields())));
            ds = renameFields(ds, clazz);

            ds.write().format("csv").option("delimiter", ";").option("header", "true").save(outputPath);

            Configuration hadoopConf = ds.sparkSession().sparkContext().hadoopConfiguration();
            FileSystem fs = FileSystem.get(hadoopConf);
            Path facilityOutputFilePath = new Path(outputDir + File.separator + outputFileName + ".csv");
            FileUtils.copyMerge(fs, new Path(outputPath), fs, facilityOutputFilePath, true, hadoopConf, null, true);
        }
    }

    private static Dataset renameFields(Dataset ds, Class clazz) {
        for (Field col : clazz.getDeclaredFields()) {
            String newColName = col.getName();
            ds = ds.withColumnRenamed(newColName, newColName.replaceAll("([A-Z])", "_$1").toLowerCase());
        }
        return ds;
    }


    private String convertDuration(long durationMillis) {
        long hours = TimeUnit.MILLISECONDS.toHours(durationMillis);
        long remainingMillis = durationMillis - TimeUnit.HOURS.toMillis(hours);
/* ⟪CUT⟫ Image 4 ends mid-method: the remainder of convertDuration (minutes/seconds computation + return) and the class-closing brace are not visible. Next screenshot batch splices in here. */
