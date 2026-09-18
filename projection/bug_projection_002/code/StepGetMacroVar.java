package com.bnpp.itg.fresh.str.steps.defaultRates;

/* ⟪CUT⟫ import block (source lines 3-31) was folded in the IDE — not visible in any screenshot */

@Component(ProjectionStepConstants.GET_MACRO_VAR)
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StepGetMacroVar extends AbstractStep<ModelDefaultRateWithTerms, ModelDefaultRateResult, ProjectionChangeData>
        implements ProjectionProcess<ModelDefaultRateWithTerms, ModelDefaultRateResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StepGetMacroVar.class);


    @Autowired
    private SparkSession sc;

    @Value(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX + ProjectionConfigurationConstants.PROPERTY_VARIABLE_MACRO_ECO + PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX)
    private String varMacroFile;

    @Value(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX+ProjectionConfigurationConstants.PROPERTY_SCENARIOS+PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX)
    private String scenario;

    private static final String CSV_SEPARATOR = ";";

    @Autowired private RunInformation runInformation;

    private static String[] getVarNameList(Dataset<ModelDefaultRateWithTerms> data) {
        return (String[]) data.select(data.col("termName"))
                .distinct().filter((Row r) -> r.get(0) != null)
                .as(Encoders.STRING())
                .collect();
    }

    @Override
    public Dataset<ModelDefaultRateResult> launch(Dataset<ModelDefaultRateWithTerms> dataset) {

        String[] varName = getVarNameList(dataset);
        Dataset<MacroVar> var = importVarMacroEco(varMacroFile, varName, SectorLoader.get(), "INV_,GDP_");
        if (runInformation.runForStressTest() || runInformation.runForIfrs9()) {
            if( scenario!= null&&!scenario.equals("")){
                String [] scenario_tab = scenario.split(";");
                String condition = "";
                for (String scenarion : scenario_tab){
                    condition =condition
                            + "'"+scenarion.toLowerCase()+"','"+scenarion.toUpperCase()+ "','"+scenarion.substring(0,1).toUpperCase()+scenarion.substring(1).toLowerCase()+"',";
                }
                condition = condition.substring(0,condition.length()-1);
                String filterConditions = "scenario in ("+condition+")";
                var=var.filter(filterConditions);
            }
            else {
                String filterConditions = "scenario in ('Central', 'central','CENTRAL', 'Adverse', 'adverse', 'ADVERSE')";
                var=var.filter(filterConditions);
            }
        }

        Dataset<Row> modelsWithScenarios = dataset.crossJoin(broadcast(var.select(col("scenario").distinct()))).withColumnRenamed("scenario", "scenarios");
        return modelsWithScenarios.join(var,
                col("Variable").eqNullSafe(dataset.col("termName"))
                        .and(modelsWithScenarios.col("scenarios").eqNullSafe(var.col("scenario")))
                        .and(modelsWithScenarios.col("sector").eqNullSafe(var.col("sector"))
                ), "left_outer"
        ).groupBy(col("scenarios"),
                col("modelName"),
                col("typeOfModel"),
                col("idealisedMatrix"),
                col("rhoInitial"),
                col("rhoDiffusion"),
                //col("correctionFactor"),
                col("boxCoxVariableCode"),
                col("zGamma"),
                col("zDelta"),
                col("zMeanM"),
                col("zSigma"),
                //col("shift"),
                //col("shiftPonderation"),
                col("formulaCode"),
                col("formula"),
                col("inputData"),
                col("zDichotomy"),
                col("zPonderationMethod"),
                modelsWithScenarios.col("sector"),
                col("exceptionMessage")
        )
                .agg(
                        collect_list(when(col("termName").isNull(), lit("").cast(StringType)).otherwise(col("termName"))).as("termName"),
                        collect_list(when(col("Date").isNull(), lit(new String[]{""}).cast(createArrayType(StringType))).otherwise(col("Date"))).as("Date"),
                        collect_list(when(col("value").isNull(), lit(new double[]{0.0}).cast(createArrayType(DoubleType))).otherwise(col("value"))).as("value")
                )
                .withColumn("projectedDRDates", lit(null).cast(createArrayType(StringType)))
                .withColumn("projectedZDates", lit(null).cast(createArrayType(StringType)))
                .withColumn("projectedDR", lit(null).cast(createArrayType(DoubleType)))
                .withColumn("projectedZ", lit(null).cast(createArrayType(DoubleType)))
                .withColumn("matrices", lit(null).cast(createArrayType(createArrayType(createArrayType(DoubleType)))))
                .withColumn("notationCode", lit(null).cast(StringType))
                .withColumn("rho", lit(null).cast(createArrayType(DoubleType)))
                .withColumn("rhoP", lit(null).cast(createArrayType(DoubleType)))
                .withColumn("ratingFrontier", lit(null).cast(createArrayType(StringType)))
                .withColumn("repartition", lit(null).cast(createArrayType(DoubleType)))
                .withColumn("migrationMatrixName", lit(null).cast(StringType))
                .withColumn("skipMatrixMigrationSteps", lit(0).cast(IntegerType))
                .withColumn("projectedUnshiftedZ", lit(null).cast(createArrayType(DoubleType)))
                .as(Encoders.bean(ModelDefaultRateResult.class));

    }


    private Dataset<MacroVar> importVarMacroEco(String filepath, String[] varNames, String[] sectors, String commonVarNames) {
        if (filepath.endsWith(".csv")) return importVarMacroEcoFromCsv(filepath, varNames, sectors, commonVarNames);
        else return importVarMacroEcoFromOrc(filepath);
    }


    private Dataset<MacroVar> importVarMacroEcoFromOrc(String filepath) {
        return sc
                .read().format("orc")
                .load(varMacroFile)
                .withColumn("sector", lit(NO_SECTOR))
                .as(Encoders.bean(MacroVar.class))
                ;
    }


    protected static HashMap<String, SectoredVarName> computeSectoredVarNames(String[] varNameList, String[] sectors) {
        HashMap<String, SectoredVarName> res = new HashMap<>();
        for (String varName : varNameList) {
            res.put(varName , new SectoredVarName(varName, NO_SECTOR));
            for (String sector : sectors) {
                res.put(varName + "_" + sector, new SectoredVarName(varName, sector));
            }
        }
        return res;
    }


    private Dataset<MacroVar> importVarMacroEcoFromCsv(String filepath, String[] varNameList, String[] sectors, String commonVarNames) {
        Dataset<Row> dataset = sc.read().format("csv").option("header", "true").option("delimiter", CSV_SEPARATOR).load(filepath);

        String[] colsTmp = dataset.columns();

        String colDate = colsTmp[0];
        String colScenario = colsTmp[1];
        String colSector = "sector";

        dataset = castDoubleColumn(dataset, colsTmp, colDate, colScenario);


        VarMacroConverter flattener = new VarMacroConverter(colsTmp, sectors, varNameList, commonVarNames);

        Dataset<Row> datasetVMEI = dataset.flatMap(flattener::flattenVarRow, getTmpEncoder(colDate, colScenario, colSector));

        String colVarName = datasetVMEI.columns()[3];
        String colValue = datasetVMEI.columns()[4];

        datasetVMEI = datasetVMEI.groupBy(colScenario, colVarName, colSector)
                .agg(collect_list(colDate), collect_list(colValue))
                .withColumnRenamed("collect_list(" + colDate + ")", colDate)
                .withColumnRenamed("collect_list(" + colValue + ")", colValue);

        return datasetVMEI.as(Encoders.bean(MacroVar.class));
    }

    private Dataset<Row> castDoubleColumn(Dataset<Row> dataset, String[] colsTmp, String colDate, String colScenario) {
        String[] castColumns = new String[colsTmp.length];
        castColumns[0] = colDate;
        castColumns[1] = colScenario;
        for (int i = 2; i < colsTmp.length; i++) {
            castColumns[i] = "cast(" + colsTmp[i] + " as double) " + colsTmp[i];
        }

        dataset = dataset.selectExpr(castColumns);
        return dataset;
    }

    private Encoder<Row> getTmpEncoder(String colDate, String colScenario, String colSector) {
        StructType structType = new StructType();
        structType = structType.add(colDate, DataTypes.StringType, false);
        structType = structType.add(colScenario, DataTypes.StringType, false);
        structType = structType.add(colSector, DataTypes.StringType, false);
        structType = structType.add("Variable", DataTypes.StringType, false);
        structType = structType.add("Value", DataTypes.DoubleType, false);

        return Encoders.row(structType);
    }

    public SparkSession getSc() { return sc; }

    public void setSc(SparkSession sc) { this.sc = sc; }

    public String getVarMacroFile() { return varMacroFile; }

    public void setVarMacroFile(String varMacroFile) { this.varMacroFile = varMacroFile; }

}
