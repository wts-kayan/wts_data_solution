/*
 * SNIPPET, not a compilable file: the changed members of
 * com.bnpp.itg.fresh.str.steps.lgdForward.LgdFwdStepGetMacroVar.
 *
 * This class carries a character-identical copy of
 * StepGetMacroVar.importVarMacroEcoFromCsv (verified with diff against the deployed
 * source), including its own private getTmpEncoder. It therefore takes the identical
 * patch — see StepGetMacroVar_importVarMacroEcoFromCsv.java, and SOLUTION.md §2.5 for
 * why this is mandatory rather than optional.
 *
 *  - getTmpEncoder(...) is DELETED
 *  - importVarMacroEcoFromCsv(...) is replaced by the version below
 *  - the two column-name constants are added to the class
 *
 * Added imports:
 *      import com.bnpp.itg.fresh.str.model.MacroVarFlat;
 *      import org.apache.spark.sql.Encoders;               // already present
 *      import static org.apache.spark.sql.functions.col;   // already present
 *
 * Removed imports: whatever was there only for getTmpEncoder
 *      (org.apache.spark.sql.Encoder, org.apache.spark.sql.types.DataTypes,
 *       org.apache.spark.sql.types.StructType) — if unused elsewhere in the class.
 *
 * NOTE: getTmpEncoder in this class sits in the region that was not visible in the
 * screenshots (source lines 1-152). The patch below assumes it is the same five-field
 * builder as in StepGetMacroVar. Confirm before applying: if this class builds a
 * different schema, MacroVarFlat does not fit it and this call site needs its own bean.
 */

    /**
     * Column names produced by the flatten step. They were previously read positionally out of
     * datasetVMEI.columns()[3] / [4], which only worked because the Row encoder fixed the column
     * order. Encoders.bean orders properties alphabetically ("value" before "variable"), so the
     * names are now stated explicitly and the select below fixes the order.
     */
    private static final String COL_VARIABLE = "Variable";
    private static final String COL_VALUE    = "Value";

    private Dataset<MacroVar> importVarMacroEcoFromCsv(String filepath, String[] varNameList, String[] sectors, String commonVarNames) {
        Dataset<Row> dataset = sc.read().format("csv").option("header", "true").option("delimiter", CSV_SEPARATOR).load(filepath);

        String[] colsTmp = dataset.columns();

        String colDate = colsTmp[0];
        String colScenario = colsTmp[1];
        String colSector = "sector";

        dataset = castDoubleColumn(dataset, colsTmp, colDate, colScenario);


        VarMacroConverter flattener = new VarMacroConverter(colsTmp, sectors, varNameList, commonVarNames);

        // Encoders.bean is identical on Spark 3.3 and 3.5; Encoders.row / RowEncoder.apply are not.
        // The bean's property names are fixed, so the select restores the runtime column names
        // (colDate and colScenario come from the CSV header) and the column order the rest of this
        // method expects.
        Dataset<Row> datasetVMEI = dataset
                .flatMap(flattener::flattenVarRow, Encoders.bean(MacroVarFlat.class))
                .select(col("date").as(colDate),
                        col("scenario").as(colScenario),
                        col("sector").as(colSector),
                        col("variable").as(COL_VARIABLE),
                        col("value").as(COL_VALUE));

        String colVarName = COL_VARIABLE;
        String colValue = COL_VALUE;

        datasetVMEI = datasetVMEI.groupBy(colScenario, colVarName, colSector)
                .agg(collect_list(colDate), collect_list(colValue))
                .withColumnRenamed("collect_list(" + colDate + ")", colDate)
                .withColumnRenamed("collect_list(" + colValue + ")", colValue);

        return datasetVMEI.as(Encoders.bean(MacroVar.class));
    }
