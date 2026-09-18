package com.bnpp.itg.fresh.str.steps.lgdForward;

/* ⟪CUT⟫ imports not visible in screenshot */

/* Sticky header (truncated at right screen edge — see C2):
public class LgdFwdStepGetMacroVar extends AbstractStep<ModelCureRateWithTerms, ModelCureRateResult, ProjectionChangeData> implements ProjectionProcess<ModelCureRateWit⟪CUT⟫
*/

/* ⟪CUT⟫ lines 1-152 */

/* Sticky header of the enclosing method whose body closes at line 155:
    protected static HashMap<String, SectoredVarName> computeSectoredVarNames(String[] varNameList, String[] sectors) {
*/
/* ⟪CUT⟫ body of computeSectoredVarNames up to line 152 */
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
}
