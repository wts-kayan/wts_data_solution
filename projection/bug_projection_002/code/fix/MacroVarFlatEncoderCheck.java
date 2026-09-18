package com.bnpp.itg.fresh.str.model;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

/** Bean-encoder version of importVarMacroEcoFromCsv, incl. the sector-duplication rule. */
public class MacroVarFlatEncoderCheck {

    static final String NO_SECTOR = "NO_SECTOR";

    /** stand-in for VarMacroConverter, emitting beans instead of Rows */
    static class Conv implements java.io.Serializable {
        private final String[] columns;
        private final List<String> sectorsTag;
        private final List<String> sectoredVarNamesPrefix;
        private final HashMap<String, String[]> sectoredVarNames; // varName -> {varName, sector}

        Conv(String[] columns, String[] sectors, String[] varNames, String prefixes) {
            this.columns = columns;
            this.sectorsTag = new LinkedList<>(Arrays.asList(sectors));
            this.sectorsTag.add(NO_SECTOR);
            this.sectoredVarNamesPrefix = Arrays.asList(prefixes.split(","));
            this.sectoredVarNames = new HashMap<>();
            for (String v : varNames) {
                sectoredVarNames.put(v, new String[]{v, NO_SECTOR});
                for (String s : sectors) sectoredVarNames.put(v + "_" + s, new String[]{v, s});
            }
        }

        boolean isSectorializableVar(String v) { return sectoredVarNamesPrefix.stream().anyMatch(v::startsWith); }

        Iterator<MacroVarFlat> flattenVarRow(Row row) {
            List<MacroVarFlat> out = new ArrayList<>();
            String date = row.getString(0), scenario = row.getString(1);
            for (int i = 2; i < columns.length; i++) {
                String varName = columns[i];
                double v = row.getDouble(i);
                if (isSectorializableVar(varName)) {
                    String[] sv = sectoredVarNames.get(varName);
                    if (sv != null) out.add(new MacroVarFlat(date, scenario, sv[1], sv[0], v));
                } else {
                    out.addAll(sectorsTag.stream()
                            .map(s -> new MacroVarFlat(date, scenario, s, varName, v))
                            .collect(Collectors.toList()));
                }
            }
            return out.iterator();
        }
    }

    static String nul(StructType s) {
        StringBuilder b = new StringBuilder();
        for (org.apache.spark.sql.types.StructField f : s.fields())
            b.append(f.name()).append(f.nullable() ? ":null" : ":NOTNULL").append(" ");
        return b.toString();
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("bean-check").master("local[1]")
                .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        StructType in = new StructType()
                .add("Date", DataTypes.StringType, false).add("scenario", DataTypes.StringType, false)
                .add("GDP_FR", DataTypes.DoubleType, false).add("INV_FR", DataTypes.DoubleType, false);
        Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(
                RowFactory.create("2026-01", "Central", 1.5, 2.5),
                RowFactory.create("2026-02", "Central", 1.6, 2.6)), in);

        String[] colsTmp = dataset.columns();
        String colDate = colsTmp[0], colScenario = colsTmp[1], colSector = "sector";

        Conv flattener = new Conv(colsTmp, new String[]{"FR"}, new String[]{"GDP", "INV"}, "INV_,GDP_");

        Dataset<MacroVarFlat> flat = dataset.flatMap(flattener::flattenVarRow, Encoders.bean(MacroVarFlat.class));
        System.out.println("BEAN " + spark.version() + " | bean-encoder column order = " + Arrays.toString(flat.columns()));

        // restore the runtime column names the rest of the method expects
        Dataset<Row> datasetVMEI = flat.select(
                col("date").as(colDate),
                col("scenario").as(colScenario),
                col("sector").as(colSector),
                col("variable").as("Variable"),
                col("value").as("Value"));

        System.out.println("after select      = " + Arrays.toString(datasetVMEI.columns()));
        String colVarName = datasetVMEI.columns()[3], colValue = datasetVMEI.columns()[4];

        datasetVMEI = datasetVMEI.groupBy(colScenario, colVarName, colSector)
                .agg(collect_list(colDate), collect_list(colValue))
                .withColumnRenamed("collect_list(" + colDate + ")", colDate)
                .withColumnRenamed("collect_list(" + colValue + ")", colValue)
                .orderBy(colVarName, colSector);

        System.out.println("final schema      = " + datasetVMEI.schema().catalogString());
        System.out.println("final nullability = " + nul(datasetVMEI.schema()));
        for (Row r : datasetVMEI.collectAsList()) System.out.println("  " + r);
        spark.stop();
    }
}
