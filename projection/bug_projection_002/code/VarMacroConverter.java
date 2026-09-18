package com.bnpp.itg.fresh.str.processes;

import com.bnpp.itg.fresh.str.model.SectoredVarName;
import com.bnpp.itg.fresh.str.utils.CommonConstants;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class VarMacroConverter implements Serializable {


    private final String[]                         columns;
    private final List<String>                     sectorsTag;
    private final List<String>                     sectoredVarNamesPrefix;
    private final HashMap<String, SectoredVarName> sectoredVarNames;

    /**
     *
     * @param columns the name of the columns of a row
     * @param sectors the possible columns
     * @param varNames the list of var without the sector suffix
     * @param sectoredVarNamesPrefix list of var name that are common to all sectors
     */
    public VarMacroConverter(String[] columns, String[] sectors, String[] varNames, String sectoredVarNamesPrefix) {
        this.columns = columns;

        sectorsTag = new LinkedList<>();
        sectorsTag.addAll(Arrays.asList(sectors));
        sectorsTag.add(CommonConstants.NO_SECTOR);

        this.sectoredVarNamesPrefix = Arrays.asList(sectoredVarNamesPrefix.split(","));
        this.sectoredVarNames       = computeSectoredVarNames(varNames, sectors);
    }

    protected static HashMap<String, SectoredVarName> computeSectoredVarNames(String[] varNameList, String[] sectors) {
        HashMap<String, SectoredVarName> res = new HashMap<>();
        for (String varName : varNameList) {
            res.put(varName , new SectoredVarName(varName, CommonConstants.NO_SECTOR));
            for (String sector : sectors) {
                res.put(varName + "_" + sector, new SectoredVarName(varName, sector));
            }
        }
        return res;
    }

    /**
     * if a var is sectorializable then the column end with the sector (no suffix if no sector)
     */
    private List<Row> flattenSectorialVar(String varName, String date, String scenario, double varValue) {
        SectoredVarName sectoredVarName = sectoredVarNames.get(varName);
        if (sectoredVarName == null) return Collections.emptyList(); // the current var is no used for the current projection
        Object[] values = {date, scenario, sectoredVarName.getSector(), sectoredVarName.getVarName(), varValue};
        return Collections.singletonList(RowFactory.create(values));
    }

    /**
     * if a var is-not sectorializable then we have to duplicate it for each possible sector
     */
    private List<Row> flattenCommonVar(String varName, String date, String scenario, double varValue) {
        return sectorsTag.stream().map((String sector) -> {
            Object[] values = {date, scenario, sector, varName, varValue};
            return RowFactory.create(values);
        }).collect(Collectors.toList());
    }


    public boolean isSectorializableVar(String varName) {
        return sectoredVarNamesPrefix.stream().anyMatch(varName::startsWith);
    }

    /**
     * a row of var macro start with the date and the scenario followed by a list of macro var so each row can contain more than one var macro value
     * the goal of this method is to have one macro var by row
     */
    public Iterator<Row> flattenVarRow(Row row) {
        ArrayList<Row> rowList = new ArrayList<>();
        String date = row.getString(0);
        String scenario = row.getString(1);
        for (int i = 2; i < columns.length; i++) {
            String varName = columns[i];

            List<Row> rows;
            if (isSectorializableVar(varName)) rows = flattenSectorialVar(varName, date, scenario, row.getDouble(i));
            else rows = flattenCommonVar(varName, date, scenario, row.getDouble(i));

            rowList.addAll(rows);
        }

        return rowList.iterator();
    }
}
