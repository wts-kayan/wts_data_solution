package com.bnpp.itg.fresh.str.model;

import java.io.Serializable;

/**
 * One macro-variable observation: the flattened form of a wide macro-variable CSV row.
 *
 * <p>This is the output type of {@code VarMacroConverter.flattenVarRow} and the type encoded by
 * {@code Encoders.bean(MacroVarFlat.class)} in {@code StepGetMacroVar.importVarMacroEcoFromCsv}.
 * It replaces the {@code Encoder<Row>} built by the former {@code getTmpEncoder}, whose factory
 * changed name between Spark 3.4 and 3.5 ({@code RowEncoder.apply} → {@code Encoders.row}) and so
 * could not be compiled once for both. {@code Encoders.bean} is unchanged across both versions.
 *
 * <p>The property names here are fixed, while the first two column names of the CSV are read from
 * its header at runtime. {@code importVarMacroEcoFromCsv} bridges that with an alias select
 * immediately after the {@code flatMap} — see the {@code select(col("date").as(colDate), …)} there.
 * Renaming a property here without updating that select silently breaks the step.
 */
public class MacroVarFlat implements Serializable {

    private static final long serialVersionUID = 1L;

    private String date;
    private String scenario;
    private String sector;
    private String variable;
    private double value;

    /** Required by {@code Encoders.bean}. */
    public MacroVarFlat() {
    }

    public MacroVarFlat(String date, String scenario, String sector, String variable, double value) {
        this.date = date;
        this.scenario = scenario;
        this.sector = sector;
        this.variable = variable;
        this.value = value;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getScenario() {
        return scenario;
    }

    public void setScenario(String scenario) {
        this.scenario = scenario;
    }

    public String getSector() {
        return sector;
    }

    public void setSector(String sector) {
        this.sector = sector;
    }

    public String getVariable() {
        return variable;
    }

    public void setVariable(String variable) {
        this.variable = variable;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
