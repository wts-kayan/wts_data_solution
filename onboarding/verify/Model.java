// Minimal stand-ins for the str-bigData model classes, enough to run
// StepCrr3IrbRwaCompute.execute() outside Spring and Spark.
//
// Only what execute() touches is modelled. Field defaults matter: several
// crr3_irb_* fields are read by the aggregation without always having been
// written in the same call, so their PRE-EXISTING value is part of the
// behaviour under test. They are seeded with sentinels, not zeros, so a
// difference between the two implementations cannot hide behind a 0.0.

import java.util.LinkedHashMap;
import java.util.Map;

enum Crr3Approach { IRBA, IRBF, SA }

/** Map key only. execute() never reads it -- which is what sub-task 1 is about. */
final class Rating {
    final String code;
    Rating(String code) { this.code = code; }
    @Override public String toString() { return code; }
}

final class Unpaid {
    private final boolean clean;
    Unpaid(boolean clean) { this.clean = clean; }
    boolean isClean() { return clean; }
}

/** Stands in for org.apache.commons.math3.util.FastMath. For double, the
  * commons-math3 versions delegate to the same IEEE-754 semantics as
  * java.lang.Math -- which is the whole basis of sub-task 4. */
final class FastMath {
    static double min(double a, double b) { return Math.min(a, b); }
    static double max(double a, double b) { return Math.max(a, b); }
}

final class MeasurementsOfRating {
    private boolean dirty;
    private Unpaid unpaid;
    private double eadReg;
    private double crr3_irb_partial_unsecured_capital;
    private double crr3_irb_partial_secured_capital;

    // Sentinels, not 0.0: these four are read by the aggregation even for
    // ratings whose branch never assigns them.
    private double crr3_irb_unsecured_rw     = -7.5;
    private double crr3_irb_secured_rw       = -3.25;
    private double crr3_irb_secured_rw_max   = -11.125;
    private double crr3_irb_rwa              = -99.0;

    private double crr3_irb_unsecured_rwa;
    private double crr3_irb_secured_rwa;
    private double crr3_irb_unsecured_capital = -1.5;
    private double crr3_irb_secured_capital   = -2.5;

    boolean isDirty() { return dirty; }
    void setDirty(boolean v) { dirty = v; }
    Unpaid getUnpaid() { return unpaid; }
    void setUnpaid(Unpaid v) { unpaid = v; }
    double getEadReg() { return eadReg; }
    void setEadReg(double v) { eadReg = v; }

    double getCrr3_irb_partial_unsecured_capital() { return crr3_irb_partial_unsecured_capital; }
    void setCrr3_irb_partial_unsecured_capital(double v) { crr3_irb_partial_unsecured_capital = v; }
    double getCrr3_irb_partial_secured_capital() { return crr3_irb_partial_secured_capital; }
    void setCrr3_irb_partial_secured_capital(double v) { crr3_irb_partial_secured_capital = v; }

    double getCrr3_irb_unsecured_rwa() { return crr3_irb_unsecured_rwa; }
    void setCrr3_irb_unsecured_rwa(double v) { crr3_irb_unsecured_rwa = v; }
    double getCrr3_irb_unsecured_rw() { return crr3_irb_unsecured_rw; }
    void setCrr3_irb_unsecured_rw(double v) { crr3_irb_unsecured_rw = v; }
    double getCrr3_irb_unsecured_capital() { return crr3_irb_unsecured_capital; }
    void setCrr3_irb_unsecured_capital(double v) { crr3_irb_unsecured_capital = v; }

    double getCrr3_irb_secured_rwa() { return crr3_irb_secured_rwa; }
    void setCrr3_irb_secured_rwa(double v) { crr3_irb_secured_rwa = v; }
    double getCrr3_irb_secured_rw() { return crr3_irb_secured_rw; }
    void setCrr3_irb_secured_rw(double v) { crr3_irb_secured_rw = v; }
    double getCrr3_irb_secured_rw_max() { return crr3_irb_secured_rw_max; }
    void setCrr3_irb_secured_rw_max(double v) { crr3_irb_secured_rw_max = v; }
    double getCrr3_irb_secured_capital() { return crr3_irb_secured_capital; }
    void setCrr3_irb_secured_capital(double v) { crr3_irb_secured_capital = v; }

    double getCrr3_irb_rwa() { return crr3_irb_rwa; }
    void setCrr3_irb_rwa(double v) { crr3_irb_rwa = v; }

    MeasurementsOfRating copy() {
        MeasurementsOfRating c = new MeasurementsOfRating();
        c.dirty = dirty; c.unpaid = unpaid; c.eadReg = eadReg;
        c.crr3_irb_partial_unsecured_capital = crr3_irb_partial_unsecured_capital;
        c.crr3_irb_partial_secured_capital = crr3_irb_partial_secured_capital;
        c.crr3_irb_unsecured_rw = crr3_irb_unsecured_rw;
        c.crr3_irb_secured_rw = crr3_irb_secured_rw;
        c.crr3_irb_secured_rw_max = crr3_irb_secured_rw_max;
        c.crr3_irb_rwa = crr3_irb_rwa;
        c.crr3_irb_unsecured_rwa = crr3_irb_unsecured_rwa;
        c.crr3_irb_secured_rwa = crr3_irb_secured_rwa;
        c.crr3_irb_unsecured_capital = crr3_irb_unsecured_capital;
        c.crr3_irb_secured_capital = crr3_irb_secured_capital;
        return c;
    }
}

final class FMeasurement {
    // LinkedHashMap on purpose: iteration order must be identical between the
    // two implementations, or the floating-point sums could differ legitimately
    // and the comparison would prove nothing.
    private final Map<Rating, MeasurementsOfRating> measurementsByRating = new LinkedHashMap<>();
    Map<Rating, MeasurementsOfRating> getMeasurementsByRating() { return measurementsByRating; }
}

final class Facility {
    private Crr3Approach approche_bale_iv_rwa;
    private final FMeasurement fMeasurement = new FMeasurement();

    private double crr3_irba_per_unsecured_ead;
    private double ccr3_irba_secured_ead_ratio;   // Ccr3, not Crr3 -- preserved as-is
    private double ccr3_irba_secured_rw;
    private double crr3_irbf_per_unsecured_ead;
    private double ccr3_irbf_secured_ead_ratio;
    private double ccr3_irbf_secured_rw;

    private double crr3_irb_rwa              = -101.0;
    private double crr3_irb_secured_rwa      = -102.0;
    private double crr3_irb_unsecured_rwa    = -103.0;
    private double crr3_irb_capital          = -104.0;
    private double crr3_irb_secured_capital  = -105.0;
    private double crr3_irb_unsecured_capital= -106.0;
    private double crr3_irb_secured_rw_min   = -107.0;
    private double crr3_irb_secured_rw_max   = -108.0;

    Crr3Approach getApproche_bale_iv_rwa() { return approche_bale_iv_rwa; }
    void setApproche_bale_iv_rwa(Crr3Approach v) { approche_bale_iv_rwa = v; }
    FMeasurement getfMeasurement() { return fMeasurement; }

    double getCrr3_irba_per_unsecured_ead() { return crr3_irba_per_unsecured_ead; }
    void setCrr3_irba_per_unsecured_ead(double v) { crr3_irba_per_unsecured_ead = v; }
    double getCcr3_irba_secured_ead_ratio() { return ccr3_irba_secured_ead_ratio; }
    void setCcr3_irba_secured_ead_ratio(double v) { ccr3_irba_secured_ead_ratio = v; }
    double getCcr3_irba_secured_rw() { return ccr3_irba_secured_rw; }
    void setCcr3_irba_secured_rw(double v) { ccr3_irba_secured_rw = v; }
    double getCrr3_irbf_per_unsecured_ead() { return crr3_irbf_per_unsecured_ead; }
    void setCrr3_irbf_per_unsecured_ead(double v) { crr3_irbf_per_unsecured_ead = v; }
    double getCcr3_irbf_secured_ead_ratio() { return ccr3_irbf_secured_ead_ratio; }
    void setCcr3_irbf_secured_ead_ratio(double v) { ccr3_irbf_secured_ead_ratio = v; }
    double getCcr3_irbf_secured_rw() { return ccr3_irbf_secured_rw; }
    void setCcr3_irbf_secured_rw(double v) { ccr3_irbf_secured_rw = v; }

    double getCrr3_irb_rwa() { return crr3_irb_rwa; }
    void setCrr3_irb_rwa(double v) { crr3_irb_rwa = v; }
    double getCrr3_irb_secured_rwa() { return crr3_irb_secured_rwa; }
    void setCrr3_irb_secured_rwa(double v) { crr3_irb_secured_rwa = v; }
    double getCrr3_irb_unsecured_rwa() { return crr3_irb_unsecured_rwa; }
    void setCrr3_irb_unsecured_rwa(double v) { crr3_irb_unsecured_rwa = v; }
    double getCrr3_irb_capital() { return crr3_irb_capital; }
    void setCrr3_irb_capital(double v) { crr3_irb_capital = v; }
    double getCrr3_irb_secured_capital() { return crr3_irb_secured_capital; }
    void setCrr3_irb_secured_capital(double v) { crr3_irb_secured_capital = v; }
    double getCrr3_irb_unsecured_capital() { return crr3_irb_unsecured_capital; }
    void setCrr3_irb_unsecured_capital(double v) { crr3_irb_unsecured_capital = v; }
    double getCrr3_irb_secured_rw_min() { return crr3_irb_secured_rw_min; }
    void setCrr3_irb_secured_rw_min(double v) { crr3_irb_secured_rw_min = v; }
    double getCrr3_irb_secured_rw_max() { return crr3_irb_secured_rw_max; }
    void setCrr3_irb_secured_rw_max(double v) { crr3_irb_secured_rw_max = v; }

    Facility copy() {
        Facility f = new Facility();
        f.approche_bale_iv_rwa = approche_bale_iv_rwa;
        f.crr3_irba_per_unsecured_ead = crr3_irba_per_unsecured_ead;
        f.ccr3_irba_secured_ead_ratio = ccr3_irba_secured_ead_ratio;
        f.ccr3_irba_secured_rw = ccr3_irba_secured_rw;
        f.crr3_irbf_per_unsecured_ead = crr3_irbf_per_unsecured_ead;
        f.ccr3_irbf_secured_ead_ratio = ccr3_irbf_secured_ead_ratio;
        f.ccr3_irbf_secured_rw = ccr3_irbf_secured_rw;
        f.crr3_irb_rwa = crr3_irb_rwa;
        f.crr3_irb_secured_rwa = crr3_irb_secured_rwa;
        f.crr3_irb_unsecured_rwa = crr3_irb_unsecured_rwa;
        f.crr3_irb_capital = crr3_irb_capital;
        f.crr3_irb_secured_capital = crr3_irb_secured_capital;
        f.crr3_irb_unsecured_capital = crr3_irb_unsecured_capital;
        f.crr3_irb_secured_rw_min = crr3_irb_secured_rw_min;
        f.crr3_irb_secured_rw_max = crr3_irb_secured_rw_max;
        for (Map.Entry<Rating, MeasurementsOfRating> e : measurementsEntries()) {
            f.fMeasurement.getMeasurementsByRating().put(e.getKey(), e.getValue().copy());
        }
        return f;
    }

    private Iterable<Map.Entry<Rating, MeasurementsOfRating>> measurementsEntries() {
        return fMeasurement.getMeasurementsByRating().entrySet();
    }
}
