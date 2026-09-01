import java.util.Collection;

/**
 * The target state after sub-tasks 1 to 7 of the onboarding tickets.
 *
 * MENTOR REFERENCE. Not to be handed to Mayssa -- sub-task 7 asks her to
 * establish the fusion's legality herself, and the answer is below.
 *
 * Applied:
 *   1  entrySet() -> values(), Map and Rating imports gone
 *   2  the dead `double ead` loop removed
 *   3  !anyMatch(...) -> noneMatch(...)
 *   4  FastMath.min/max -> Math.min/max, FastMath import gone
 *   5  RATIO_CAPITAL_TO_RWA is final
 *   6  getMeasurementsByRating() hoisted, after the guards
 *   7  the four passes fused into one
 *
 * WHY THE FUSION IS LEGAL
 * Take the four passes as unsecured, secured, total, agg. For a rating N:
 *   - unsecured reads isDirty, getEadReg, getCrr3_irb_partial_unsecured_capital
 *     and its own crr3_irb_unsecured_rwa. All belong to N.
 *   - secured reads isDirty, getEadReg, getCrr3_irb_partial_secured_capital and
 *     its own crr3_irb_secured_rwa. All belong to N. None is written by the
 *     unsecured pass, so interleaving the two cannot change either.
 *   - total reads crr3_irb_secured_rwa and crr3_irb_unsecured_rwa of N, both
 *     written earlier in the same iteration once fused.
 *   - agg accumulates four fields of N, and no later iteration writes to N.
 * No pass reads a value produced for a different rating, so one pass in the
 * same iteration order computes the same values. The accumulation order is
 * unchanged, which matters: reordering double additions would be a legitimate
 * source of difference and the non-regression run would catch it.
 *
 * THE TRAP
 * The `continue` in the unsecured and secured passes skips only THAT pass for a
 * dirty rating. The total and agg passes iterate every rating, dirty included.
 * A fused loop that keeps a single `continue` for dirty ratings would skip
 * their total and their contribution to the aggregation, which the original
 * does not do. The dirty branches are therefore if/else here, not continue.
 * That is what acceptance test 4 of sub-task 7 is checking.
 *
 * Also preserved deliberately:
 *   - crr3_irb_unsecured_capital / crr3_irb_secured_capital are NOT set for a
 *     dirty rating, because the original's `continue` jumps over the setter;
 *   - crr3_irb_unsecured_rw is set only on the non-zero branch, and
 *     crr3_irb_secured_rw / _rw_max likewise, so the aggregation keeps reading
 *     whatever those fields already held;
 *   - rw_secured_min / rw_secured_max keep their `+=`, wrong as it looks.
 */
final class StepRefactored {

    public static final double RATIO_CAPITAL_TO_RWA = 12.5;

    static Facility execute(Facility facility) {
        //if(!useCrr3) return facility;

        if (facility.getApproche_bale_iv_rwa() != null && facility.getApproche_bale_iv_rwa() != Crr3Approach.IRBA
                && facility.getApproche_bale_iv_rwa() != Crr3Approach.IRBF) {
            return facility;
        }
        if (facility.getfMeasurement().getMeasurementsByRating().values().stream()
                .noneMatch(m -> m.getUnpaid() != null && m.getUnpaid().isClean())) {
            return facility;
        }

        // Hoisted AFTER the guards: above them it would run on the early-return
        // path too. Harmless here, but the habit is the point.
        Collection<MeasurementsOfRating> measurements =
                facility.getfMeasurement().getMeasurementsByRating().values();

        double unsecured_ead_ratio;
        double secured_ead_ratio;
        double secured_rw;
        if (facility.getApproche_bale_iv_rwa() == Crr3Approach.IRBA) {
            unsecured_ead_ratio = facility.getCrr3_irba_per_unsecured_ead();
            secured_ead_ratio = facility.getCcr3_irba_secured_ead_ratio();
            secured_rw = facility.getCcr3_irba_secured_rw();
        } else {
            unsecured_ead_ratio = facility.getCrr3_irbf_per_unsecured_ead();
            secured_ead_ratio = facility.getCcr3_irbf_secured_ead_ratio();
            secured_rw = facility.getCcr3_irbf_secured_rw();
        }

        double rwa_unsecured = 0;
        double rwa_secured = 0;
        double rw_secured_min = 0;
        double rw_secured_max = 0;

        for (MeasurementsOfRating measurementsOfRating : measurements) {
            boolean dirty = measurementsOfRating.isDirty();

            /* unsecured */
            if (dirty) {
                measurementsOfRating.setCrr3_irb_unsecured_rwa(0.0);
            } else {
                if (unsecured_ead_ratio == 0.0 || measurementsOfRating.getEadReg() == 0) {
                    measurementsOfRating.setCrr3_irb_unsecured_rwa(0.0);
                } else {
                    double rwa_unsecured_rating =
                            measurementsOfRating.getCrr3_irb_partial_unsecured_capital() * RATIO_CAPITAL_TO_RWA;
                    measurementsOfRating.setCrr3_irb_unsecured_rwa(rwa_unsecured_rating);

                    double rw_unsecured =
                            rwa_unsecured_rating / (measurementsOfRating.getEadReg() * unsecured_ead_ratio);
                    measurementsOfRating.setCrr3_irb_unsecured_rw(rw_unsecured);
                }
                measurementsOfRating.setCrr3_irb_unsecured_capital(
                        measurementsOfRating.getCrr3_irb_unsecured_rwa() / RATIO_CAPITAL_TO_RWA);
            }

            /* secured */
            if (dirty) {
                measurementsOfRating.setCrr3_irb_secured_rwa(0.0);
            } else {
                if (secured_ead_ratio == 0.0 || measurementsOfRating.getEadReg() == 0) {
                    measurementsOfRating.setCrr3_irb_secured_rwa(0.0);
                } else {
                    double rwa_tmp_secured =
                            measurementsOfRating.getCrr3_irb_partial_secured_capital() * RATIO_CAPITAL_TO_RWA;

                    double rw_tmp =
                            rwa_tmp_secured / (measurementsOfRating.getEadReg() * secured_ead_ratio);

                    double rw = Math.min(secured_rw, rw_tmp);
                    double rwMax = Math.max(secured_rw, rw_tmp);
                    measurementsOfRating.setCrr3_irb_secured_rw(rw);
                    measurementsOfRating.setCrr3_irb_secured_rw_max(rwMax);

                    double rwa_secured_rating = measurementsOfRating.getEadReg() * rw * secured_ead_ratio;

                    measurementsOfRating.setCrr3_irb_secured_rwa(rwa_secured_rating);
                }
                measurementsOfRating.setCrr3_irb_secured_capital(
                        measurementsOfRating.getCrr3_irb_secured_rwa() / RATIO_CAPITAL_TO_RWA);
            }

            /* total -- runs for dirty ratings too, as in the original */
            measurementsOfRating.setCrr3_irb_rwa(
                    measurementsOfRating.getCrr3_irb_secured_rwa()
                            + measurementsOfRating.getCrr3_irb_unsecured_rwa());

            /* agg -- runs for dirty ratings too, as in the original */
            rwa_unsecured += measurementsOfRating.getCrr3_irb_unsecured_rwa();
            rwa_secured += measurementsOfRating.getCrr3_irb_secured_rwa();
            rw_secured_min += measurementsOfRating.getCrr3_irb_secured_rw();
            rw_secured_max += measurementsOfRating.getCrr3_irb_secured_rw_max();
        }

        facility.setCrr3_irb_secured_rwa(rwa_secured);
        facility.setCrr3_irb_unsecured_rwa(rwa_unsecured);
        facility.setCrr3_irb_rwa(rwa_secured + rwa_unsecured);
        facility.setCrr3_irb_secured_rw_min(rw_secured_min);
        facility.setCrr3_irb_secured_rw_max(rw_secured_max);

        facility.setCrr3_irb_secured_capital(rwa_secured / RATIO_CAPITAL_TO_RWA);
        facility.setCrr3_irb_unsecured_capital(rwa_unsecured / RATIO_CAPITAL_TO_RWA);
        facility.setCrr3_irb_capital(
                facility.getCrr3_irb_unsecured_capital() + facility.getCrr3_irb_secured_capital());

        return facility;
    }
}
