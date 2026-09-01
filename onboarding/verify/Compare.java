import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Runs StepOriginal and StepRefactored over the same generated population and
 * compares every field sub-task 8 lists, with exact ==. No epsilon: if a
 * tolerance were needed the refactor would be wrong.
 *
 * This stands in for the UAT non-regression run. It cannot replace it -- it
 * uses stub models and synthetic data -- but it does answer the question the
 * ticket is really asking: is the refactoring described in sub-tasks 1 to 7
 * actually behaviour-preserving, including the awkward branches?
 *
 *   javac -d out *.java && java -cp out Compare [facilities] [seed]
 */
public final class Compare {

    // The 8 facility-level fields from sub-task 8.
    private static final String[] FACILITY_FIELDS = {
        "crr3_irb_rwa", "crr3_irb_secured_rwa", "crr3_irb_unsecured_rwa",
        "crr3_irb_capital", "crr3_irb_secured_capital", "crr3_irb_unsecured_capital",
        "crr3_irb_secured_rw_min", "crr3_irb_secured_rw_max"
    };

    // The 8 rating-level fields from sub-task 8.
    private static final String[] RATING_FIELDS = {
        "crr3_irb_rwa", "crr3_irb_secured_rwa", "crr3_irb_unsecured_rwa",
        "crr3_irb_secured_rw", "crr3_irb_secured_rw_max", "crr3_irb_unsecured_rw",
        "crr3_irb_secured_capital", "crr3_irb_unsecured_capital"
    };

    private static double[] facilityValues(Facility f) {
        return new double[] {
            f.getCrr3_irb_rwa(), f.getCrr3_irb_secured_rwa(), f.getCrr3_irb_unsecured_rwa(),
            f.getCrr3_irb_capital(), f.getCrr3_irb_secured_capital(), f.getCrr3_irb_unsecured_capital(),
            f.getCrr3_irb_secured_rw_min(), f.getCrr3_irb_secured_rw_max()
        };
    }

    private static double[] ratingValues(MeasurementsOfRating m) {
        return new double[] {
            m.getCrr3_irb_rwa(), m.getCrr3_irb_secured_rwa(), m.getCrr3_irb_unsecured_rwa(),
            m.getCrr3_irb_secured_rw(), m.getCrr3_irb_secured_rw_max(), m.getCrr3_irb_unsecured_rw(),
            m.getCrr3_irb_secured_capital(), m.getCrr3_irb_unsecured_capital()
        };
    }

    /** Java 8 has no String.repeat. This harness has to compile on the same JDK
      * as the engine it is checking, which is 8. */
    private static String rule(int n) {
        char[] c = new char[n];
        java.util.Arrays.fill(c, '=');
        return new String(c);
    }

    /** Bit-level equality. Double.compare so that -0.0 != 0.0 and NaN == NaN,
      * which `==` would get wrong in both directions. */
    private static boolean same(double a, double b) {
        return Double.compare(a, b) == 0;
    }

    /** A population that reaches every branch, not just the happy one. */
    private static Facility generate(Random rnd, int i) {
        Facility f = new Facility();

        // approach: IRBA, IRBF, null (falls through guard 1), and SA (returns early)
        int a = i % 7;
        f.setApproche_bale_iv_rwa(a == 0 ? null : a == 1 ? Crr3Approach.SA
                                 : (a % 2 == 0 ? Crr3Approach.IRBA : Crr3Approach.IRBF));

        // ratios: sometimes exactly 0.0, which is its own branch
        f.setCrr3_irba_per_unsecured_ead(i % 5 == 0 ? 0.0 : rnd.nextDouble());
        f.setCcr3_irba_secured_ead_ratio(i % 6 == 0 ? 0.0 : rnd.nextDouble());
        f.setCcr3_irba_secured_rw(rnd.nextDouble() * 2);
        f.setCrr3_irbf_per_unsecured_ead(i % 4 == 0 ? 0.0 : rnd.nextDouble());
        f.setCcr3_irbf_secured_ead_ratio(i % 8 == 0 ? 0.0 : rnd.nextDouble());
        f.setCcr3_irbf_secured_rw(rnd.nextDouble() * 2);

        int nRatings = i % 6;   // includes 0 -> empty map, which returns at guard 2
        Map<Rating, MeasurementsOfRating> byRating = f.getfMeasurement().getMeasurementsByRating();
        for (int r = 0; r < nRatings; r++) {
            MeasurementsOfRating m = new MeasurementsOfRating();
            m.setDirty(rnd.nextInt(4) == 0);
            int u = rnd.nextInt(4);
            m.setUnpaid(u == 0 ? null : new Unpaid(u != 1));   // null / not clean / clean
            m.setEadReg(rnd.nextInt(5) == 0 ? 0.0 : rnd.nextDouble() * 1_000_000);
            m.setCrr3_irb_partial_unsecured_capital(rnd.nextDouble() * 1000);
            m.setCrr3_irb_partial_secured_capital(rnd.nextDouble() * 1000);
            byRating.put(new Rating("R" + r), m);
        }
        return f;
    }

    public static void main(String[] args) {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 200_000;
        // Seed is an argument so the check can be repeated over different
        // populations. A single fixed seed only ever proves the refactoring is
        // equivalent on one sequence of draws.
        long seed = args.length > 1 ? Long.parseLong(args[1]) : 20260901L;
        Random rnd = new Random(seed);

        int facilitiesCompared = 0;
        int ratingsCompared = 0;
        int facilityMismatches = 0;
        int ratingMismatches = 0;
        List<String> firstFailures = new ArrayList<>();

        // Branch coverage counters, so a green run cannot be green by accident
        int earlyGuard1 = 0, earlyGuard2 = 0, processed = 0, dirtyRatings = 0,
            zeroEad = 0, zeroRatio = 0;

        for (int i = 0; i < n; i++) {
            Facility input = generate(rnd, i);

            boolean guard1 = input.getApproche_bale_iv_rwa() != null
                    && input.getApproche_bale_iv_rwa() != Crr3Approach.IRBA
                    && input.getApproche_bale_iv_rwa() != Crr3Approach.IRBF;
            boolean guard2 = !guard1 && input.getfMeasurement().getMeasurementsByRating().values()
                    .stream().noneMatch(m -> m.getUnpaid() != null && m.getUnpaid().isClean());
            if (guard1) earlyGuard1++;
            else if (guard2) earlyGuard2++;
            else processed++;

            for (MeasurementsOfRating m : input.getfMeasurement().getMeasurementsByRating().values()) {
                if (m.isDirty()) dirtyRatings++;
                if (m.getEadReg() == 0) zeroEad++;
            }
            if (input.getCrr3_irba_per_unsecured_ead() == 0.0
                    || input.getCcr3_irba_secured_ead_ratio() == 0.0
                    || input.getCrr3_irbf_per_unsecured_ead() == 0.0
                    || input.getCcr3_irbf_secured_ead_ratio() == 0.0) zeroRatio++;

            Facility before = StepOriginal.execute(input.copy());
            Facility after  = StepRefactored.execute(input.copy());

            facilitiesCompared++;
            double[] bf = facilityValues(before), af = facilityValues(after);
            for (int k = 0; k < bf.length; k++) {
                if (!same(bf[k], af[k])) {
                    facilityMismatches++;
                    if (firstFailures.size() < 10)
                        firstFailures.add(String.format(
                            "facility[%d].%s : original=%s refactored=%s",
                            i, FACILITY_FIELDS[k], bf[k], af[k]));
                }
            }

            List<MeasurementsOfRating> bm =
                    new ArrayList<>(before.getfMeasurement().getMeasurementsByRating().values());
            List<MeasurementsOfRating> am =
                    new ArrayList<>(after.getfMeasurement().getMeasurementsByRating().values());
            if (bm.size() != am.size()) {
                ratingMismatches++;
                firstFailures.add("facility[" + i + "] rating count " + bm.size() + " vs " + am.size());
                continue;
            }
            for (int r = 0; r < bm.size(); r++) {
                ratingsCompared++;
                double[] br = ratingValues(bm.get(r)), ar = ratingValues(am.get(r));
                for (int k = 0; k < br.length; k++) {
                    if (!same(br[k], ar[k])) {
                        ratingMismatches++;
                        if (firstFailures.size() < 10)
                            firstFailures.add(String.format(
                                "facility[%d].rating[%d].%s : original=%s refactored=%s",
                                i, r, RATING_FIELDS[k], br[k], ar[k]));
                    }
                }
            }
        }

        System.out.println(rule(78));
        System.out.println("StepCrr3IrbRwaCompute -- original vs refactored, exact comparison");
        System.out.println(rule(78));
        System.out.printf("seed                     : %d%n", seed);
        System.out.printf("facilities compared      : %,d%n", facilitiesCompared);
        System.out.printf("rating rows compared     : %,d%n", ratingsCompared);
        System.out.printf("facility fields per row  : %d%n", FACILITY_FIELDS.length);
        System.out.printf("rating fields per row    : %d%n", RATING_FIELDS.length);
        System.out.println();
        System.out.println("branch coverage of the generated population");
        System.out.printf("  returned at guard 1    : %,d%n", earlyGuard1);
        System.out.printf("  returned at guard 2    : %,d%n", earlyGuard2);
        System.out.printf("  fully processed        : %,d%n", processed);
        System.out.printf("  dirty ratings          : %,d%n", dirtyRatings);
        System.out.printf("  ratings with eadReg=0  : %,d%n", zeroEad);
        System.out.printf("  facilities w/ a 0 ratio: %,d%n", zeroRatio);
        System.out.println();
        System.out.printf("facility-level mismatches: %,d%n", facilityMismatches);
        System.out.printf("rating-level mismatches  : %,d%n", ratingMismatches);

        if (!firstFailures.isEmpty()) {
            System.out.println();
            System.out.println("first failures:");
            firstFailures.forEach(l -> System.out.println("  " + l));
        }

        System.out.println(rule(78));
        boolean ok = facilityMismatches == 0 && ratingMismatches == 0
                && processed > 0 && dirtyRatings > 0 && zeroEad > 0
                && earlyGuard1 > 0 && earlyGuard2 > 0;
        if (facilityMismatches == 0 && ratingMismatches == 0 && !ok)
            System.out.println(">>> INCONCLUSIVE: zero mismatches, but the population did not "
                    + "reach every branch. Widen the generator.");
        System.out.println(ok ? ">>> BIT-IDENTICAL on every compared field."
                              : ">>> DIFFERENCES FOUND -- the refactoring is not behaviour-preserving.");
        System.exit(ok ? 0 : 1);
    }
}
