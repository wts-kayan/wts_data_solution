# Ticket — Refactor `StepCrr3IrbRwaCompute` with non-regression proof

Derived from `ONBOARDING_Mayssa_StepCrr3IrbRwaCompute.md`, written in the
`TEMPLATE_jira_ticket_STE.md` format (itself taken from `AERL_STE-844`).

| Field | Value |
|---|---|
| Board | Kanban Moteurs / Test Engines |
| Project | `str_simulator_engine` / `str-bigData` |
| Branch | `develop` |
| Assignee | Mayssa |
| Type | Technical / refactoring |
| Estimate | 2–3 days including validation |
| Target class | `str-bigData/src/main/java/com/bnpparibas/sit/credit/risk/steps/crr3/irb/StepCrr3IrbRwaCompute.java` |

> **The deliverable is the non-regression proof, not the refactored code.** On a
> regulatory engine nothing is refactored without demonstrating bit-identical
> output. A "harmless improvement" that moves a figure by 1e-12 is a production
> incident, not a rounding detail.

---

## Copy/paste into Jira

```
**Context:** The StepCrr3IrbRwaCompute class computes CRR3/Basel IV IRB RWA per facility, iterating the rating map four separate times and carrying a dead EAD computation. The behaviour is correct and must not change. We need to:

1. Apply five mechanical clean-ups that provably preserve behaviour (Tier 1)
2. Hoist the repeated {{facility.getfMeasurement().getMeasurementsByRating()}} lookup, resolved 5 times in one method
3. Fuse the four passes over the rating map into one, but only after the legality of the fusion has been argued in writing
4. Prove bit-identical output on a reference UAT scenario

**Business Rules:**

* Tier 1 — behaviour-preserving clean-ups, one commit each:
   * Replace {{entrySet()}} with {{values()}} in the four loops where the {{Rating}} key is never read, then drop the now-unused {{java.util.Map}} and {{...core.model.Rating}} imports
   * Delete the dead {{double ead}} computation, after confirming with Find Usages that nothing reads it
   * Replace {{!....anyMatch(...)}} with {{noneMatch(...)}} in guard 2
   * Replace {{FastMath.min}}/{{FastMath.max}} with {{Math.min}}/{{Math.max}}, and drop the {{org.apache.commons.math3.util.FastMath}} import if it becomes unused
   * Make {{RATIO_CAPITAL_TO_RWA}} {{final}}, after checking all 6 usages are reads
* Tier 2 — hoist the map lookup into a single local
* Tier 2 — fuse the unsecured, secured, total and agg loops into one pass, accumulating the facility-level values inline
* The fusion is conditional on a written argument that no rating's computation reads a value produced for a different rating. If that argument cannot be made, the fusion is not done and the ticket still closes
* Output must be bit-identical. Not "within tolerance" — identical
* Do NOT replace {{/ RATIO_CAPITAL_TO_RWA}} with {{* 0.08}}. 1/12.5 is exactly representable in binary floating point, 0.08 is not; the results differ in the last bits
* Do NOT remove {{useCrr3}} or the commented-out {{//if(!useCrr3) return facility;}}. That is a disabled feature flag injected from {{ConfigurationConstants.PROPERTY_USE_CRR3_OPTIONS}}, not dead code
* Do NOT correct the {{Ccr3}}/{{Crr3}} getter misspelling ({{getCcr3_irba_secured_ead_ratio()}}, {{getCcr3_irba_secured_rw()}} and the IRBF equivalents). Renaming reaches the model class, its Spark encoders and possibly persisted schemas
* Do NOT change the {{+=}} on {{rw_secured_min}}/{{rw_secured_max}}. It is almost certainly wrong, and it is a functional change with regulatory impact — separate ticket, separate impact analysis, product owner sign-off

**New Reference:**

{code:java}
// Tier 1 result — guard 2, and the shape of every loop after entrySet() -> values()

if (facility.getfMeasurement().getMeasurementsByRating().values().stream()
        .noneMatch(m -> m.getUnpaid() != null && m.getUnpaid().isClean())) {
    return facility;
}

// (the dead `double ead` loop is removed here)

for (MeasurementsOfRating measurementsOfRating
        : facility.getfMeasurement().getMeasurementsByRating().values()) {
    ...
}

double rw    = Math.min(secured_rw, rw_tmp);
double rwMax = Math.max(secured_rw, rw_tmp);

public static final double RATIO_CAPITAL_TO_RWA = 12.5;
{code}

{code:java}
// Tier 2 target SHAPE. The body is deliberately not spelled out: writing it is
// the task, and it is only legal once the argument in Business Rules is made.

Collection<MeasurementsOfRating> measurements =
        facility.getfMeasurement().getMeasurementsByRating().values();

double rwa_unsecured  = 0;
double rwa_secured    = 0;
double rw_secured_min = 0;
double rw_secured_max = 0;

for (MeasurementsOfRating measurementsOfRating : measurements) {
    // unsecured, then secured, then total, for THIS rating only
    // then accumulate the four facility-level values
}

// facility-level assignments, unchanged, after the loop
{code}

**Code Snippet Scala**

{code:scala}
// Ordered edits to StepCrr3IrbRwaCompute.java -- one commit per line
// T1.1  entrySet() -> values() x4, drop Map and Rating imports
// T1.2  delete the dead `double ead` loop
// T1.3  !anyMatch(...) -> noneMatch(...)
// T1.4  FastMath.min/max -> Math.min/max, drop the FastMath import
// T1.5  RATIO_CAPITAL_TO_RWA -> public static final
// T2.1  hoist getMeasurementsByRating() into one local
// T2.2  fuse the four loops -- ONLY after the legality argument is written
{code}

**Acceptance Tests:**

1. Verify that the class compiles with no new IDE warnings, and that the 3 pre-existing warnings are reduced
2. Verify that no import left in the file is unused
3. Verify that the reference UAT scenario runs to completion after the change
4. Verify that a FULL OUTER JOIN on the business key between the before and after outputs returns ZERO mismatching rows on the 8 facility-level fields: crr3_irb_rwa, crr3_irb_secured_rwa, crr3_irb_unsecured_rwa, crr3_irb_capital, crr3_irb_secured_capital, crr3_irb_unsecured_capital, crr3_irb_secured_rw_min, crr3_irb_secured_rw_max
5. Verify that the same join returns ZERO mismatching rows on the 8 MeasurementsOfRating-level fields: crr3_irb_rwa, crr3_irb_secured_rwa, crr3_irb_unsecured_rwa, crr3_irb_secured_rw, crr3_irb_secured_rw_max, crr3_irb_unsecured_rw, crr3_irb_secured_capital, crr3_irb_unsecured_capital
6. Verify that the comparison uses exact {{=}} on the double columns and NOT an epsilon predicate. If a tolerance is needed to make it pass, the refactor is wrong and the ticket is not done
7. Verify that before and after row counts are equal, at both facility and rating level
8. Verify that nothing listed as "Do NOT" in Business Rules has been modified, by reading the diff

**Impacts on Inputs/Outputs:**

* No change. No field is added, removed or renamed, and no configuration option is introduced
* The method contract {{public Facility execute(Facility facility)}} is unchanged
* One dependency edge is removed: the step no longer calls commons-math3 {{FastMath}}, which reduces exposure to the version conflicts already seen on the YARN classpath

**Impacts on Data Quality:**

* None expected, and that is the acceptance criterion rather than a hope. Every output field must be bit-identical
* This is NOT a case of "expected volume change" — any difference at all, in any row, is a defect in the refactor and blocks the PR

**Impacts on TWIST:**

* No test case update required: the functional contract does not change
* The non-regression comparison from Acceptance Tests 4-7 must be attached to the PR as evidence, with row counts and mismatch counts

**Impacts on Data Dictionary:**

* No changes to data dictionary as this is a pure refactoring with no schema, field or type change

**Configuration Example:**

Not applicable — this ticket introduces no configuration option. The existing
useCrr3 flag is explicitly out of scope and is left as it is.
```

---

## Observations to record in the PR, not to act on

These come out of reading the class. They are **observations only** — none of them
is in scope, and acting on any of them would break the bit-identical requirement.

1. **`rw_secured_min` / `rw_secured_max` are summed, not min/maxed.** The two
   facility fields are fed with `+=` over per-rating risk weights. Summing risk
   weights is dimensionally meaningless; this looks like it should be
   `Math.min` / `Math.max`, or an EAD-weighted average. Functional change,
   regulatory impact — needs its own ticket and product owner sign-off.
2. **Fields left unassigned on the zero branch.** In the unsecured loop
   `crr3_irb_unsecured_rw` is set only in the `else`; in the secured loop
   `crr3_irb_secured_rw` and `crr3_irb_secured_rw_max` likewise. The aggregation
   then reads them. What do they hold at that point, where does that value come
   from, and is it deterministic across runs?
3. **Dirty ratings.** They `continue` out of the unsecured and secured loops, so
   their `rw` fields are never set — yet the total and agg loops iterate over all
   ratings including dirty ones. Intended?
4. **Guard 2 reads as** "return early unless at least one rating has a *clean
   unpaid*". Does that match the functional specification? Worth putting to the BA.
5. **`getEadReg() == 0` on a `double`** with no epsilon, at two places. When is
   that safe and when is it not?

---

## Definition of done

- [ ] One commit per task, clear messages
- [ ] Compiles, no new warnings, no unused imports
- [ ] Non-regression run executed on the reference UAT scenario
- [ ] **Zero** mismatching rows across all 16 fields in Acceptance Tests 4 and 5
- [ ] PR description carries the loop-fusion legality argument (or states that the
      fusion was not done, and why)
- [ ] PR description carries the non-regression evidence: row counts and mismatch counts
- [ ] PR description carries the observations above, as observations
- [ ] Nothing from the "Do NOT" rules modified
