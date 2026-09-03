# Onboarding tickets — Mayssa — `StepCrr3IrbRwaCompute`

Onboarding onto the Java/Spark code of the Simulator engine, as a **sequence of
small tickets**: one parent, eight sub-tasks, each independently doable,
reviewable and revertable.

Source: `ONBOARDING_Mayssa_StepCrr3IrbRwaCompute.md`.
Format: `TEMPLATE_jira_ticket_STE.md` (from `AERL_STE-844`).

| Field | Value |
|---|---|
| Board | Kanban Moteurs / Test Engines |
| Project | `str_simulator_engine` / `str-bigData` |
| Branch | `develop` |
| Assignee | Mayssa |
| Target class | `str-bigData/src/main/java/com/bnpparibas/sit/credit/risk/steps/crr3/irb/StepCrr3IrbRwaCompute.java` |

## The sequence

Do them in this order. Each is one commit. The order is not cosmetic: **T1.1**
changes the loop variable that **T2.1** hoists, and **T1.2** removes one of the
passes that **T2.2** fuses.

| # | Ticket | Tier | Effort | Depends on |
|---|---|---|---|---|
| 1 | `entrySet()` → `values()` | 1 | 30 min | — |
| 2 | Remove the dead `ead` computation | 1 | 30 min | — |
| 3 | `!anyMatch(...)` → `noneMatch(...)` | 1 | 15 min | — |
| 4 | `FastMath.min/max` → `Math.min/max` | 1 | 15 min | — |
| 5 | `RATIO_CAPITAL_TO_RWA` → `final` | 1 | 15 min | — |
| 6 | Hoist the repeated map lookup | 2 | 30 min | 1 |
| 7 | Fuse the four loops | 2 | half a day | 1, 2, 6 |
| 8 | Non-regression proof | — | 1 day | all |

Ticket 8 is the one that matters. Tickets 1–7 are the exercise; ticket 8 is the
deliverable. **No PR is opened before 8 is green.**

> **Core rule of this platform:** on a regulatory engine you never refactor
> without demonstrating bit-identical output. Regulatory results are audited. A
> "harmless improvement" that shifts a number by 1e-12 is a production incident.

---

# PARENT — Onboarding: refactor and optimise `StepCrr3IrbRwaCompute`

```
**Context:** StepCrr3IrbRwaCompute is one step of the CRR3/Basel IV IRB computation chain, a Spring @Component(PROTOTYPE) extending AbstractStepMap and invoked per-facility inside a Spark map. Its behaviour is correct and must not change. It carries a dead EAD computation and iterates the same rating map four times. This is an onboarding assignment: the goal is to learn the codebase and the platform's non-regression discipline, not to deliver a feature. We need to:

1. Apply five mechanical clean-ups that provably preserve behaviour (sub-tasks 1-5)
2. Hoist a map lookup resolved 5 times in one method (sub-task 6)
3. Fuse four passes over the rating map into one, conditional on a written legality argument (sub-task 7)
4. Prove the output is bit-identical on a reference UAT scenario (sub-task 8)

**Business Rules:**

* One sub-task per commit. Small commits make the review and the rollback easy
* Output must be bit-identical. Not "within tolerance" -- identical. Any difference in any row is a defect in the refactor, not a rounding detail
* Do NOT replace {{/ RATIO_CAPITAL_TO_RWA}} with {{* 0.08}}. 1/12.5 is exactly representable in binary floating point, 0.08 is not; the results differ in the last bits and the figure is audited
* Do NOT remove {{useCrr3}} or the commented-out {{//if(!useCrr3) return facility;}}. That is a disabled feature flag injected from {{ConfigurationConstants.PROPERTY_USE_CRR3_OPTIONS}}, not dead code. Ask the author before touching it
* Do NOT correct the {{Ccr3}}/{{Crr3}} getter misspelling ({{getCcr3_irba_secured_ead_ratio()}}, {{getCcr3_irba_secured_rw()}} and the IRBF equivalents). Renaming reaches the model class, its Spark encoders and possibly persisted schemas. This codebase has several load-bearing misspellings ({{statingUtils}}, {{isAppliable}}, {{used_worfklow}}, {{lauchType}}, {{standard_apporach}}) -- never silently correct one
* Do NOT change the {{+=}} on {{rw_secured_min}}/{{rw_secured_max}}. It is almost certainly wrong, and it is a functional change on a regulatory figure: separate ticket, separate impact analysis, product owner sign-off

**New Reference:**

{code:java}
// Scope of the assignment: the execute() method only.
public Facility execute(Facility facility) throws SparkException
{code}

**Code Snippet Scala**

{code:scala}
// Sub-task order -- the dependencies are real, not cosmetic
// 1  entrySet() -> values()            (changes the variable 6 hoists)
// 2  delete the dead `double ead` loop (removes a pass 7 would fuse)
// 3  !anyMatch(...) -> noneMatch(...)
// 4  FastMath.min/max -> Math.min/max
// 5  RATIO_CAPITAL_TO_RWA -> final
// 6  hoist getMeasurementsByRating()   (needs 1)
// 7  fuse the four loops               (needs 1, 2, 6)
// 8  non-regression proof              (needs all)
{code}

**Acceptance Tests:**

1. Verify that sub-tasks 1 to 8 are all closed
2. Verify that the history contains one commit per sub-task, with a message naming it
3. Verify that the class compiles with no new IDE warnings, and that the 3 pre-existing warnings are reduced
4. Verify that no import left in the file is unused
5. Verify that sub-task 8 reports ZERO mismatching rows
6. Verify that the PR description carries the loop-fusion legality argument from sub-task 7, or states that the fusion was not done and why
7. Verify that nothing listed as "Do NOT" in Business Rules has been modified, by reading the diff

**Impacts on Inputs/Outputs:**

* No change. No field is added, removed or renamed, no configuration option is introduced, and the {{execute(Facility)}} contract is unchanged
* One dependency edge is removed: the step stops calling commons-math3 {{FastMath}}, which reduces exposure to the version conflicts already seen on the YARN classpath

**Impacts on Data Quality:**

* None expected, and that is the acceptance criterion rather than a hope
* This is NOT a case of "expected volume change". Any difference in any row blocks the PR

**Impacts on TWIST:**

* No test case update required: the functional contract does not change
* The non-regression comparison from sub-task 8 is attached to the PR as evidence, with row counts and mismatch counts

**Impacts on Data Dictionary:**

* No changes to data dictionary as this is a pure refactoring with no schema, field or type change
```

---

# 1 — Replace `entrySet()` with `values()`

```
**Context:** Four loops in execute() iterate getMeasurementsByRating().entrySet() and immediately call mapEntry.getValue(). The Rating key is never read in any of them. We need to:

1. Replace the four entrySet() loops with values() loops
2. Remove the imports that become unused

**Business Rules:**

* The four loops are the unsecured, secured, total and agg passes
* In each, the first statement of the body is {{MeasurementsOfRating measurementsOfRating = mapEntry.getValue();}} -- confirm that before changing it, do not assume
* Once all four are done, {{java.util.Map}} and {{com.bnpparibas.sit.credit.risk.core.model.Rating}} are unused. Remove both imports
* Behaviour change: none. {{values()}} iterates the same entries in the same order as {{entrySet()}} on the same map instance

**New Reference:**

{code:java}
// before
for (Map.Entry<Rating, MeasurementsOfRating> mapEntry
        : facility.getfMeasurement().getMeasurementsByRating().entrySet()) {
    MeasurementsOfRating measurementsOfRating = mapEntry.getValue();
    ...
}

// after
for (MeasurementsOfRating measurementsOfRating
        : facility.getfMeasurement().getMeasurementsByRating().values()) {
    ...
}
{code}

**Acceptance Tests:**

1. Verify that no occurrence of {{entrySet()}} remains in the class
2. Verify that no occurrence of {{mapEntry}} remains in the class
3. Verify that the imports {{java.util.Map}} and {{...core.model.Rating}} are gone
4. Verify that the class compiles with no new warning
5. Verify that the diff touches only loop headers, the removed {{getValue()}} lines and the two imports -- NOT a single arithmetic expression

**Impacts on Inputs/Outputs:**

* No change

**Impacts on Data Quality:**

* No change. This is a mechanical rewrite with identical semantics

**Impacts on TWIST:**

* No change

**Impacts on Data Dictionary:**

* No changes to data dictionary as this is a code-level refactoring
```

---

# 2 — Remove the dead `ead` computation

```
**Context:** execute() runs a full pass over the rating map to accumulate a local double ead. Nothing reads it. We need to:

1. Confirm the variable is genuinely unused
2. Delete the variable and the loop that fills it

**Business Rules:**

* Run Find Usages on {{ead}} BEFORE deleting, and confirm zero reads within the method. Get into this habit -- never delete on the strength of a visual scan
* The loop being deleted is the one guarded by {{if (value.isDirty()) continue;}} that accumulates {{value.getEadReg()}}
* Do not confuse this local with {{measurementsOfRating.getEadReg()}}, which IS read later in the unsecured and secured loops and must stay
* Behaviour change: none. The loop has no side effect -- it only writes a local

**New Reference:**

{code:java}
// delete entirely
double ead = 0;
for (MeasurementsOfRating value : facility.getfMeasurement().getMeasurementsByRating().values()) {
    if (value.isDirty()) continue;
    ead += value.getEadReg();
}
{code}

**Acceptance Tests:**

1. Verify that Find Usages on {{ead}} reported no read before the deletion, and record that in the commit message
2. Verify that the local {{ead}} no longer exists in the class
3. Verify that {{getEadReg()}} is still called in the unsecured and secured loops
4. Verify that the class compiles with no new warning
5. Verify that the number of passes over the rating map drops from five to four

**Impacts on Inputs/Outputs:**

* No change

**Impacts on Data Quality:**

* No change. The deleted loop wrote only a local variable that was never read

**Impacts on TWIST:**

* No change

**Impacts on Data Dictionary:**

* No changes to data dictionary as this is a code-level refactoring
```

---

# 3 — `!...anyMatch(...)` → `noneMatch(...)`

```
**Context:** Guard 2 reads as a negated anyMatch. noneMatch expresses the same thing directly. We need to:

1. Replace the negation with noneMatch

**Business Rules:**

* {{!stream().anyMatch(p)}} and {{stream().noneMatch(p)}} are equivalent for every predicate and every stream, including the empty one -- both are true when nothing matches
* The predicate is unchanged: {{m.getUnpaid() != null && m.getUnpaid().isClean()}}
* Do not change what the guard MEANS while you are in there. Whether "return early unless at least one rating has a clean unpaid" matches the functional spec is a question for the BA, recorded as an observation on the parent, not resolved here

**New Reference:**

{code:java}
if (facility.getfMeasurement().getMeasurementsByRating().values().stream()
        .noneMatch(m -> m.getUnpaid() != null && m.getUnpaid().isClean())) {
    return facility;
}
{code}

**Acceptance Tests:**

1. Verify that the class contains no {{!}}-negated {{anyMatch}}
2. Verify that the predicate body is byte-for-byte the one that was there before
3. Verify that the class compiles with no new warning
4. Verify that a facility whose ratings have NO clean unpaid still returns early, unchanged

**Impacts on Inputs/Outputs:**

* No change

**Impacts on Data Quality:**

* No change. Identical semantics on every input including an empty rating map

**Impacts on TWIST:**

* No change

**Impacts on Data Dictionary:**

* No changes to data dictionary as this is a code-level refactoring
```

---

# 4 — `FastMath.min/max` → `Math.min/max`

```
**Context:** The secured loop calls commons-math3 FastMath.min and FastMath.max on doubles. For double, java.lang.Math.min/max are JVM intrinsics and are semantically identical. We need to:

1. Replace the two calls
2. Remove the FastMath import if it becomes unused

**Business Rules:**

* Applies to the two calls that produce {{rw}} and {{rwMax}} from {{secured_rw}} and {{rw_tmp}}
* {{Math.min}}/{{Math.max}} for {{double}} follow the same IEEE-754 rules as the commons-math3 versions, including the NaN and signed-zero cases. The result is bit-identical
* This also removes one dependency on commons-math3, which is a live problem on our YARN classpath -- version conflicts there have already caused NoSuchMethodError elsewhere in the platform
* Check whether {{FastMath}} is used anywhere else in the file before removing the import

**New Reference:**

{code:java}
double rw    = Math.min(secured_rw, rw_tmp);
double rwMax = Math.max(secured_rw, rw_tmp);
{code}

**Acceptance Tests:**

1. Verify that no {{FastMath}} call remains in the class
2. Verify that the {{org.apache.commons.math3.util.FastMath}} import is gone
3. Verify that the argument ORDER is unchanged in both calls -- {{(secured_rw, rw_tmp)}}, not the reverse
4. Verify that the class compiles with no new warning

**Impacts on Inputs/Outputs:**

* No change to any field. One library dependency edge removed

**Impacts on Data Quality:**

* No change. Bit-identical for double on every input

**Impacts on TWIST:**

* No change

**Impacts on Data Dictionary:**

* No changes to data dictionary as this is a code-level refactoring
```

---

# 5 — Make `RATIO_CAPITAL_TO_RWA` final

```
**Context:** RATIO_CAPITAL_TO_RWA is declared public static double and is never written. A mutable public static field in a bean that executes on Spark executors is a latent hazard: a write would be non-deterministic across the cluster and would not propagate the way the author expected. We need to:

1. Confirm all usages are reads
2. Add final

**Business Rules:**

* The constant is 12.5, the inverse of the 8% minimum capital ratio (1 / 0.08). Converting between RWA and capital is a multiply or divide by it
* Check the 6 reported usages are all reads before applying. If any write exists, STOP and raise it -- that is a finding, not an obstacle
* Do NOT take the opportunity to replace {{/ RATIO_CAPITAL_TO_RWA}} with {{* 0.08}} anywhere. 1/12.5 is exactly representable in binary floating point, 0.08 is not

**New Reference:**

{code:java}
public static final double RATIO_CAPITAL_TO_RWA = 12.5;
{code}

**Acceptance Tests:**

1. Verify that all usages were reads, and record the count in the commit message
2. Verify that the field is declared {{public static final double}}
3. Verify that the value is still exactly {{12.5}}
4. Verify that no division by the constant was rewritten as a multiplication
5. Verify that the class compiles with no new warning

**Impacts on Inputs/Outputs:**

* No change

**Impacts on Data Quality:**

* No change. The field was never written, so making it final cannot alter any computed value

**Impacts on TWIST:**

* No change

**Impacts on Data Dictionary:**

* No changes to data dictionary as this is a code-level refactoring
```

---

# 6 — Hoist the repeated map lookup

```
**Context:** facility.getfMeasurement().getMeasurementsByRating() is resolved five times in one method: once in guard 2 and once per loop. We need to:

1. Resolve it once into a local and use that everywhere

**Business Rules:**

* Depends on sub-task 1: after {{entrySet()}} -> {{values()}} the natural local type is {{Collection<MeasurementsOfRating>}}, not {{Map<Rating, MeasurementsOfRating>}}
* Declare it AFTER the two early-return guards, not before. Hoisting above a guard changes what runs on the early-return path -- harmless here, but the habit is what matters
* The map instance is not replaced anywhere in the method, so one lookup is equivalent to five. Confirm that: search the method for any setter on {{getfMeasurement()}}
* Behaviour change: none

**New Reference:**

{code:java}
Collection<MeasurementsOfRating> measurements =
        facility.getfMeasurement().getMeasurementsByRating().values();
{code}

**Acceptance Tests:**

1. Verify that {{getMeasurementsByRating()}} appears at most twice in the method -- once in the guard, once in the hoist
2. Verify that the local is declared after both early-return guards
3. Verify that no setter replaces the measurement map anywhere in the method
4. Verify that the class compiles with no new warning
5. Verify that the required import for {{Collection}} is present and that no other import was added

**Impacts on Inputs/Outputs:**

* No change

**Impacts on Data Quality:**

* No change. The same map instance is read, once instead of five times

**Impacts on TWIST:**

* No change

**Impacts on Data Dictionary:**

* No changes to data dictionary as this is a code-level refactoring
```

---

# 7 — Fuse the four loops into one pass

```
**Context:** After sub-task 2 there are four passes over the same rating collection: unsecured, secured, total, agg. They may be fusable into one. Whether they ARE is the question this ticket is really asking. We need to:

1. Establish, in writing, whether the fusion is legal
2. If and only if it is, fuse the four loops into one
3. If it is not, close the ticket with the argument and leave the loops alone

**Business Rules:**

* The legality question is exactly this: does the computation for rating N read any value produced for a rating M != N? Answer it by reading the four loop bodies and listing, for each read, where the value it reads was written
* Write the reasoning in the PR description. This is the analysis the task is testing -- an unexplained fusion that happens to work is a fail, and a well-argued "not fusable" is a pass
* Depends on sub-tasks 1, 2 and 6
* If fused: accumulate {{rwa_unsecured}}, {{rwa_secured}}, {{rw_secured_min}}, {{rw_secured_max}} inline, and keep every facility-level assignment AFTER the loop, in the order it is in today
* The per-facility gain is modest -- the rating map is small. The gain is multiplied by the facility volume of a full stress-test run, and the readability gain is the real prize. Do NOT claim a speed-up you have not measured
* Do NOT change the {{+=}} on {{rw_secured_min}}/{{rw_secured_max}} while fusing. Carry the existing behaviour across verbatim, wrong as it looks

**New Reference:**

{code:java}
// SHAPE only. The body is deliberately not given: writing it is the task, and
// it is legal only once the argument above has been made.

double rwa_unsecured  = 0;
double rwa_secured    = 0;
double rw_secured_min = 0;
double rw_secured_max = 0;

for (MeasurementsOfRating measurementsOfRating : measurements) {
    // unsecured, then secured, then total -- for THIS rating only
    // then accumulate the four facility-level values
}

// facility-level assignments, unchanged, after the loop
{code}

**Acceptance Tests:**

1. Verify that the PR description contains the legality argument, naming for each cross-loop read where the value was written
2. Verify that the class iterates the rating collection ONCE in execute(), or that the ticket is closed with a documented refusal to fuse
3. Verify that the facility-level assignments after the loop are in the same order and compute the same expressions as before
4. Verify that the dirty-rating and zero-ratio early paths behave as they did -- the {{continue}} in the unsecured loop and the {{continue}} in the secured loop do NOT mean the same thing once fused
5. Verify that {{rw_secured_min}} and {{rw_secured_max}} are still accumulated with {{+=}}
6. Verify that the class compiles with no new warning

**Impacts on Inputs/Outputs:**

* No change

**Impacts on Data Quality:**

* None expected, and sub-task 8 is what proves it. This is the sub-task most likely to break bit-identity, which is why it is last before the proof

**Impacts on TWIST:**

* No change

**Impacts on Data Dictionary:**

* No changes to data dictionary as this is a code-level refactoring
```

---

# 8 — Non-regression proof

```
**Context:** Sub-tasks 1-7 change code on an audited regulatory engine. Nothing merges until the output is shown to be bit-identical. This ticket is the deliverable of the whole assignment. We need to:

1. Run a reference UAT scenario before the changes and persist the output
2. Run it after and persist the output
3. Compare every affected field for strict equality
4. Report row counts and mismatch counts

**Business Rules:**

* Pick a reference UAT scenario with a representative facility population -- ask which one, do not choose alone
* Compare with a FULL OUTER JOIN on the business key and an equality predicate per column
* Use exact {{=}} on the double columns. NOT {{abs(a-b) < eps}}. If a tolerance is needed to make it pass, the refactor is wrong
* Facility level, 8 fields: {{crr3_irb_rwa}}, {{crr3_irb_secured_rwa}}, {{crr3_irb_unsecured_rwa}}, {{crr3_irb_capital}}, {{crr3_irb_secured_capital}}, {{crr3_irb_unsecured_capital}}, {{crr3_irb_secured_rw_min}}, {{crr3_irb_secured_rw_max}}
* MeasurementsOfRating level, 8 fields: {{crr3_irb_rwa}}, {{crr3_irb_secured_rwa}}, {{crr3_irb_unsecured_rwa}}, {{crr3_irb_secured_rw}}, {{crr3_irb_secured_rw_max}}, {{crr3_irb_unsecured_rw}}, {{crr3_irb_secured_capital}}, {{crr3_irb_unsecured_capital}}
* A FULL OUTER JOIN is used rather than an inner one on purpose: it catches a row present on one side only, which an inner join would silently drop
* The expected result is ZERO mismatching rows. Zero, not "a handful in the last digit"

**Acceptance Tests:**

1. Verify that the before and after runs used the same scenario, the same input data and the same configuration
2. Verify that facility-level row counts are equal before and after
3. Verify that rating-level row counts are equal before and after
4. Verify that the comparison reports ZERO mismatching rows on all 8 facility-level fields
5. Verify that the comparison reports ZERO mismatching rows on all 8 rating-level fields
6. Verify that the comparison query uses exact {{=}} and contains no epsilon or rounding
7. Verify that the row counts and mismatch counts are pasted into the PR description, not merely stated as "passed"
8. Verify that if ANY mismatch is found, the PR stays closed and the offending sub-task is identified by bisecting the commits

**Impacts on Inputs/Outputs:**

* No change. This ticket produces evidence, not code

**Impacts on Data Quality:**

* This ticket IS the data quality check for the whole assignment

**Impacts on TWIST:**

* The comparison output is attached to the PR as the non-regression evidence
* No TWIST test case changes: the functional contract is unchanged

**Impacts on Data Dictionary:**

* No changes to data dictionary as this ticket only produces evidence
```

---

## Observations — record in the PR, do not act on

None of these is in scope. Acting on any of them breaks bit-identity. Spot them
yourself before we discuss them.

1. **`rw_secured_min` / `rw_secured_max` are summed, not min/maxed.** Both
   facility fields are fed with `+=` over per-rating risk weights. Summing risk
   weights is dimensionally meaningless; it looks like it should be `Math.min` /
   `Math.max`, or an EAD-weighted average. **Needs its own ticket, its own impact
   analysis and product owner sign-off** — it changes a regulatory figure.
2. **Fields left unassigned on the zero branch.** In the unsecured loop
   `crr3_irb_unsecured_rw` is set only in the `else`; in the secured loop
   `crr3_irb_secured_rw` and `crr3_irb_secured_rw_max` likewise. The aggregation
   then reads them. What do they hold at that point, where does that value come
   from, and is it deterministic across runs?
3. **Dirty ratings.** They `continue` out of the unsecured and secured loops, so
   their `rw` fields are never set — yet the total and agg loops iterate over all
   ratings including dirty ones. Intended?
4. **Guard 2** reads as "return early unless at least one rating has a *clean
   unpaid*". Does that match the functional specification? One for the BA.
5. **`getEadReg() == 0` on a `double`** with no epsilon, in two places. When is
   that safe and when is it not?
