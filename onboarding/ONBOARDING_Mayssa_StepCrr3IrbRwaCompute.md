# Onboarding task — `StepCrr3IrbRwaCompute`

**Assignee:** Mayssa
**Project:** `str_simulator_engine` / `str-bigData`
**Branch:** `develop`
**File:** `str-bigData/src/main/java/com/bnpparibas/sit/credit/risk/steps/crr3/irb/StepCrr3IrbRwaCompute.java`
**Estimated effort:** 2–3 days including validation

---

## 1. Objective

This is a **learning task, not a delivery task**. The goal is for you to:

1. Read and understand a real production step of the CRR3 / Basel IV RWA engine.
2. Perform a set of safe, incremental refactorings.
3. **Prove** that the refactoring changes nothing in the output.

The deliverable that matters is not the refactored code — it is the **non-regression proof**.

> **Core rule of this platform:** on a regulatory engine, you never refactor without demonstrating bit-identical output. Regulatory results are audited. A "harmless improvement" that shifts a number by 1e-12 is a production incident.

---

## 2. Context — what this class does

`StepCrr3IrbRwaCompute` is a Spring bean (`@Component`, `@Scope(PROTOTYPE)`) implementing one step of the CRR3 (Basel IV) IRB computation chain. It extends `AbstractStepMap` and its contract is a single method:

```java
public Facility execute(Facility facility) throws SparkException
```

It takes one `Facility`, mutates it, and returns it. It is invoked per-facility inside a Spark map operation.

### Functional flow

| Stage | Lines | What happens |
|---|---|---|
| Guard 1 | 34–37 | Skip unless the Basel IV approach is `IRBA` or `IRBF` |
| Guard 2 | 38–41 | Skip unless at least one rating has a non-null, "clean" unpaid |
| Dead code | 43–47 | Computes `ead` — **never used** |
| Ratio selection | 49–60 | Picks unsecured/secured EAD ratios and secured RW depending on IRBA vs IRBF |
| Unsecured loop | 62–81 | Per rating: `rwa_unsecured = partial_unsecured_capital × 12.5`, derive `rw`, derive capital |
| Secured loop | 84–111 | Per rating: same idea, but `rw = min(secured_rw, rw_tmp)` — the **RW floor/cap logic** |
| Total loop | 113–118 | Per rating: `rwa = secured + unsecured` |
| Aggregation | 120–140 | Sum per-rating values up to facility level, derive capital |

### Domain reminder

`RATIO_CAPITAL_TO_RWA = 12.5` is the inverse of the 8% minimum capital ratio (`1 / 0.08 = 12.5`). Converting between RWA and capital requirement is a multiplication/division by this constant. **This is why you must not "simplify" it** — see §5.

---

## 3. Tasks

Do them **in order**, one commit per task. Small commits make the review and the rollback easy.

### Tier 1 — Zero behaviour change, zero risk

#### T1.1 — Replace `entrySet()` with `values()`

Four occurrences: lines 63, 85, 114, 125. In each, the `Rating` key is never used — the first statement in the body is always `mapEntry.getValue()`.

```java
// before
for (Map.Entry<Rating, MeasurementsOfRating> mapEntry
        : facility.getfMeasurement().getMeasurementsByRating().entrySet()) {
    MeasurementsOfRating measurementsOfRating = mapEntry.getValue();
    ...

// after
for (MeasurementsOfRating measurementsOfRating
        : facility.getfMeasurement().getMeasurementsByRating().values()) {
    ...
```

Once all four are done, the imports `java.util.Map` and `...core.model.Rating` become unused — remove them.

#### T1.2 — Remove the dead `ead` computation (lines 43–47)

A full pass over the rating map is performed to compute a local `double ead` that nothing reads.

**Before deleting:** run *Find Usages* on the variable and confirm it is genuinely unused within the method. Get into this habit — never delete on the strength of a visual scan.

#### T1.3 — `!...anyMatch(...)` → `noneMatch(...)`

Line 38. Same semantics, expresses the intent directly.

```java
if (facility.getfMeasurement().getMeasurementsByRating().values().stream()
        .noneMatch(m -> m.getUnpaid() != null && m.getUnpaid().isClean())) {
    return facility;
}
```

#### T1.4 — `FastMath.min/max` → `Math.min/max`

Lines 101–102. For `double`, `Math.min`/`Math.max` are JVM intrinsics and are semantically identical to the commons-math3 versions. This also removes one more dependency on commons-math3, which is a live problem on our YARN classpath (version conflicts have already caused `NoSuchMethodError` elsewhere in the platform).

If this removes the last `FastMath` usage in the file, drop the `org.apache.commons.math3.util.FastMath` import too.

#### T1.5 — Make `RATIO_CAPITAL_TO_RWA` final

```java
public static final double RATIO_CAPITAL_TO_RWA = 12.5;
```

A mutable `public static` field in a bean that executes on Spark executors is a latent hazard: any write would be non-deterministic across the cluster. Check the 6 reported usages are all reads before applying.

---

### Tier 2 — Real refactor, still behaviour-preserving

#### T2.1 — Hoist the repeated map lookup

`facility.getfMeasurement().getMeasurementsByRating()` is resolved 5 times. Extract it once at the top of the method:

```java
Map<Rating, MeasurementsOfRating> measurementsByRating =
        facility.getfMeasurement().getMeasurementsByRating();
```

(Or `Collection<MeasurementsOfRating>` if T1.1 is already done.)

#### T2.2 — Fuse the loops

After T1.2, there are four passes over the same collection: *unsecured*, *secured*, *total*, *agg*.

**First, convince yourself the fusion is legal.** Ask: does the computation for rating *N* read any value produced for rating *M ≠ N*? Write your reasoning in the PR description. This is the analysis skill the task is really testing.

If — and only if — the answer is no, fuse them into a single pass, accumulating `rwa_unsecured`, `rwa_secured`, `rw_secured_min`, `rw_secured_max` inline, and doing the facility-level assignments after the loop.

The raw performance gain per facility is modest (the rating map is small), but it is multiplied by the facility volume across a full stress-test run, and the readability gain is the real prize.

---

## 4. Non-regression validation — **the actual deliverable**

Do not open the PR without this.

1. Pick a reference UAT scenario with a representative facility population (ask me which one).
2. Run the step **before** your changes; persist the output.
3. Run the step **after**; persist the output.
4. Compare the following fields for **strict bit-level equality** — not "close enough", not a tolerance:

   **Facility level**
   - `crr3_irb_rwa`
   - `crr3_irb_secured_rwa`
   - `crr3_irb_unsecured_rwa`
   - `crr3_irb_capital`
   - `crr3_irb_secured_capital`
   - `crr3_irb_unsecured_capital`
   - `crr3_irb_secured_rw_min`
   - `crr3_irb_secured_rw_max`

   **MeasurementsOfRating level**
   - `crr3_irb_rwa`
   - `crr3_irb_secured_rwa`, `crr3_irb_unsecured_rwa`
   - `crr3_irb_secured_rw`, `crr3_irb_secured_rw_max`, `crr3_irb_unsecured_rw`
   - `crr3_irb_secured_capital`, `crr3_irb_unsecured_capital`

5. Report row counts and the count of mismatching rows. **The expected result is zero.**

> A `FULL OUTER JOIN` on the business key with an equality predicate per column is enough. Use exact `=` on the double columns, not `abs(a-b) < eps`. If a tolerance is needed to make it pass, the refactor is wrong.

---

## 5. Out of scope — do NOT touch

These are deliberately left alone. Understanding *why* is part of the exercise.

### 5.1 — Do not replace `/ RATIO_CAPITAL_TO_RWA` with `* 0.08`

Mathematically equivalent, numerically **not**. `1/12.5` is exactly representable in binary floating point, `0.08` is not. The results would differ in the last bits, the non-regression test would fail, and on an audited regulatory figure that is a defect, not a rounding detail.

### 5.2 — Do not remove `useCrr3` or the commented-out line 32

```java
//if(!useCrr3) return facility;
```

`private boolean useCrr3` is injected via `@Value` from `ConfigurationConstants.PROPERTY_USE_CRR3_OPTIONS` and is currently referenced only by this commented line. That is a **disabled feature flag**, not dead code. Removing it would break the configuration contract. Ask the author before touching it.

### 5.3 — Do not "fix" the `Ccr3` / `Crr3` getter typo

Lines 54–55 and 58–59 call `getCcr3_irba_secured_ead_ratio()`, `getCcr3_irba_secured_rw()`, etc. — `Ccr3`, not `Crr3`. This looks like a typo in `Facility`, but renaming it touches the model class, its Spark encoders, and possibly persisted schemas. Out of scope. Note it, don't fix it.

This codebase has several such preserved misspellings (`statingUtils`, `isAppliable`, `used_worfklow`, `lauchType`, `standard_apporach`). **Rule: never silently correct one.** They are load-bearing.

### 5.4 — Do not fix the `+=` on `rw_secured_min` / `rw_secured_max`

Lines 129–130:

```java
rw_secured_min += measurementsOfRating.getCrr3_irb_secured_rw();
rw_secured_max += measurementsOfRating.getCrr3_irb_secured_rw_max();
```

These *sum* per-rating risk weights and then assign the result to facility fields named `_min` and `_max`. Summing risk weights is dimensionally meaningless — this is almost certainly meant to be `Math.min` / `Math.max`, or an EAD-weighted average.

**But this is a functional change with regulatory impact.** It gets its own ticket, its own impact analysis, and product owner sign-off. Do not bundle it into a refactoring PR. Flag it in the PR description as an observation.

---

## 6. Discussion points — try to spot these yourself

Have a look before we talk about them.

### 6.1 — Unset fields on the zero-ratio branch

In the unsecured loop (lines 71–79), when `unsecured_ead_ratio == 0.0` or `getEadReg() == 0`, only `crr3_irb_unsecured_rwa` is set to `0.0`. `crr3_irb_unsecured_rw` is set **only in the `else` branch**.

Same pattern in the secured loop: on the zero branch, `crr3_irb_secured_rw` and `crr3_irb_secured_rw_max` are never assigned.

The aggregation loop then reads those never-assigned fields. **Questions:** what value do they hold at that point? Where does it come from? Is it deterministic across runs?

### 6.2 — Dirty rows

Dirty rows `continue` early in the unsecured and secured loops, so their `rw` fields are also never set — yet the total loop and the agg loop iterate over **all** ratings including dirty ones. Is that intended?

### 6.3 — Guard 2 semantics

```java
.anyMatch(m -> m.getUnpaid() != null && m.getUnpaid().isClean())
```

Read literally: "return early unless at least one rating has a *clean unpaid*". Does that match the functional specification of the step? Worth asking the business analyst.

### 6.4 — Floating-point equality

Lines 71 and 94 test `getEadReg() == 0` on a `double` with no epsilon. When is that safe, when is it not?

---

## 7. Definition of done

- [ ] One commit per task, clear messages
- [ ] Compiles, no new IDE warnings (the current 3 should be reduced)
- [ ] Unused imports removed
- [ ] Non-regression run executed on the reference UAT scenario
- [ ] **Zero** mismatching rows on all fields listed in §4
- [ ] PR description contains:
  - the loop-fusion legality argument (T2.2)
  - the non-regression evidence (row counts, mismatch counts)
  - the observations from §5.4 and §6, as observations only
- [ ] Nothing from §5 modified

---

## 8. Reference

Extracted source as-is (145 lines): `StepCrr3IrbRwaCompute.java`

Transcription caveats on that file:
- **Line 39** — `MeasurementsOfRating` in `.anyMatch( MeasurementsOfRating measurementsOfRating -> ...)` appears to be an IntelliJ parameter-type inlay hint rather than source text (the literal form would not compile). Check the real file in the repo.
- `getfMeasurement()` — lowercase `f`, correct and consistent with the rest of the codebase.
- Blank-line placement at lines 61, 82–83, 92–93 is inferred from screenshot overlap seams; line count reconciles to 145.
