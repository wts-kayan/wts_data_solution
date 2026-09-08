# Code review — `StepCrr3IrbRwaCompute` refactor

**Author:** Mayssa
**Ticket:** onboarding sub-tasks 1–7 (see `../TICKETS_Mayssa_StepCrr3IrbRwaCompute.md`)
**Submission under review:** [`StepCrr3IrbRwaCompute_Mayssa.java`](StepCrr3IrbRwaCompute_Mayssa.java)
**Baseline:** [`../StepCrr3IrbRwaCompute.java`](../StepCrr3IrbRwaCompute.java)
**Date:** 2026-09-08

---

## Verdict

**Request changes.** The submission does not compile against the model, and once
made to compile it is not behaviour-preserving.

The refactoring *idea* is right and four of the eight sub-tasks are done
correctly. The damage is concentrated in two mistakes, both introduced by the
loop fusion in sub-task 7. Neither is a careless slip — both are the specific
traps this ticket exists to teach — but both change reported RWA, so nothing
ships until they are fixed.

---

## How this was checked

Her code was transcribed into the equivalence harness at `../verify/`, changing
only the four identifiers that would not compile, and run against
`StepOriginal` over the standard population: 200,000 facilities, 115,116 fully
processed, 125,132 dirty ratings, every branch reached. Comparison is exact
(`Double.compare`), no epsilon.

| Variant | Facility-field mismatches | Rating-field mismatches |
|---|---:|---:|
| As submitted | **689,264** | **94,715** |
| With only defect ① fixed | 134,104 | 94,715 |
| Reference (all 7 sub-tasks) | 0 | 0 |

Reproduce with `cd onboarding/verify && ./run.sh`.

---

## Blocking defects

### ① The aggregation accumulators are reused as per-row temporaries

Highest severity. Every facility-level total is wrong.

In the original these were two *different* variables that happened to share a
name. `rwa_unsecured` was declared **inside** the else block (baseline line 74)
as a per-row temp; the aggregation loop had its own `double rwa_unsecured = 0`
(baseline line 120) in a separate scope. Fusing the loops put both in one scope,
the inner `double` was dropped, and the temp assignment now clobbers the
accumulator:

```java
double rwa_unsecured = 0;                                    // accumulator
...
rwa_unsecured = ...getCrr3_irb_partial_unsecured_capital()
                * RATIO_CAPITAL_TO_RWA;                      // ← wipes the running total
measurementsOfRating.setCrr3_irb_unsecured_rwa(rwa_unsecured);
...
rwa_unsecured += measurementsOfRating.getCrr3_irb_unsecured_rwa();   // ← 2 × this row
```

Every row reaching the else branch discards the sum so far and leaves
`2 × its own RWA`. Identical bug verbatim in `rwa_secured`.

Fields affected: facility `crr3_irb_rwa`, `crr3_irb_secured_rwa`,
`crr3_irb_unsecured_rwa`, and the three capital fields derived from them. This
defect alone accounts for roughly 555,000 of the mismatches.

**Fix.** Give the per-row values their own names (`rwa_unsecured_rating`,
`rwa_secured_rating`), declared at the point of use. An accumulator should never
appear on the left of a plain `=` inside the loop.

Same root cause, currently harmless: `double rw_unsecured;` was also lifted to
the top of the method. It does not collide today, but it is the identical
instinct — widening a temp's scope until it *can* collide. Declare temps where
they are used.

### ② `continue` inside the fused loop

This is the trap sub-task 7 is built around, and acceptance test 4 exists
specifically to catch it.

In the original, the `continue` in the unsecured pass skipped only *the rest of
that pass*. Dirty ratings still went through the secured pass, the total pass
and the aggregation pass. In a single fused loop, `continue` skips **all** of
them. For every dirty rating the submission now:

- never sets `crr3_irb_secured_rwa` to `0.0` — the original does;
- never sets `crr3_irb_rwa` at all — the harness shows it retaining the `-99.0`
  sentinel where the original writes `0.0`;
- contributes nothing to `rw_secured_min` / `rw_secured_max`.

That last point is why `facility[3].crr3_irb_secured_rw_min` comes out `0.015`
against the original's `-3.235`. Note that the stub model in `../verify/Model.java`
deliberately pre-seeds these fields with non-zero sentinels precisely so that
stale-field bugs surface; in production the field holds whatever the previous
step left there.

**Fix.** The two dirty branches must be `if/else`, not `if { ...; continue; }`.
The total and aggregation blocks have to run for every rating, dirty included.

### ③ The second dirty check is unreachable

```java
if (measurementsOfRating.isDirty()) {      // the first check already continued
    measurementsOfRating.setCrr3_irb_secured_rwa(0.0);
    continue;
}
```

Dead code, and misleading dead code: it *looks* like it preserves the original's
dirty-secured behaviour while being incapable of running. It disappears on its
own once ② is fixed — call `isDirty()` once into a local `boolean dirty` and
branch on that.

### ④ Four identifiers that do not exist

| Written | Should be |
|---|---|
| `facility.getMeasurement()` | `facility.getfMeasurement()` |
| `getCrr3_irba_secured_rw()` | `getCcr3_irba_secured_rw()` |
| `getCrr3_irbf_secured_ead_ratio()` | `getCcr3_irbf_secured_ead_ratio()` |
| `getCrr3_irbf_secured_rw()` | `getCcr3_irbf_secured_rw()` |

Three of these are the `Ccr3` typo in the model being silently "corrected" — but
only three of the four `Ccr3` getters, since `getCcr3_irba_secured_ead_ratio()`
was left alone.

If the typo is worth fixing, fix it in the model, in its own ticket, across the
board. Half-correcting it inside an unrelated refactor is the one option worse
than either leaving it or fixing it properly.

---

## Sub-tasks not completed

| # | Asked for | State |
|---|---|---|
| 1 | `entrySet()` → `values()` | **Not done** — still `entrySet()` with an unused key |
| 5 | `RATIO_CAPITAL_TO_RWA` final | **Partial** — became `final` but lost `public static` |
| 6 | Hoist `getMeasurementsByRating()`, typed `Collection<MeasurementsOfRating>` | **Partial** — hoisted, but as a `Map`, and placed *above* the guards |

On **5**: `final double RATIO_CAPITAL_TO_RWA = 12.5;` is now an instance field on
a prototype-scoped bean, and any external reference to
`StepCrr3IrbRwaCompute.RATIO_CAPITAL_TO_RWA` stops compiling. It should be
`public static final`.

On **6**: the hoist landed above both guards, so it runs on the early-return
paths too. Harmless here because the call is cheap and non-null — but the ticket
asks for it after the guards, and the habit is the entire point of the sub-task.

---

## Done correctly

- **Sub-task 2** — the dead `double ead` accumulation loop is gone, and she
  correctly established that nothing consumed it.
- **Sub-task 3** — `!anyMatch(...)` → `noneMatch(...)`.
- **Sub-task 4** — `FastMath.min/max` → `Math.min/max`.
- The `if (dirty) → set 0.0` semantics, the `== 0.0` guards, and the
  accumulation *order* are all preserved as written.

**Most importantly: she left the pre-existing oddities alone.**

`rw_secured_min` accumulating with `+=` despite being named "min"; a null
approach silently falling into the IRBF branch; exact float `== 0.0`
comparisons; capital not being set for dirty ratings — every one of these looks
wrong, and every one is out of scope for a behaviour-preserving refactor.
Resisting the urge to fix them is the right instinct and the hardest part of
this ticket to teach.

She should now raise them as separate tickets rather than let them drop.

---

## Suggested order of work

1. Fix ① and ②. These are the only two changes that affect numbers.
2. Re-run `onboarding/verify/run.sh` and confirm bit-identical.
3. Then clean up ③, ④ and the three incomplete sub-tasks — all cosmetic.
4. Open follow-up tickets for the pre-existing smells listed above.

The harness in `../verify/` answers exactly the question this review asked, so
she can check her own work before the next round. That is what it is for.
