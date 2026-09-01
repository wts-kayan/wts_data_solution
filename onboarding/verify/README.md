# Reference solution + equivalence harness

**Mentor material. Do not hand `StepRefactored.java` to Mayssa** — sub-task 7
asks her to establish the loop fusion's legality herself, and the answer is
written at the top of that file.

This exists to answer one question: *is the refactoring the tickets describe
actually behaviour-preserving?* It is checked the way the tickets demand it be
checked — run both versions, compare every field with exact equality, no epsilon.

## Run

```bash
./run.sh                      # 200,000 facilities, default seed
./run.sh 2000000              # more facilities
./run.sh 300000 424242        # a different population
```

Exit code 0 means bit-identical, 1 means a difference was found.

## What is here

| File | What it is |
|---|---|
| `Model.java` | Minimal stand-ins for `Facility`, `MeasurementsOfRating`, `Rating`, `Unpaid`, `Crr3Approach`, and a `FastMath` that delegates to `Math` |
| `StepOriginal.java` | `execute()` verbatim from `develop`. Do not tidy it — it is the reference |
| `StepRefactored.java` | The target after sub-tasks 1–7, with the fusion legality argument in the header |
| `Compare.java` | Generates a population, runs both, compares the 8 facility and 8 rating fields from sub-task 8 |

## Result

```
facilities compared      : 200,000
rating rows compared     : 499,996

branch coverage of the generated population
  returned at guard 1    : 28,572
  returned at guard 2    : 56,312
  fully processed        : 115,116
  dirty ratings          : 125,132
  ratings with eadReg=0  : 100,183
  facilities w/ a 0 ratio: 93,334

facility-level mismatches: 0
rating-level mismatches  : 0
>>> BIT-IDENTICAL on every compared field.
```

The branch counters are part of the result, not decoration. Zero mismatches over
a population that never reached the dirty or zero-EAD branches would prove
nothing, so the harness reports INCONCLUSIVE rather than success if any branch
was missed.

## Runs

| Facilities | Seed | Result |
|---|---|---|
| 200,000 | 20260901 | 0 mismatches |
| 2,000,000 | 20260901 | 0 mismatches |
| 300,000 | 1 | 0 mismatches |
| 300,000 | 7 | 0 mismatches |
| 300,000 | 424242 | 0 mismatches |
| 300,000 | 987654321 | 0 mismatches |

The seed is an argument for a reason: a single fixed one only ever proves the
refactoring is equivalent on one sequence of draws.

## The harness detects the failures it is meant to detect

A green run is only worth something if a red one is possible. Fusing the loops
the obvious way — keeping a single `continue` for dirty ratings — gives:

```
facility-level mismatches: 134,104
rating-level mismatches  : 94,715
  facility[3].rating[0].crr3_irb_rwa : original=0.0 refactored=-99.0
```

That is the trap sub-task 7 warns about, and acceptance test 4 of that sub-task
is what catches it in review. `-99.0` is the sentinel the model seeds
`crr3_irb_rwa` with: the naive fusion never wrote the field at all, and a
zero-initialised stub would have hidden that behind a matching `0.0`.

**Second control — the `* 0.08` substitution the parent ticket forbids.**
Replacing every `/ RATIO_CAPITAL_TO_RWA` with `* 0.08`, which is the same number
in decimal, gives:

```
facility-level mismatches: 58,369
rating-level mismatches  : 94,536
  facility[2].crr3_irb_capital : original=1984.087765417187 refactored=1984.0877654171873
  facility[3].crr3_irb_secured_capital : original=581.3383818821881 refactored=581.3383818821882
```

One digit in the seventeenth significant figure, on 58,369 facilities. That is
the whole argument for the rule: `1/12.5` is exactly representable in binary
floating point and `0.08` is not, so the two differ in the last bits. On an
audited regulatory figure that is a defect, and it is exactly the kind of change
that looks free in review.

## What this does and does not prove

**Does:** the seven refactorings compose to something behaviour-preserving,
including the branches that are easy to get wrong — dirty ratings, zero EAD,
zero ratios, empty rating maps, both early returns.

**Does not:** replace the UAT non-regression run in sub-task 8. Synthetic data
and stub models are not production data and the real `Facility`. This says the
refactoring is sound in principle; sub-task 8 says it is sound on the real
population, and that is the one that gates the merge.
