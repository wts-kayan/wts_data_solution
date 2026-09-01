# AERL_STE-844 — reference example

**URL:** https://jira.group.echonet/browse/AERL_STE-844
**Board:** Kanban Moteurs / Test Engines
**Target class:** `StepAgingPortfolio.java`

Transcribed verbatim from screenshots, used as the worked example for
`TEMPLATE_jira_ticket_STE.md`.

---

## Description

**Context:** The `StepAgingPortfolio` class currently applies aging logic unconditionally
when a `dateArrete` is present. We need to:

1. Add support for a new `"AGING-ENABLED"` subRunType to make this step optional
2. Remove the constraint that filters out facilities with `agingPtf <= 0`
3. Ensure the step only executes when explicitly enabled via the subRunType

**Business Rules:**

- Add new subRunType `"AGING-ENABLED"` to control when aging logic should be applied
- Remove the `filter((FacilityInit fac) -> fac.getResidualMaturity() > 0)` constraint
- Maintain backward compatibility by defaulting to enabled behavior if no subRunType is specified
- Only apply aging logic when:
  - The step is explicitly enabled via `"AGING-ENABLED"` subRunType, **OR**
  - No subRunType is specified (backward compatibility)

**New Reference:**

Java

```java
@Override
public Dataset<FacilityInit> launchInit(Dataset<FacilityInit> rdd) throws SparkException {
    // Check if step is enabled via subRunType
    boolean isEnabled = getRunType().contains("AGING-ENABLED") || getRunType().isEmpty();
    if (!isEnabled) {
        return rdd; // Return original dataset if not enabled
    }

    Dataset<FacilityInit> filterPtf = rdd.map(
            (MapFunction<FacilityInit, FacilityInit>) facility -> {
                return executeInit(facility);
            },
            Encoders.kryo(FacilityInit.class));

    return filterPtf; // Removed the residualMaturity > 0 filter
}
```

**Code Snippet Scala**

Scala

```scala
// Apply changes to StepAgingPortfolio.java
// 1. Add subRunType check in launchInit method
// 2. Remove the residualMaturity > 0 filter
```

**Acceptance Tests:**

1. Verify that aging logic is applied when `"AGING-ENABLED"` is in subRunType
2. Verify that aging logic is applied when no subRunType is specified (backward compatibility)
3. Verify that aging logic is **NOT** applied when `"AGING-ENABLED"` is NOT in subRunType
4. Verify that facilities with `residualMaturity <= 0` are now processed
5. Verify that the step works correctly with all combinations of subRunTypes

**Impacts on Inputs/Outputs:**

- New optional subRunType `"AGING-ENABLED"` added to configuration
- Output will now include aged facilities with `residualMaturity <= 0` (previously filtered out)

**Impacts on Data Quality:**

- May include additional facilities in output that were previously filtered
- This is expected behavior as we're removing the `residualMaturity` constraint

**Impacts on TWIST:**

- May require updates to test cases to account for new subRunType
- May require updates to monitoring to track aging-enabled runs

**Impacts on Data Dictionary:**

- No changes to data dictionary as this is a configuration option

**Configuration Example:**

Xml

```xml
<!-- content not visible in the source screenshots -->
```

---

## Transcription flags

1. **`New Reference` Java block was flattened in Jira.** The original was pasted inside
   `{{ }}` inline-code markers rather than a `{code:java}` block, so Jira collapsed the
   newlines — it renders as `{{@Overridepublic Dataset<FacilityInit> launchInit(...`,
   with `@Override` and `public` fused, and `// Check if step is enabled via subRunType`
   fused into `boolean isEnabled`. The block above is a **reconstruction** into valid,
   readable Java. Semantics are unchanged, but formatting is inferred, not verbatim.

2. **`Code Snippet Scala` contains Java comments, not Scala.** The section is labelled
   Scala and tagged `Scala`, but the body is three `//` comment lines describing edits to
   `StepAgingPortfolio.java`. Either the section is mislabelled or the Scala counterpart
   was never filled in. Worth clarifying with the author before treating it as a spec.

3. **Terminology inconsistency between Context and Business Rules.** Context item 2 says
   *"facilities with `agingPtf <= 0`"*; the Business Rule and acceptance test 4 both say
   `residualMaturity`. These are presumably the same constraint under two names, but the
   ticket never reconciles them. Confirm which field the filter actually reads.

4. **`Configuration Example` not captured.** The Xml block was below the fold in both
   screenshots. Retrieve it from Jira before using this ticket as a full spec.

5. **`getRunType()` vs "subRunType".** The prose consistently says *subRunType*, but the
   code calls `getRunType()`. Check whether `getRunType()` returns the sub-run types or
   whether a distinct accessor exists.
