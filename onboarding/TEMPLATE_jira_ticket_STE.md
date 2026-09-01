# Jira ticket template — STE / Simulator engines

Format reference extracted from `AERL_STE-844` (https://jira.group.echonet/browse/AERL_STE-844).

Use this as the standard structure for a functional/technical change ticket on the
`str_simulator_engine` / `str-bigData` platform.

---

## Section order (canonical)

| # | Section | Required | Purpose |
|---|---|---|---|
| 1 | **Context** | Yes | Current behaviour + numbered list of what must change |
| 2 | **Business Rules** | Yes | The functional contract, bullet form |
| 3 | **New Reference** (`Java`) | Yes | Target code, language-tagged block |
| 4 | **Code Snippet Scala** (`Scala`) | If applicable | Scala counterpart / change summary |
| 5 | **Acceptance Tests** | Yes | Numbered, each independently verifiable |
| 6 | **Impacts on Inputs/Outputs** | Yes | Contract changes visible to upstream/downstream |
| 7 | **Impacts on Data Quality** | Yes | Volume / population / null-rate changes |
| 8 | **Impacts on TWIST** | Yes | Test framework and monitoring updates |
| 9 | **Impacts on Data Dictionary** | Yes | Schema/field additions — "No changes" if none |
| 10 | **Configuration Example** (`Xml`) | If applicable | Workflow XML / HOCON fragment |

> The four `Impacts on ...` sections are mandatory even when the answer is "none".
> Writing "No changes to data dictionary as this is a configuration option" is a
> deliberate statement, not an omission.

---

## Blank template — copy/paste into Jira

```
**Context:** The <ClassName> class currently <describes current behaviour>. We need to:

1. <Change 1>
2. <Change 2>
3. <Change 3>

**Business Rules:**

* <Rule — what is added>
* Remove the {{<exact code expression to remove>}} constraint
* Maintain backward compatibility by <default behaviour when new option absent>
* Only apply <logic> when:
   * <Condition A>, OR
   * <Condition B (backward compatibility)>

**New Reference:**

{code:java}
<target code>
{code}

**Code Snippet Scala**

{code:scala}
<scala counterpart, or the ordered list of edits to apply>
{code}

**Acceptance Tests:**

1. Verify that <behaviour> is applied when <condition>
2. Verify that <behaviour> is applied when <no config> (backward compatibility)
3. Verify that <behaviour> is NOT applied when <negative condition>
4. Verify that <previously excluded population> is now processed
5. Verify that the step works correctly with all combinations of <options>

**Impacts on Inputs/Outputs:**

* <New optional config / new field>
* <Output population change>

**Impacts on Data Quality:**

* <Expected volume or content change>
* <Statement that this is expected, not a regression>

**Impacts on TWIST:**

* <Test case updates required>
* <Monitoring updates required>

**Impacts on Data Dictionary:**

* <Schema change, or "No changes to data dictionary as this is a configuration option">

**Configuration Example:**

{code:xml}
<workflow XML fragment showing the new option in place>
{code}
```

---

## Markdown variant (for briefs, PRs, handoff docs)

Same section order, standard fenced blocks:

````markdown
## Context

The `<ClassName>` class currently <current behaviour>. We need to:

1. <Change 1>
2. <Change 2>
3. <Change 3>

## Business Rules

- <Rule>
- Remove the `<exact expression>` constraint
- Maintain backward compatibility by <default>
- Only apply <logic> when:
  - <Condition A>, **OR**
  - <Condition B> (backward compatibility)

## New Reference

```java
<target code>
```

## Code Snippet Scala

```scala
<scala counterpart>
```

## Acceptance Tests

1. Verify that ...
2. Verify that ... (backward compatibility)
3. Verify that ... is **NOT** applied when ...
4. Verify that ...
5. Verify that ... with all combinations of ...

## Impacts on Inputs/Outputs

- ...

## Impacts on Data Quality

- ...

## Impacts on TWIST

- ...

## Impacts on Data Dictionary

- ...

## Configuration Example

```xml
<config fragment>
```
````

---

## Writing conventions observed in `AERL_STE-844`

**Context** — one sentence of current behaviour, then a numbered list of required changes.
Numbered, not bulleted: the items are ordered work, not an unordered set.

**Business Rules** — bulleted, functional voice. Exact code expressions are wrapped in
inline monospace so there is no ambiguity about what is being removed:

> Remove the `filter((FacilityInit fac) -> fac.getResidualMaturity() > 0)` constraint

Nested bullets are used for OR/AND conditions rather than prose.

**Backward compatibility is stated explicitly as a rule**, not left implicit. Where a new
option gates existing behaviour, the ticket says what happens when the option is absent.

**Acceptance Tests** — numbered, one assertion each, phrased as "Verify that ...".
Negative cases are included and the negation is capitalised (`NOT`) so it survives a
fast read. The last test always covers the combinatorial case.

**Impacts sections** — separated by concern rather than merged into one "Impacts" heading.
Each one has a named owner in practice (I/O → dev, Data Quality → BA, TWIST → QA,
Data Dictionary → data governance), which is why they stay split.

**"This is expected behavior"** — when a change intentionally alters output volume, the
ticket says so in *Impacts on Data Quality*. This pre-empts the non-regression run being
reported as a failure.

---

## Notes

- `TWIST` is the test/monitoring framework referenced in the Impacts section — keep the
  heading spelled exactly as-is.
- Jira renders `{{...}}` as inline monospace and `{code:java}...{code}` as a block. When
  a whole code block is pasted into `{{ }}` instead, Jira flattens the newlines — this
  happened in the *New Reference* section of AERL_STE-844 and made the snippet unreadable.
  **Use `{code:java}` blocks for anything multi-line.**
- Keep the language label line (`Java`, `Scala`, `Xml`) that Jira emits above each block.
