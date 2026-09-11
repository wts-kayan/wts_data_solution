# flair-LBO — `String is not a valid external type for schema of timestamp`

**Module:** `str_flair` / branch `develop`
**Package:** `com.bnpparibas.itg.fresh.str.lbo`
**Affected column:** `ti_lbo_counterparty_rating_date` (ordinal **77**, exprId `#644`)
**Status:** Root cause identified — `Flair.scala:38-39`
**Extraction date:** 2026-09-11

> Redacted for a public repository: IDE blame annotations appear as
> `<author-A/B/C>`, the driver hostname as `<driver-host>`, and the input file as
> `<counterparty-input>.csv`. Column names, ordinals, types and traces are
> verbatim — the diagnosis depends on them.

---

## 1. Summary

`Flair.process` rebuilds the `cptr` DataFrame by pairing an existing `RDD[Row]`
with a schema derived by reflection from the `Counterparties` case class.
`spark.createDataFrame(rdd, schema)` performs **no conversion** — it relabels the
rows and trusts the caller. The rows still hold the raw `java.lang.String` values
produced by `IO.readCsv`, while the reflected schema declares ordinal 77 as
`TimestampType`.

The mismatch is invisible until an action forces the `ExpressionEncoder`
serializer to materialise the rows (here, a cached/persisted relation), at which
point `validateexternaltype` throws.

**This is not a Spark 3.5.4 regression.** The same defect reproduces on the
production cluster's older Spark (see §5.3). The upgrade only changed *which*
external type the encoder demanded.

---

## 2. Environment

| | Local | Production cluster |
|---|---|---|
| Spark | 3.5.4 | older (see §5.3 line numbers) |
| `ExpressionEncoder.scala` frame | `:214` | `:207` |
| `QueryExecutionErrors.scala` frame | `:1416` | `:1236` |
| `spark.sql.datetime.java8API.enabled` | observed **both** `true` and `false` across runs | `false` |
| Scala | 2.12.18 | — |
| IDE | IntelliJ IDEA, run config `flair-LBO` | — |

> **⚠ Environment divergence (A-03).** The `java8API` flag is not pinned and
> differs between runs and environments. See §7.

---

## 3. Extracted source

### 3.1 `IO.scala` — `object IO`

`str_flair/src/main/com/bnpparibas/itg/fresh/str/spark/IO.scala`
Authorship annotation: `<author-A> +3` (object), `<author-A> +1` (both readers).

```scala
object IO {

  def readCsvAsDataset[E <: Product : TypeTag](path: String, hasHeader: Boolean, canBeEmpty: Boolean = false, delimeter: String = ";"): Dataset[E] = {
    spark.read
      .format("csv")
      .option("delimiter", delimeter)
      .option("header", hasHeader)      // Required
      .schema(schema)
      .load(path)
      .as[E](Encoders.product[E])
  }

  def readCsv(path: String, hasHeader: Boolean = true, canBeEmpty: Boolean = false, delimeter: String = ";", inferSchema: Boolean = true): DataFrame = {
    val spark = SparkSession.getActiveSession.get

    if (canBeEmpty && path.isEmpty) {
      spark.emptyDataFrame
    } else {
      spark.read
        .format("csv")
        .option("delimiter", delimeter)
        .option("header", hasHeader)      // Required
        .option("inferSchema", inferSchema.toString) Choose schema
        .load(path)
    }
  }
```

> **B-01** — `delimeter` (parameter, both methods) vs `"delimiter"` (option key).
> Misspelled parameter name is load-bearing; transcribed as-is. IDE spell-check
> underlines it in the screenshot, confirming the read.
> **B-02** — `Choose schema` appears without `//` on the `inferSchema` line.
> Likely an IDE inlay hint rather than source text, but transcribed as seen.
> **C-01** — ~~resolved~~. `inferSchema: Boolean = true` is the fifth parameter
> of `readCsv`; the earlier screenshot was truncated at the right edge.
> **C-03** — `readCsvAsDataset` references a bare `schema` in `.schema(schema)`
> that is not visible in the screenshot and is not a parameter. Either an
> enclosing-scope value or a line hidden between the signature and `spark.read`.
> **C-04** — `readCsvAsDataset` declares `canBeEmpty` but does not appear to use
> it, unlike `readCsv`. Unverified — the visible body has no branch on it.
> **C-05** — `readCsvAsDataset` uses a bare `spark` while `readCsv` resolves
> `SparkSession.getActiveSession.get` locally. Two different session-acquisition
> idioms in the same object.

#### ★ The correct pattern already exists in this file

`readCsvAsDataset` is exactly the right approach: it applies an explicit
`.schema(...)` **to the CSV reader**, so the parser converts text to typed values
during parsing, then attaches a matching product encoder. No `Row` ever carries a
`String` in a slot the schema calls `timestamp`.

`Flair.process` does not use it. It calls `readCsv` (all-inferred / all-string for
column 77) and then bolts the typed schema on afterwards via
`createDataFrame(rdd, schema)` — which converts nothing. See §6.2 option B.

#### Open question — why is ordinal 77 `StringType`?

`readCsv` defaults to `inferSchema = true`, and `Flair` calls it with defaults,
yet the diagnostic (§4) prints `StringType` for `ti_lbo_counterparty_rating_date`.
So CSV inference examined the column and declined to make it a timestamp. That
implies at least one value in the full file does not parse under the default
timestamp format — the two-row sample is not representative.

**This matters for the fix.** `cast(TimestampType)` will return `null` for
whatever value defeated inference, silently. Identify it before merging:

```scala
val raw = IO.readCsv(props.counterpartFilePath)
raw.select($"ti_lbo_counterparty_rating_date")
   .filter($"ti_lbo_counterparty_rating_date".isNotNull &&
           to_timestamp($"ti_lbo_counterparty_rating_date").isNull)
   .distinct()
   .show(50, truncate = false)
```

Anything this returns is a value that will become `null` after the fix. Apply the
same probe to the seven other timestamp ordinals in A-08.


### 3.2 `IO.scala` — `stringSchemaForTable`

```scala
  def stringSchemaForTable(data: Dataset[_]): String = {
    data.schema.map {
      case StructField(name, StringType, _, _)    => s"$name String"
      case StructField(name, DoubleType, _, _)    => s"$name Double"
      case StructField(name, IntegerType, _, _)   => s"$name Int"
      case StructField(name, TimestampType, _, _) => s"$name timestamp"
      case default => throw new IllegalArgumentException("datatype not handled is not handled " + c /* ⟪CUT⟫ */
    }.mkString(", ")
  }
```

> **A-01 (preserved bug)** — exception message reads
> `"datatype not handled is not handled "` — phrase duplicated. Preserved verbatim.
> **A-02 (latent defect)** — the match is non-exhaustive. Missing:
> `TimestampNTZType`, `DateType`, `LongType`, `BooleanType`, `DecimalType`,
> `NullType`. Spark 3.4+ can infer `TimestampNTZType` from CSV when
> `spark.sql.timestampType=TIMESTAMP_NTZ`, which would hit `case default`.
> Not the current failure, but a live risk post-upgrade.

Authorship annotation in IDE: `<author-A> +3` (object `IO`),
`<author-A>` (`stringSchemaForTable`).

### 3.3 `Flair.scala` — `object Flair`

```scala
object Flair extends AbstractFlair[LboProperties] {
  override def process(props: LboProperties): Unit = {
    /**
     * LOAD INPUT
     */
    val cptrList      = IO.readCsv(props.counterpartFilePath)
    println(cptrList.schema.fields(77))                                    // ← diagnostic, added during investigation
    val p             = IO.readCsv(props.coeffMultiPFilePath)
    val pd            = IO.readCsv(props.scorePDFilePath, props.scorePDFileHasHeader)
    val ratingWithPd  = IO.readCsv(props.ratingWithPd)
    var shocks        = IO.readCsv(props.shockFilePath, props.shockFileHasHeader, canBeEmpty = true)

    /**
     * COMPUTE
     */
    /* ⟪CUT — lines 31-33 not visible⟫ */

    val ratingWithPdBound = ScoreWithPdBound.computeRatingWithPdBoundDs(ratingWithPd)

    var cptr = ScoreComputation.removeCurrency(cptrList)

    val encoderSchema = ScalaReflection.schemaFor[Counterparties].dataType.asInstanceOf[StructType]   // ← line 38  ★ ROOT CAUSE
    cptr = spark.createDataFrame(cptr.rdd, encoderSchema)                                             // ← line 39  ★ ROOT CAUSE

    val stressedCptr = renameColumns(shockCounterparties(shocks, props, "RMPM_ID")(cptr))

    val rating = ScoreComputation.addScore(stressedCptr, p, scoreWithPdBound, ratingWithPdBound)

    /**
     * ⟪CUT — remainder of process() not visible⟫
     */
  }
}
```

Authorship annotation: `<author-B> +3`.

IDE inlay hints captured at line 34 and 36:
- `ratingWithPd: [Frontiere: str…` ⟪truncated⟫
- `cptrList: [rmpm_id: string, ctp_company_name: string ... 7…` ⟪truncated⟫

> **C-02** — line 43 references `scoreWithPdBound`, which is not among the
> `val`s visible in the extracted range. Presumably defined in the cut region
> (lines 31-33).

### 3.4 `Counterparties.scala` — the reflected target schema

`str_flair/src/main/com/bnpparibas/itg/fresh/str/lbo/score/Counterparties.scala`
Authorship annotation: `<author-C> +3`.

```scala
package com.bnpparibas.itg.fresh.str.lbo.score

import java.sql.Timestamp

case class Counterparties (

                            rmpm_id : String,                                    // ordinal  0
                            ctp_company_name: String,                            //  1
                            ti_model_cd_pays_bus: String,                        //  2
                            ti_py_rtg_cr: String,                                //  3
                            business_group_code: String,                         //  4
                            business_group_name: String,                         //  5
                            arch_wkf_current_status_token: String,               //  6
                            code_psn: String,                                    //  7
                            validation_date: Option[Timestamp],                  //  8  ⚠ TS
                            client_type: String,                                 //  9
                            intrinsic_rating_calculated: String,                 // 10
                            intrinsic_rating_proposed: String,                   // 11
                            counterparty_rating_proposed: String,                // 12
                            CounterpartyRatingRatified: String,                  // 13  ⚠ A-05
                            counterparty_rating_ratified: String,                // 14
                            credit_decision_date: Option[Timestamp],             // 15  ⚠ TS
                            managing_site: Option[Integer],                      // 16  ⚠ A-06
                            managing_site_name: String,                          // 17
                            site_pole_code: String,                              // 18
                            client_coverage: String,                             // 19
                            arch_wkf_modification_date: Option[Timestamp],       // 20  ⚠ TS
                            deferral_cc_date: Option[Timestamp],                 // 21  ⚠ TS
                            deferral_date: Option[Timestamp],                    // 22  ⚠ TS
                            ti_lbo_ir_indvol: Option[Double],                    // 23
                            ti_lbo_ir_indvol_cap: String,                        // 24
                            ti_lbo_ir_techrisk: String,                          // 25
                            ti_lbo_ir_barriers: String,                          // 26
                            ti_lbo_fin_date: Option[Timestamp],                  // 27  ⚠ TS
                            ti_lbo_fin_unit: String,                             // 28
                            ti_lbo_fin_label: String,                            // 29
                            ti_lbo_ir_marketpos: String,                         // 30
                            ti_lbo_ir_sales: Option[Double],                     // 31
                            ti_lbo_fin_sales: Option[Double],                    // 32
                            ti_lbo_fin_sales_cap: String,                        // 33
                            ti_lbo_ir_client_concentration: Option[Double],      // 34
                            ti_lbo_ir_client_concentration_cap: String,          // 35
                            ti_lbo_ir_margin_risk: String,                       // 36
                            ti_lbo_ir_competitive_adv: String,                   // 37
                            ti_lbo_ir_track_record_exp: String,                  // 38
                            ti_lbo_ir_mgt_commitment: String,                    // 39
                            ti_lbo_ir_track_record_cap: String,                  // 40
                            ti_lbo_ir_spr_commitment: Option[Double],            // 41
                            ti_lbo_ir_spr_commitment_cap: String,                // 42
                            ti_lbo_ir_grossdebt: Option[Double],                 // 43
                            ti_lbo_ir_cash: Option[Double],                      // 44
                            ti_lbo_ir_net_debt: Option[Double],                  // 45
                            ti_lbo_ir_ebitda: Option[Double],                    // 46
                            ti_lbo_ir_net_debt_to_ebitda: Option[Double],        // 47
                            ti_lbo_ir_net_debt_to_ebitda_cap: String,            // 48
                            ti_lbo_ir_taxes: Option[Double],                     // 49
                            ti_lbo_ir_cash_interest: Option[Double],             // 50
                            ti_lbo_ir_avg_capex: Option[Double],                 // 51
                            ti_lbo_ir_net_debt_to_eti_ccm: Option[Double],       // 52  ⚠ A-07
                            ti_lbo_ir_net_debt_to_eticcm_cap: String,            // 53  ⚠ A-07
                            ti_lbo_ir_ebitda_to_cash_interest: Option[Double],   // 54
                            ti_lbo_ir_ebitda_to_cash_interest_cap: String,       // 55
                            ti_lbo_ir_obs_liabilities: String,                   // 56
                            ti_lbo_ir_enterprise_value_to_ebitda: Option[Double],// 57  ⚠ A-07
                            ti_lbo_ir_closing_date: Option[Timestamp],           // 58  ⚠ TS
                            ti_lbo_ir_enterprisevalue: Option[Double],           // 59  ⚠ A-07
                            ti_lbo_ir_e_value_to_net_debt: Option[Double],       // 60  ⚠ A-07
                            ti_lbo_ir_e_valuetonetdebt_cap: String,              // 61  ⚠ A-07
                            ti_lbo_ir_warsd: Option[Double],                     // 62
                            ti_lbo_ir_warsd_cap: String,                         // 63
                            ti_lbo_ir_capexplan: Option[Double],                 // 64
                            ti_lbo_ir_debt_service_12m: Option[Double],          // 65
                            ti_lbo_ir_dscr: Option[Double],                      // 66
                            ti_lbo_ir_dscr_cap: String,                          // 67
                            ti_lbo_ir_committed_undrawn: Option[Double],         // 68
                            ti_lbo_ir_external_liquidity: Option[Double],        // 69
                            ti_lbo_ir_external_liquidity_cap: String,            // 70
                            ti_lbo_ir_model_rating: String,                      // 71
                            ti_lbo_ir_expert_rating: String,                     // 72
                            ti_lbo_ir_override_comment: String,                  // 73
                            ti_lbo_ir_override_justif1: String,                  // 74
                            ti_lbo_ir_override_justif2: String,                  // 75
                            ti_lbo_counterparty_rating: String,                  // 76
                            ti_lbo_counterparty_rating_date: Option[Timestamp]   // 77  ★ THE FAILING FIELD
                          )
```

> Ordinal comments added during extraction; they are **not** in the source.
> Source line numbers 8-85 map to ordinals 0-77 (78 fields total).

#### Ordinal 77 confirmed

`ti_lbo_counterparty_rating_date` sits at source line 85 → ordinal **77**, matching
`getexternalrowfield(..., 77, ti_lbo_counterparty_rating_date)` in every stack
trace. `Option[Timestamp]` with `import java.sql.Timestamp` is what
`ScalaReflection.schemaFor[Counterparties]` turns into a nullable `TimestampType`.
The chain is now fully closed.

#### Scope is wider than one column

**A-08 — the defect affects 8 timestamp fields and 26 `Option[Double]` fields, not one.**

Timestamp ordinals: **8, 15, 20, 21, 22, 27, 58, 77**.

The encoder writes fields in order and throws on the *first* type violation it
reaches. In the test row, ordinals 8-58 are empty (`;;;;` in the CSV), so
`isNullAt` short-circuits each of them and the failure surfaces at 77 — the first
timestamp carrying an actual value. **On production data where earlier timestamps
are populated, the trace will name a lower ordinal.** Do not treat a different
ordinal in a future trace as a different bug.

The same reasoning applies to every `Option[Double]` field: a `String` is not a
valid external type for `double` either. They are currently masked by nulls.

This makes the cast-based `conform` fix in §6 mandatory rather than optional —
a per-column patch on ordinal 77 would leave 33 latent failures behind.

---

## 4. Input data

File: `<counterparty-input>.csv`
Delimiter: `;`

Header tail (line 1), as visible:

```
…_cap;ti_lbo_ir_committed_undrawn;ti_lbo_ir_external_liquidity;ti_lbo_ir…
```

Data row (line 2) — **two formats were tested**:

```
…oor;;;;7+.Poor;2019-10-24T00:00:00.000Z      ← variant A (ISO-8601 with zone)
…oor;;;;7+.Poor;2024-07-30 00:00:00           ← variant B (local, no zone)
```

Both variants fail identically. The file format is **not** the cause.

### Column-position cross-check (IntelliJ CSV table preview)

Preview taken with the header line **not** designated as a header, so IntelliJ
labels columns generically `C0…C77` and shows the header text as the first row.

| Col | Header text (row 1) | Sample values |
|---|---|---|
| C70 | `ti_lbo_ir_external_liqu…` | `capped at 1.35`, `<null>`, `<null>`, `<null>`, `<null>` |
| C71 | `ti_lbo_ir_model_rating` | `7+.Poor`, `8+.Weak`, `7+.Poor`, `7-.Poor`, `7-.Poor` |
| C72 | `ti_lbo_ir_expert_rating` | `7+.Poor`, `8+.Weak`, `7+.Poor`, `7+.Poor`, `7-.Poor` |
| **C77** | **`ti_lbo_counterparty_rating_dat…`** | `2024-07-30 00:00:00`, `2023-10-02 00:00:00`, `2020-04-10 00:00:00`, `2022-03-16 00:00:00`, `2021-09-22 00:00:00` |

**Ordinal 77 independently confirmed a third time.** The CSV physical column index
(C77), the case-class field position (§3.4), and the `getexternalrowfield(..., 77, ...)`
in every stack trace all agree.

Observations:

- All values use `yyyy-MM-dd HH:mm:ss` with a zero time component. The column is
  date-valued but typed as a timestamp. `DateType` would be the honest type;
  changing it is out of scope here (regulatory output equivalence).
- Real dates span 2020-2024, so the column is genuinely populated across rows —
  not an edge case confined to one record.
- C70 shows `<null>` for most rows and `capped at 1.35` for one — free-text
  content in a `_cap` column. Consistent with `ti_lbo_ir_external_liquidity_cap:
  String` in the case class.

#### ★ C-06 candidate: the trailing `<unset>` row

The final row of the preview shows **`<unset>` in every visible column**,
including C77.

In the IntelliJ CSV editor `<unset>` and `<null>` mean different things:
`<null>` is a present-but-empty field, `<unset>` is a field that **does not exist
in that record** — the row has fewer delimiters than the header. So the file ends
with a short/ragged record, most likely a stray trailing newline or a truncated
final line.

This is the leading explanation for **C-06** (why `inferSchema = true` still
yielded `StringType` for ordinal 77). Confirm with:

```bash
tail -c 200 <counterpart_file> | xxd | tail -5     # trailing bytes / newline
awk -F';' 'NF != 78 {print NR": "NF" fields"}' <counterpart_file>
```

Any line reported by the `awk` is a ragged record. If the only offender is a
trailing empty line, the fix is trivial and inference should recover. If a real
data line is short, that is a data-quality defect to raise with the file producer
before any code change — and note that `readCsvAsDataset` (Option B, §6.2) is
stricter about ragged rows than the inferring `readCsv`.

> **C-07** — Text partially visible at the right edge of the screenshot
> (`sk. We expe…`, `20-12-30 00:`, `ting (12/08/2…`, `qu'en cas de`) could not be
> read reliably. Appears to be a French/English comment or documentation pane.
> Not transcribed.

### Diagnostic output

**Run 1 — 2026-09-11 ~12:59, input variant A (`2019-10-24T00:00:00.000Z`)**

```
StructField(ti_lbo_counterparty_rating_date,StringType,true)
```

**Run 2 — 2026-09-11 14:15:46, input variant B (`2024-07-30 00:00:00`)**

```
StructField(ti_lbo_counterparty_rating_date,TimestampType,true)
```

#### ★ C-06 resolved — and §2's format claim corrected

CSV schema inference **is** format-sensitive here, and in the opposite direction
to what was assumed earlier in this document:

| Input format | Inferred type for ordinal 77 |
|---|---|
| `2019-10-24T00:00:00.000Z` (ISO-8601 with zone) | `StringType` |
| `2024-07-30 00:00:00` (local, no zone) | `TimestampType` |

The trailing-`<unset>`/ragged-row hypothesis in §4 is therefore **not** the
explanation for run 1. Retained as a data-quality item worth checking, demoted
from cause to observation.

> **Correction.** §2 states "the file format is not the cause" and the earlier
> analysis asserted that the `Z` form parses cleanly under default inference.
> Empirically the `Z` form is what *defeats* inference in this environment. The
> format does change the inferred schema. It does **not** fix the defect — see
> below.

#### The defect survives correct inference — still a timestamp failure

Ordinal 77 now matches `encoderSchema`, but the job fails at the same point with
the **same** error. Top-level output from this run (14:15:55,569):

```
2026-09-11 14:15:55,569 [task-result-getter-2] ERROR org.apache.spark.scheduler.TaskSetManager:76 - Task 0 in stage 14.0 failed 1 times; aborting job
Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 14.0 failed 1 times, most recent failure:
Lost task 0.0 in stage 14.0 (TID 14) (<driver-host> executor driver): org.apache.spark.SparkRuntimeException: Error while encoding:
 java.lang.RuntimeException: java.lang.String is not a valid external type for schema of timestamp
```

> **Correction to an earlier reading in this document.** The six
> `staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType,
> fromString, …)` lines visible in the run-2 console were briefly interpreted as a
> new, mirror-image `Double`-in-`String` failure. That was wrong. `Error while
> encoding:` dumps the serializer expression for **every field in the schema**,
> not just the failing one, so those `UTF8String` lines are simply the `String`
> columns being listed. The failure is, and remains, `String` in a `timestamp`
> slot.

**What this actually confirms is A-08.** With ordinal 77 fixed by inference, the
encoder now reaches a *different* timestamp field and fails there instead. Seven
other candidates exist — ordinals 8, 15, 20, 21, 22, 27, 58 — and inference did
not type them as `TimestampType`, either because they are empty in the sample
(so inference defaults to `StringType`) or because their values use a format
inference rejects.

This is precisely the behaviour predicted in §3.4: *"on production data where
earlier timestamps are populated, the trace will name a lower ordinal."*

**Consequence for the fix.** Chasing this per-column, or by adjusting the input
format, is futile — each fix simply promotes the next timestamp field to being
the failure. Inference derives types from data; `Counterparties` fixes them by
declaration; the two will not converge. Only an explicit whole-schema conversion
— `conform` (Option A) or the typed reader (Option B) — closes the gap in one
move.

**Next diagnostic step:** capture the ordinal named in this run's
`getexternalrowfield(..., N, <field>)` line. That identifies which timestamp
field is now first to fail and gives a second confirmed instance of A-08.

> Evidence only — it is **not** a prerequisite for the fix. The change in §6.1
> converts every column in one projection, so the identity of the next failing
> ordinal no longer gates anything.

#### Companion warnings, same run

```
2026-09-11 14:15:53,348 [main] WARN  org.apache.spark.sql.catalyst.util.SparkStringUtils:72 - Truncated the string representation of a plan since it was too large. This ⟪CUT⟫
2026-09-11 14:15:55,537 [Executor task launch worker for task 0.0 in stage 14.0 (TID 14)] WARN  org.apache.spark.storage.BlockManager:72 - Putting block rdd_79_0 failed ⟪CUT⟫
```

- `SparkStringUtils` plan truncation: consistent with 78 columns plus the deep
  lineage of A-04.
- `BlockManager … rdd_79_0 failed`: the cache write aborting as the encoder
  throws — the same `.cache()` masking the origin, noted in §5.1.

#### Case-class cross-check

The same screenshot shows `Counterparties.scala` lines 64-68, which match the
§3.4 extraction exactly:

```scala
ti_lbo_ir_obs_liabilities: String,                    // 64 → ordinal 56
ti_lbo_ir_enterprise_value_to_ebitda: Option[Double], // 65 → ordinal 57
ti_lbo_ir_closing_date: Option[Timestamp],            // 66 → ordinal 58
ti_lbo_ir_enterprisevalue: Option[Double],            // 67 → ordinal 59
ti_lbo_ir_e_value_to_net_debt: Option[Double],        // 68 → ordinal 60
```

Line-to-ordinal offset of 8 confirmed independently.

---

## 5. Error traces

### 5.1 Local, Spark 3.5.4, `java8API = true`

```
if (assertnotnull(input[0, org.apache.spark.sql.Row, true]).isNullAt) null else staticinvoke(class org.apache.spark.sql.catalyst.util.DateTimeUtils$, TimestampType,
  instantToMicros, validateexternaltype(getexternalrowfield(assertnotnull(input[0, org.apache.spark.sql.Row, true]), 77, ti_lbo_counterparty_rating_date), TimestampType,
  ObjectType(class java.time.Instant)), true, false, true) AS ti_lbo_counterparty_rating_date#644.
    at org.apache.spark.sql.errors.QueryExecutionErrors$.expressionEncodingError(QueryExecutionErrors.scala:1416)
    at org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$Serializer.apply(ExpressionEncoder.scala:217)
    at org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$Serializer.apply(ExpressionEncoder.scala:200)
    at scala.collection.Iterator$$anon$10.next(Iterator.scala:461)
    …
    at org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer$$anon$1.hasNext(InMemoryRelation.scala:119)
    at org.apache.spark.sql.execution.columnar.CachedRDDBuilder$$anon$2.hasNext(InMemoryRelation.scala:288)
    at org.apache.spark.storage.MemoryStore.putIterator(MemoryStore.scala:223)
    at org.apache.spark.storage.MemoryStore.putIteratorAsValues(MemoryStore.scala:302)
    at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1597)
    at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1524)
    at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1588)
    at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1389)
    at org.apache.spark.storage.BlockManager.getOrElseUpdateRDDBlock(BlockManager.scala:1343)
    at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:379)
    at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
    at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
    at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
    at org.apache.spark.scheduler.Task.run(Task.scala:141)
    at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
    at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
    at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
    at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
    at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    at java.lang.Thread.run(Thread.java:750)
Caused by: java.lang.RuntimeException: java.lang.String is not a valid external type for schema of timestamp
    at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.If_8$(Unknown Source)
    at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.writeFields_0_1$(Unknown Source)
    at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.apply(Unknown Source)
    at org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$Serializer.apply(ExpressionEncoder.scala:214)
```

### 5.2 Local, Spark 3.5.4, `java8API = false`

Identical except:

```
  fromJavaTimestamp, validateexternaltype(…, TimestampType, ObjectType(class java.sql.Timestamp))
…
Caused by: java.lang.RuntimeException: java.lang.String is not a valid external type for schema of timestamp
    at …GeneratedClass$SpecificUnsafeProjection.If_21$(Unknown Source)
    at …GeneratedClass$SpecificUnsafeProjection.writeFields_0_3$(Unknown Source)
    at …ExpressionEncoder$Serializer.apply(ExpressionEncoder.scala:214)
    ... 34 more
```

### 5.3 Production cluster (older Spark)

```
if (assertnotnull(input[0, org.apache.spark.sql.Row, true]).isNullAt) null else staticinvoke(class org.apache.spark.sql.catalyst.util.DateTimeUtils$,
  TimestampType, fromJavaTimestamp, validateexternaltype(getexternalrowfield(assertnotnull(input[0, org.apache.spark.sql.Row, true]), 77,
  ti_lbo_counterparty_rating_date), TimestampType, false), true, false, true) AS ti_lbo_counterparty_rating_date#644
    at org.apache.spark.sql.errors.QueryExecutionErrors$.expressionEncodingError(QueryExecutionErrors.scala:1236)
    at org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$Serializer.apply(ExpressionEncoder.scala:210)
    at org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$Serializer.apply(ExpressionEncoder.scala:193)
    at scala.collection.Iterator$$anon$10.next(Iterator.scala:461)
    at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
    at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
    at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:760)
    at org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer$$anon$1.next(InMemoryRelation.scala:87)
    at org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer$$anon$1.next(InMemoryRelation.scala:79)
    at scala.collection.Iterator$$anon$10.next(Iterator.scala:461)
    at org.apache.spark.storage.MemoryStore.putIterator(MemoryStore.scala:224)
    at org.apache.spark.storage.MemoryStore.putIteratorAsValues(MemoryStore.scala:302)
    at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1523)
    at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1450)
    at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1514)
    at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1337)
    at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:376)
    at org.apache.spark.rdd.RDD.iterator(RDD.scala:327)
    ⟪ ~25 alternating frames: MapPartitionsRDD.compute(MapPartitionsRDD.scala:62)
                            / RDD.computeOrReadCheckpoint(RDD.scala:365)
                            / RDD.iterator(RDD.scala:329) ⟫
    at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
    at org.apache.spark.scheduler.Task.run(Task.scala:136)
    at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:551)
    at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1505)
    at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:554)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    at java.lang.Thread.run(Thread.java:748)
Caused by: java.lang.RuntimeException: java.lang.String is not a valid external type for schema of timestamp
    at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.StaticInvoke_20$(Unknown Source)
    at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.writeFields_0_10$(Unknown Source)
    at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.apply(Unknown Source)
    at org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$Serializer.apply(ExpressionEncoder.scala:207)
    ... 57 more
```

> **A-04 (latent)** — the ~25 nested `MapPartitionsRDD.compute` /
> `computeOrReadCheckpoint` frames indicate very deep RDD lineage, consistent
> with a per-scenario derivation loop. Independent of this defect; costs driver
> memory and stage-planning time. Separate ticket.

---

## 6. Fix

### 6.1 The change — `Flair.scala:38-39`

Line 38 is unchanged. Line 39 is replaced by two lines.

```scala
val encoderSchema = ScalaReflection.schemaFor[Counterparties].dataType.asInstanceOf[StructType]   // line 38 — unchanged

// BEFORE — relabels Rows, converts nothing; the lie surfaces at encoding time
cptr = spark.createDataFrame(cptr.rdd, encoderSchema)

// AFTER — same column mapping, values actually converted inside Catalyst
cptr = cptr.toDF(encoderSchema.fieldNames: _*)
           .select(encoderSchema.fields.map(f => col(f.name).cast(f.dataType).as(f.name)): _*)
```

The only other edit is `import org.apache.spark.sql.functions.col`, if
`Flair.scala` does not already carry it.

**Nothing else changes.** No new class or file, no change to `IO.scala`,
`ScoreComputation.removeCurrency`, `renameColumns`, `shockCounterparties`, or any
downstream call. `cptr` stays a `DataFrame`, so the `var` and every later
statement are untouched.

### 6.2 Why this is sufficient

- **It closes A-08 in one move.** Every column is converted to its declared type
  in the same projection, so all 8 timestamp ordinals — and any other column
  where the inferred type differs from the declared one — are handled together.
  The "next diagnostic step" in §4 (identify which timestamp field fails after
  77) is no longer needed to produce a fix; it remains useful only as evidence.
- **It ends the format sensitivity documented in §4.** `cast(TimestampType)`
  parses both `2019-10-24T00:00:00.000Z` and `2024-07-30 00:00:00`, so the run-1
  / run-2 divergence disappears and the input format stops changing the outcome.
- **It removes the `.rdd` round-trip**, which forced a `DataFrame → RDD →
  DataFrame` boundary and defeated Catalyst optimisation across that point.

### 6.3 Why this is safe against the equivalence requirement

- **`toDF` reproduces `createDataFrame`'s column mapping exactly** — both rename
  positionally. That mapping is independently confirmed for this input: the Row's
  ordinal 77 is `ti_lbo_counterparty_rating_date` and the CSV's physical C77 is
  the same column (§4), so positions survive `removeCurrency`.
- **`toDF` is its own arity guard.** A wrong column count throws
  `IllegalArgumentException: The number of columns doesn't match` on the driver,
  before any task runs. The current code has no such check — it builds the
  mismatched schema and fails later, inside an executor.
- **Identity casts cost nothing and change nothing.** For every column where
  inference already produced the declared type, Catalyst eliminates the cast
  (`SimplifyCasts`). Those columns are untouched, which is what bit-identical
  output requires.
- **The residual risk is a cast that returns `null` instead of failing.** That is
  what the pre-merge check in §8.1 exists to rule out, and it is the one outcome
  worse than today's crash.

### 6.4 Explicitly not done in this change

| Rejected | Why |
|---|---|
| Changing the input file format | Run 2 (§4) proves it only promotes the next timestamp field to failing |
| Patching ordinal 77 alone | Same reason — A-08 / A-09 |
| Removing the `.cache()` | `BlockManager … rdd_79_0 failed` is the cache aborting *as* the encoder throws, not the cause. Removing it relocates the failure or lets a lying schema through |
| Fixing A-01 / A-02 in `stringSchemaForTable` | Real, but a different file and not on this failure path |

### 6.5 Deferred — the structural fix

`IO.readCsvAsDataset[Counterparties]` is the right target state (§3.1): it pushes
the schema into the CSV parser, so conversion happens at parse time and the swap
at lines 38-39 disappears entirely.

```scala
val cptrList = IO.readCsvAsDataset[Counterparties](props.counterpartFilePath, hasHeader = true)
…
var cptr = ScoreComputation.removeCurrency(cptrList)
// lines 38-39 deleted
```

Not in this change, because it carries four open items:

- **C-03 is unresolved** — `.schema(schema)` references a bare `schema` that is
  neither a parameter nor visible. Until that is read, the method may not do what
  its name implies.
- `ScoreComputation.removeCurrency` must accept a `Dataset[Counterparties]`, or
  the result must be re-typed.
- An explicit `.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")` is required.
  With a declared schema the parser uses that option; `UnivocityParser` does have
  a lenient fallback for `TimestampType`, but "probably parses" is not a basis
  for regulatory output.
- Parse-time conversion is stricter than `cast`: a value `cast` turns into `null`
  may instead fail the read — arguably correct, but a behaviour change.

It also removes the whole class of defect rather than this instance of it, since
`inferSchema` never gets a vote. Separate ticket.

### 6.6 Other call sites

`p`, `pd`, `ratingWithPd`, and `shocks` all originate from the same `IO.readCsv`
and are therefore inference-typed. Any similar reflection-schema swap applied to
them has the same defect. Audit with:

```bash
grep -rn "createDataFrame" src/main/scala/com/bnpparibas/itg/fresh/str/
grep -rn "ScalaReflection.schemaFor" src/main/scala/com/bnpparibas/itg/fresh/str/
```

---

## 7. Required configuration hardening

```scala
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

Mandatory: input variant B (`2024-07-30 00:00:00`) carries no zone offset and is
therefore interpreted in the session timezone. A driver defaulting to
`Europe/Paris` and a UTC-configured cluster node will produce **different stored
instants from the same file, silently, with no error**.

Pin `spark.sql.datetime.java8API.enabled` explicitly and identically across local
and cluster. It was observed as `true` in one local run and `false` in another
and in production (§5.1 vs §5.2/5.3).

---

## 8. Regression control

Regulatory output equivalence applies.

### 8.1 Pre-merge check — run once, then delete

> **Supersedes the null-rate parity test** carried in earlier revisions of this
> document. That test compared a "before" run against an "after" run, but the
> before-run does not complete — it throws at the encoder, so the column never
> materialises and there is no before-count to compare. The baseline has to come
> from the source strings themselves.

Insert immediately above the line-38/39 block, run once against the real file,
read both outputs, then remove before committing. Needs
`import org.apache.spark.sql.functions.{col, trim}` and
`import org.apache.spark.sql.types.{StringType, TimestampType}`.

```scala
// TEMPORARY — pre-merge verification, delete before commit

// (a) the actual conversion set: every ordinal where inferred type != declared type
cptr.schema.fields.zip(encoderSchema.fields).zipWithIndex.foreach { case ((s, t), i) =>
  if (s.dataType != t.dataType)
    println(f"$i%3d  ${s.name}%-45s ${s.dataType.simpleString}%-10s -> ${t.dataType.simpleString}")
}

// (b) values that the cast would silently turn into null
val named = cptr.toDF(encoderSchema.fieldNames: _*)
encoderSchema.fields.zipWithIndex.foreach {
  case (f, i) if f.dataType == TimestampType && cptr.schema(i).dataType == StringType =>
    val c = col(f.name)
    val lost = named.filter(c.isNotNull && trim(c) =!= "" && c.cast(f.dataType).isNull).count()
    println(f"${f.name}%-45s unparseable=$lost")
  case _ =>
}
```

Acceptance criteria — all three must hold:

1. **Every `unparseable` count is 0.** A non-zero count is a rating date that the
   fix will replace with `null`, silently. That is the only outcome worse than the
   current crash. If any value fails, decide the policy (reject the file, or fail
   the job) before merging — do not merge and let it null.

2. **No field declared `String` appears in list (a).** A `String` field that
   inference typed `double`/`int` does not survive the round trip: `0042` → `42`,
   `1.50` → `1.5`. `rmpm_id` is the join key in
   `shockCounterparties(…, "RMPM_ID")`, so a reformatted key is a join that
   quietly matches fewer rows. The traces indicate this is not happening today,
   but it is data-dependent — re-run this check for each quarterly file.

3. **List (a) contains only the expected conversions** — the A-08 timestamp
   ordinals, nothing else. Arity is checked implicitly: `toDF` throws if the
   column count does not match.

Record the output of (a) in the merge request. It is the evidence that the change
converts exactly what was intended and nothing more.

### 8.2 Value parity

Full-column diff of the parsed timestamps against a reference run, with
`spark.sql.session.timeZone` pinned identically in both (§7).

### 8.3 Downstream figures

Bit-identical regulatory output vs. the pre-change baseline, for every column
except those listed by check (a) — which have no working baseline, since the job
currently aborts before producing them.

---

## 9. Anomaly register

| Code | Type | Location | Description |
|---|---|---|---|
| A-01 | Preserved bug | `IO.stringSchemaForTable` | Exception message duplicates the phrase: `"datatype not handled is not handled "` |
| A-02 | Latent defect | `IO.stringSchemaForTable` | Non-exhaustive match; missing `TimestampNTZType`, `DateType`, `LongType`, `BooleanType`, `DecimalType`, `NullType` |
| A-03 | Config divergence | environment | `spark.sql.datetime.java8API.enabled` unpinned, differs between runs and environments |
| A-04 | Latent perf | pipeline | ~25 nested `MapPartitionsRDD` frames — very deep lineage, consider `checkpoint()` |
| A-05 | Preserved anomaly | `Counterparties:13-14` | `CounterpartyRatingRatified` (PascalCase) sits directly beside `counterparty_rating_ratified` (snake_case). Two distinct fields, near-identical names. Preserved verbatim — renaming either would shift column mapping |
| A-06 | Preserved anomaly | `Counterparties:16` | `managing_site: Option[Integer]` uses boxed `java.lang.Integer` inside `Option`, unlike every other numeric field (`Option[Double]`). Double-nullable |
| A-07 | Preserved anomaly | `Counterparties` | Inconsistent token separation between a metric and its `_cap` partner: `net_debt_to_eti_ccm` / `net_debt_to_eticcm_cap`; `e_value_to_net_debt` / `e_valuetonetdebt_cap`; `enterprise_value_to_ebitda` / `enterprisevalue`. Load-bearing — these are actual column names |
| A-08 | Scope finding | `Counterparties` | 8 timestamp ordinals (8, 15, 20, 21, 22, 27, 58, 77) and 26 `Option[Double]` ordinals share the defect. Only 77 surfaces on the test row because the earlier ones are null. See §3.4 |
| B-01 | Low-confidence read | `IO` (both readers) | Param `delimeter` vs option key `"delimiter"` — misspelling preserved verbatim |
| B-02 | Low-confidence read | `IO.readCsv` | `Choose schema` transcribed without `//`; likely an IDE inlay hint rather than source |
| ~~C-01~~ | Resolved | `IO.readCsv` | `inferSchema: Boolean = true` is the fifth parameter; earlier screenshot was edge-truncated |
| C-02 | Cut / inferred | `Flair.process` | `scoreWithPdBound` referenced at line 43, definition falls in the cut region (lines 31-33) |
| C-03 | Unresolved reference | `IO.readCsvAsDataset` | `.schema(schema)` references a bare `schema` that is neither a parameter nor visible in the screenshot |
| C-04 | Unused parameter | `IO.readCsvAsDataset` | Declares `canBeEmpty` but the visible body has no branch on it, unlike `readCsv` |
| C-05 | Inconsistency | `IO` | `readCsvAsDataset` uses a bare `spark`; `readCsv` resolves `SparkSession.getActiveSession.get` locally |
| A-09 | Confirmation of A-08 | run 2 | Fixing ordinal 77 via input format did not fix the job — the same `String`-in-`timestamp` error recurs at another timestamp ordinal. Per-column and format-level fixes only promote the next field to failing. See §4 |
| ~~C-06~~ | Resolved | `IO.readCsv` / input | CSV inference is format-sensitive: the `…Z` form yields `StringType`, the `yyyy-MM-dd HH:mm:ss` form yields `TimestampType`. Corrects the §2 claim that format is irrelevant |
| C-07 | Unreadable | screenshot edge | Partially visible French/English text pane (`sk. We expe…`, `ting (12/08/2…`, `qu'en cas de`). Not transcribed |
| C-08 | Data quality | input file | Trailing `<unset>` (ragged) record from §4 — no longer a suspected cause, but still worth the `awk` check before Option B, which is stricter about ragged rows |

All `⟪CUT⟫` markers denote screenshot discontinuities. No content was
reconstructed or inferred across them.
