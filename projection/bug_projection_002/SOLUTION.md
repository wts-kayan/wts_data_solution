# bug_projection_002 — one jar that runs on Spark 3.3.2 and 3.5.4

**Failure:** [`str_projection_engine_error_2026-09-17.md`](str_projection_engine_error_2026-09-17.md)
**Goal:** during the 3.3.2 → 3.5.4 migration, the same
`str_projection_engine` jar must run on both runtimes
**Chosen fix:** bean encoder — no reflection, no version-specific API (§2)
**Call sites:** two, `StepGetMacroVar` and `LgdFwdStepGetMacroVar`, identical patch (§2.3)
**Status:** written and verified on both versions (§4)
**Date:** 2026-09-18

---

## 1. The incompatibility

One call is version-specific: `StepGetMacroVar.getTmpEncoder` builds the encoder for
the `flatMap` that flattens the macro-variable CSV.

```java
return Encoders.row(structType);          // Spark >= 3.5 only
```

The factory for a Row encoder moved in 3.5. Checked with `javap` against the jars in
the local Maven repository:

| Spark | `Encoders.row(StructType)` | `RowEncoder.apply(StructType)` | `RowEncoder` lives in |
|---|---|---|---|
| 3.2.0 / 3.3.1 / 3.4.0 | absent | present → `ExpressionEncoder<Row>` | `spark-catalyst` |
| 3.5.0 / 3.5.4 | **present** → `Encoder<Row>` | absent (only `encoderFor` → `AgnosticEncoder<Row>`) | `spark-sql-api` |

The two are disjoint across the 3.4 / 3.5 boundary: `Encoders.row` fails to link on
3.3.2 (the failure of 2026-09-17), `RowEncoder.apply` fails to link on 3.5.4 (the
mirror failure, which is why the code was changed in the first place).

**`Encoders.bean` is the way out.** It is unchanged across both versions, so the
problem disappears rather than being bridged: encode a typed bean instead of a `Row`,
and no version-specific factory is called at all.

---

## 2. The fix

### 2.1 New bean — [`code/fix/MacroVarFlat.java`](code/fix/MacroVarFlat.java)

Five properties matching what the flatten step emits: `date`, `scenario`, `sector`,
`variable`, `value`. Public no-arg constructor, getters and setters,
`Serializable`.

### 2.2 `VarMacroConverter` emits beans — [`code/fix/VarMacroConverter.java`](code/fix/VarMacroConverter.java)

`RowFactory.create(values)` → `new MacroVarFlat(date, scenario, sector, varName, value)`,
and the three return types change from `List<Row>` / `Iterator<Row>` to
`List<MacroVarFlat>` / `Iterator<MacroVarFlat>`. The flatten *logic* — the
sectorializable lookup, the "duplicate across all sectors" rule, the
"drop vars not in this projection" filter — is untouched. Input rows are still
`Row`; only the output type changed.

This also removes the `Object[] values = {...}` / `RowFactory` indirection, so a field
order mistake becomes a compile error instead of a wrong column.

### 2.3 The two call sites — identical patch

`importVarMacroEcoFromCsv` exists twice, character for character (`diff` of the
deployed sources reports no difference), each class carrying its own private
`getTmpEncoder`, `castDoubleColumn` and `computeSectoredVarNames`:

| Class | Patch |
|---|---|
| `…steps.defaultRates.StepGetMacroVar` | [`code/fix/StepGetMacroVar_importVarMacroEcoFromCsv.java`](code/fix/StepGetMacroVar_importVarMacroEcoFromCsv.java) |
| `…steps.lgdForward.LgdFwdStepGetMacroVar` | [`code/fix/LgdFwdStepGetMacroVar_importVarMacroEcoFromCsv.java`](code/fix/LgdFwdStepGetMacroVar_importVarMacroEcoFromCsv.java) |

The patch below is written against `StepGetMacroVar`; the second file is the same
edit in the other class. §2.5 explains why both must be done in the same commit.

### 2.4 The patch — [`code/fix/StepGetMacroVar_importVarMacroEcoFromCsv.java`](code/fix/StepGetMacroVar_importVarMacroEcoFromCsv.java)

`getTmpEncoder` is deleted. Inside `importVarMacroEcoFromCsv`:

```diff
-        Dataset<Row> datasetVMEI = dataset.flatMap(flattener::flattenVarRow, getTmpEncoder(colDate, colScenario, colSector));
+        Dataset<Row> datasetVMEI = dataset
+                .flatMap(flattener::flattenVarRow, Encoders.bean(MacroVarFlat.class))
+                .select(col("date").as(colDate),
+                        col("scenario").as(colScenario),
+                        col("sector").as(colSector),
+                        col("variable").as(COL_VARIABLE),
+                        col("value").as(COL_VALUE));
 
-        String colVarName = datasetVMEI.columns()[3];
-        String colValue = datasetVMEI.columns()[4];
+        String colVarName = COL_VARIABLE;
+        String colValue = COL_VALUE;
```

plus two constants on the class:

```java
private static final String COL_VARIABLE = "Variable";
private static final String COL_VALUE    = "Value";
```

Everything below that point — the `groupBy(...).agg(collect_list, collect_list)`, the
two `withColumnRenamed` calls, the final `as(Encoders.bean(MacroVar.class))` — is
unchanged, because the select hands it exactly the column names and order the Row
encoder used to produce.

### 2.5 Both call sites, one commit — not optional

`VarMacroConverter` is shared by the two classes, and §2.2 changes its return type.
So the moment the converter emits beans, `LgdFwdStepGetMacroVar` stops compiling:
its `flatMap(flattener::flattenVarRow, Encoder<Row>)` no longer type-checks. There is
no "fix one now, one later" path — the compiler enforces the pair.

That is also the reassuring part: the failure mode is a compile error in CI, not a
`NoSuchMethodError` on a cluster at 14:11.

> **A-01 — unverified assumption.** `LgdFwdStepGetMacroVar.getTmpEncoder` sits in the
> region not captured in the screenshots (source lines 1-152). The patch assumes it
> builds the same five-field schema as the one in `StepGetMacroVar`. Confirm before
> applying: if that class builds a different schema, `MacroVarFlat` does not fit it
> and that call site needs its own bean. Everything visible — the identical
> `importVarMacroEcoFromCsv`, the shared `VarMacroConverter`, the same
> `as(Encoders.bean(MacroVar.class))` at the end — says it is the same five fields.

### 2.6 Worth doing while you are here: delete one of the copies

Two identical 25-line methods, two identical `getTmpEncoder`s, two identical
`castDoubleColumn`s and two identical `computeSectoredVarNames`es (a third copy of
that one sits in `VarMacroConverter` itself). This duplication is why a one-line Spark
API change became a two-class fix.

A static helper — `MacroVarCsvLoader.load(sc, filepath, varNameList, sectors, commonVarNames)`
returning `Dataset<MacroVar>` — collapses both call sites into one, and the next
version-sensitive change lands in a single place. Each step then calls:

```java
return MacroVarCsvLoader.load(sc, filepath, varNameList, sectors, commonVarNames);
```

Not folded into this fix: it moves code across packages and deserves its own review
and its own commit. Do the migration fix first, the extraction second.

### 2.7 Why the select is needed — the two objections, answered

**Runtime column names.** `colDate` and `colScenario` come from the CSV header
(`colsTmp[0]`, `colsTmp[1]`), and a Java bean cannot have runtime-determined property
names. The alias select bridges the two: fixed property name in, runtime column name
out. Five lines, and it makes the mapping explicit at the call site instead of
implicit in a `StructType` built three methods away.

**Alphabetical property order.** Confirmed on both versions — `Encoders.bean` orders
properties alphabetically, and `value` sorts before `variable`:

```
bean-encoder column order = [date, scenario, sector, value, variable]
```

So the old `datasetVMEI.columns()[3]` / `[4]` would have picked up `value` and
`variable` the wrong way round. The select fixes the order, and naming the two columns
with constants removes the positional lookup that caused the hazard. This is the one
place where the change is more than mechanical — **do not** keep the positional reads.

---

## 3. Build and packaging

1. **Compile against 3.3.2, run on both.** After this change nothing in the step needs
   a 3.5-only API, so 3.3.2 is the safer compile target: what compiles against 3.3
   exists in 3.5, apart from the removals in §5. The reverse is not true — that
   asymmetry is what produced this bug.
2. **`provided` scope, one coordinate.** `spark-core` / `spark-sql` / `spark-catalyst`
   pinned to the CDP coordinate `3.3.2.3.3.7191000.1-126` in `provided` scope, never
   packaged in the fat jar.
3. **Compile against both in CI.** Parameterize the Spark version
   (`-Dspark.version=…`) and build twice, 3.3.2 and 3.5.4. Both must compile. That is
   what catches the next `Encoders.row`-shaped problem before a cluster does.
4. **Drop the bundled SLF4J binding** (§2.3 of the error report): the fat jar ships its
   own `StaticLoggerBinder`, competing with `log4j-slf4j-impl` and `slf4j-reload4j`
   from the parcel. Logging belongs in `provided` scope on CDP. Same family as the
   earlier b001 `LoggingEventBuilder` crash.

---

## 4. Verification — actually run, both versions

[`code/fix/MacroVarFlatEncoderCheck.java`](code/fix/MacroVarFlatEncoderCheck.java)
reproduces `importVarMacroEcoFromCsv` end to end: a wide input frame, the
sector-duplication rule, `flatMap(Row → Iterator<MacroVarFlat>, Encoders.bean(...))`,
the alias select, then the same `groupBy(...).agg(collect_list, collect_list)` and
renames. It carries a self-contained stand-in for `VarMacroConverter` so it runs
outside the project (the real one needs `SectoredVarName` and `CommonConstants`).

Compiled with `javac --release 8` and run on a local session against each version's
full dependency classpath (`mvn dependency:build-classpath`):

```
================ Spark 3.3.1
BEAN 3.3.1 | bean-encoder column order = [date, scenario, sector, value, variable]
after select      = [Date, scenario, sector, Variable, Value]
final schema      = struct<scenario:string,Variable:string,sector:string,Date:array<string>,Value:array<double>>
final nullability = scenario:null Variable:null sector:null Date:NOTNULL Value:NOTNULL
  [Central,GDP,FR,WrappedArray(2026-01, 2026-02),WrappedArray(1.5, 1.6)]
  [Central,INV,FR,WrappedArray(2026-01, 2026-02),WrappedArray(2.5, 2.6)]

================ Spark 3.5.4
BEAN 3.5.4 | bean-encoder column order = [date, scenario, sector, value, variable]
after select      = [Date, scenario, sector, Variable, Value]
final schema      = struct<scenario:string,Variable:string,sector:string,Date:array<string>,Value:array<double>>
final nullability = scenario:null Variable:null sector:null Date:NOTNULL Value:NOTNULL
  [Central,GDP,FR,WrappedArray(2026-01, 2026-02),WrappedArray(1.5, 1.6)]
  [Central,INV,FR,WrappedArray(2026-01, 2026-02),WrappedArray(2.5, 2.6)]
```

Same column order, same schema, same nullability, same rows — from the **same class
files** on both classpaths.

This covers both call sites: the patched method bodies in §2.3 are identical, so the
run exercises the code of each. What it does not cover is the assumption in A-01,
that `LgdFwdStepGetMacroVar.getTmpEncoder` builds the same schema.

Re-run it with:

```bash
mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt     # once per Spark version
javac --release 8 -cp "$(cat cp.txt)" -d out MacroVarFlat.java MacroVarFlatEncoderCheck.java
java --add-opens=java.base/java.lang=ALL-UNNAMED … -cp "out;$(cat cp.txt)" \
     com.bnpp.itg.fresh.str.model.MacroVarFlatEncoderCheck
```

### 4.1 One cross-version difference, and why it is harmless

`Encoders.bean` types the primitive `double` differently on the two versions:

```
3.3.1  bean schema : date:null scenario:null sector:null value:null    variable:null
3.5.4  bean schema : date:null scenario:null sector:null value:NOTNULL variable:null
```

Spark 3.5 marks a primitive `double` property non-nullable; 3.3 does not. It does not
propagate: after `collect_list` the aggregated schema is identical on both (the
`final nullability` line above), and `value` is a primitive that the converter always
sets, so no row can carry a null there. Recorded because it is the kind of difference
that surfaces later as a schema-comparison failure in an unrelated test.

> **V-01** — the 3.3 side was verified against **3.3.1** from the local Maven
> repository, not the CDP build `3.3.2.3.3.7191000.1-126`. `Encoders.bean` is
> unchanged across 3.3.x, so this is a low-risk gap.
>
> **V-02** — run on JDK 11 with `--add-opens` (no JDK 8 on this workstation). The
> cluster is JDK 1.8.0_241. Nothing here depends on the module system.
>
> **V-03** — the harness uses a stand-in converter, not the real
> `VarMacroConverter`. The flatten logic it exercises is a transcription of that
> class, but the production class compiles only inside the project.

---

## 5. What this does *not* cover

`Encoders.row` is the call that failed on 2026-09-17. It is unlikely to be the only
3.5-only API in a 7.8.2 jar that was ported to 3.5.

The cheap sweep: compile the whole module against 3.3.2. Every error is a call site
that will `NoSuchMethodError` on the old cluster. Known 3.4/3.5 removals to look for:

| Removed / moved in 3.5 | Use instead |
|---|---|
| `RowEncoder.apply(StructType)` | a bean encoder (this fix), or `RowEncoderCompat` (§6) |
| `Encoders.row` on 3.3 | same |
| `ExpressionEncoder` as a declared type | keep variables typed as `Encoder<T>`, never the concrete class |
| `Dataset.unpivot` / `melt` (3.4+) | not on 3.3 — use `stack()` SQL |

**Caller sweep for this change:** `grep -rn "flattenVarRow\|getTmpEncoder\|Encoders.row"`
across the module. Over the sources collected here it returns two step classes
(§2.3) plus `VarMacroConverter` itself, all handled. Run it against the real module —
anything else that consumed `Iterator<Row>` from the converter, tests especially, now
gets `Iterator<MacroVarFlat>` and must be updated. The compiler finds all of them.

---

## 6. Fallback kept: [`code/fix/RowEncoderCompat.java`](code/fix/RowEncoderCompat.java)

The reflective Row-encoder shim, resolving `Encoders.row` (3.5+) or
`RowEncoder.apply` (≤3.4) once in a static initializer. **Not** used by this fix, kept
because it was verified working on both versions and because a second Row-encoder site
may turn up in the §5 sweep where a bean is not practical — for instance a schema
genuinely built at runtime, where no fixed set of properties exists.

Prefer the bean wherever the columns are known at compile time: no reflection, typed
construction, and the compiler checks the field order.

---

## 7. Alternatives considered

**Drop the encoder entirely.** The flatten is an unpivot: wide macro-variable columns
to `(Variable, Value)` rows, with the sector-duplication rule from
`VarMacroConverter`. As a `stack()` SQL expression it needs no encoder, no `flatMap`
and no JVM-side serialization at all, and `stack()` behaves identically on 3.3 and
3.5. Better end state, but a real refactor of the converter's semantics (the
`sectoredVarNames` lookup and the drop-unused-vars filter). Follow-up ticket, not this
fix.

**Two jars, one per Spark version.** Rejected: doubles the release matrix for the
duration of the migration, and the point is a single artifact that can be rolled back
onto 3.3.2.

---

## 8. Note while you are in this code

`VarMacroConverter.flattenVarRow` calls `row.getDouble(i)` on columns produced by
`cast(... as double)`. A non-numeric cell casts to `null`, and `getDouble` then throws
an NPE inside the task. The behaviour is the same on 3.3 and 3.5, so it is **not** a
migration risk — noted only because a bad CSV cell surfaces here as an opaque task
failure rather than a data-quality message. With `MacroVarFlat` in place, a
`Double` property (instead of `double`) plus a null check would let the step report
the offending column and row instead.
