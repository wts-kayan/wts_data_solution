# b001 — `NoClassDefFoundError: org/slf4j/spi/LoggingEventBuilder`

**Module:** `com.bnpparibas.ilt.fresh.str:str-circe-var-macro-generator:2.2.2-RELEASE`
**Symptom:** [`error.txt`](error.txt) — fails at class-init of `Circe$`, on the very first `LoggerFactory.getLogger`
**Reproduction pom:** [`pom.xml`](pom.xml) (kept as-is; this is the evidence)
**Proposed pom:** [`pom-fixed.xml`](pom-fixed.xml)
**Date:** 2026-09-08

---

## Summary

The build puts **three SLF4J bindings** on the classpath. SLF4J permits exactly
one. Two of the three ship a class with the *same fully-qualified name*
(`org.apache.logging.slf4j.Log4jLoggerFactory`) compiled against *different*
SLF4J API versions, and `maven-assembly-plugin`'s `jar-with-dependencies`
flattens them into one jar where the last one written silently wins.

The failure is the slf4j-**2.x** flavour of that class being loaded while the
`slf4j-api` actually resolved at runtime is **1.7.x** — so
`org.slf4j.spi.LoggingEventBuilder`, which only exists in slf4j 2.x, is absent.

---

## Evidence

`mvn -f pom.xml dependency:tree -Dincludes='org.slf4j:*,org.apache.logging.log4j:*'`

```
com.bnpparibas.ilt.fresh.str:str-circe-var-macro-generator:jar:2.2.2-RELEASE
+- org.apache.spark:spark-core_2.12:jar:3.5.4:provided
|  \- org.apache.spark:spark-common-utils_2.12:jar:3.5.4:provided
|     +- org.slf4j:jul-to-slf4j:jar:2.0.7:provided
|     +- org.slf4j:jcl-over-slf4j:jar:2.0.7:provided
|     +- org.apache.logging.log4j:log4j-slf4j2-impl:jar:2.20.0:provided   <-- binding 1
|     \- org.apache.logging.log4j:log4j-1.2-api:jar:2.20.0:provided
+- org.slf4j:slf4j-api:jar:2.0.7:compile
+- org.apache.logging.log4j:log4j-slf4j-impl:jar:2.20.0:compile          <-- binding 2
|  +- org.apache.logging.log4j:log4j-api:jar:2.20.0:compile
|  \- org.apache.logging.log4j:log4j-core:jar:2.20.0:runtime
\- org.slf4j:slf4j-reload4j:jar:2.0.7:compile                            <-- binding 3
```

Maven also prints, twice:

```
[WARNING] The artifact org.slf4j:slf4j-log4j12:jar:2.0.7 has been relocated to
          org.slf4j:slf4j-reload4j:jar:2.0.7
```

### What each declaration actually did

| Declared in `pom.xml` | What lands | Problem |
|---|---|---|
| `log4j-slf4j-impl:2.20.0` | binding 2 | This is the **slf4j 1.7** binding. With `slf4j-api` 2.x the correct artifact is `log4j-slf4j2-impl`. |
| `slf4j-log4j12:${slf4j-api.version}` | binding 3 | `slf4j-log4j12` **does not exist at 2.0.7** — the line ended at 1.7.36. Maven silently relocates it to `slf4j-reload4j`, binding SLF4J to reload4j (log4j 1.2) instead of log4j2. Almost certainly not what was intended. |
| Spark `provided` | binding 1 | Correct for slf4j 2.x, and already there. Nothing needed declaring at all. |

### Why it manifests as a *missing class* rather than SLF4J's usual warning

`log4j-slf4j-impl` and `log4j-slf4j2-impl` both contain
`org.apache.logging.slf4j.Log4jLoggerFactory` — same FQCN, different bytecode.
The slf4j2 variant references `org.slf4j.spi.LoggingEventBuilder`; the 1.7
variant does not. Normally SLF4J would detect multiple bindings and print
`Class path contains multiple SLF4J bindings`, but here the duplicate is at the
*class* level inside a shaded jar, so there is only ever one `Log4jLoggerFactory`
visible and SLF4J's own multiple-binding check never fires. You get a hard
`NoClassDefFoundError` instead of a warning.

Which of the two variants wins depends on jar ordering in the assembly — so this
bug is **order-dependent and will appear to come and go** between builds and
between IDE-run and cluster-run.

### The one thing left to confirm on the failing host

The pom resolves `slf4j-api` to 2.0.7, yet the trace proves the `slf4j-api`
*loaded at runtime* was 1.7.x (otherwise `LoggingEventBuilder` would be present).
Since the Spark dependencies are `provided`, they are not in the fat jar and the
runtime picks up the cluster's own jars — Hadoop-side distributions commonly
still ship `slf4j-api` 1.7.x. The stack is also JDK 8
(`sun.misc.Launcher$AppClassLoader`).

Confirm with, on the machine that produced `error.txt`:

```bash
java -verbose:class -cp <the runtime classpath> com.bnpparibas.itg.fresh.str.Circe \
  | grep -iE 'slf4j-api|Log4jLoggerFactory|LoggingEventBuilder'
```

This does not change the fix below — three bindings is wrong regardless — but it
tells you whether the cluster classpath also needs pinning.

---

## Fix

Applied in [`pom-fixed.xml`](pom-fixed.xml):

1. **Drop `slf4j-log4j12` entirely.** It is the phantom relocation to
   `slf4j-reload4j` and a binding nobody asked for.
2. **`log4j-slf4j-impl` → `log4j-slf4j2-impl`**, matching `slf4j-api` 2.x.
3. **Scope the whole logging stack `provided`.** Spark already supplies
   `slf4j-api` 2.0.7, `log4j-slf4j2-impl` 2.20.0 and `log4j-core` 2.20.0 at
   exactly these versions. Declaring them `compile` is what copied a second set
   into the assembly jar in the first place.
4. **Declare `log4j-core` explicitly** so the backend version is pinned rather
   than inherited by accident.

Result — `mvn -f pom-fixed.xml dependency:tree` with the same filter:

```
+- org.slf4j:slf4j-api:jar:2.0.7:provided
+- org.apache.logging.log4j:log4j-slf4j2-impl:jar:2.20.0:provided
|  \- org.apache.logging.log4j:log4j-api:jar:2.20.0:provided
\- org.apache.logging.log4j:log4j-core:jar:2.20.0:provided
```

**Exactly one binding.** No `slf4j-reload4j`, no `log4j-slf4j-impl`, and nothing
logging-related enters the shaded jar.

### Recommended guard so this cannot recur

The real defect is that nothing failed the build when a second binding appeared.
Add to `<build><plugins>`:

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-enforcer-plugin</artifactId>
  <version>3.4.1</version>
  <executions>
    <execution>
      <id>ban-duplicate-slf4j-bindings</id>
      <goals><goal>enforce</goal></goals>
      <configuration>
        <rules>
          <bannedDependencies>
            <excludes>
              <exclude>org.slf4j:slf4j-log4j12</exclude>
              <exclude>org.slf4j:slf4j-reload4j</exclude>
              <exclude>org.slf4j:slf4j-simple</exclude>
              <exclude>org.slf4j:slf4j-jdk14</exclude>
              <exclude>org.apache.logging.log4j:log4j-slf4j-impl</exclude>
              <exclude>ch.qos.logback:logback-classic</exclude>
            </excludes>
          </bannedDependencies>
          <dependencyConvergence/>
        </rules>
      </configuration>
    </execution>
  </executions>
</plugin>
```

Not added to `pom-fixed.xml` — `dependencyConvergence` will very likely fail on
this dependency set today and that is a separate cleanup, not part of b001.

---

## Secondary findings

Not the cause of b001. Listed so they can be ticketed separately.

1. **`renjin-script-engine:3.5-beta76` is not on Maven Central and the pom
   declares no `<repositories>`.** `mvn dependency:resolve` against central
   fails outright; `dependency:tree` only warns
   *"The POM for org.renjin:renjin-script-engine is missing, no dependency
   information available"*. The build therefore works **only** against the
   internal mirror, and Maven cannot see renjin's transitive dependencies at all.
   Directly relevant here: the existing `<exclusion>` of `slf4j-jdk14` on renjin
   shows somebody already hit an SLF4J conflict from this dependency, and with
   its POM unreadable, Maven cannot reliably apply that exclusion. Pin the Renjin
   repository explicitly or vendor the artifact.
2. **`scalatest_2.11:2.1.3` against `scala.version=2.12.18`.** Wrong Scala
   cross-version — drags a 2.11 `scala-library` onto the test classpath.
   Changed to `scalatest_${spark-scala.version}:3.2.18` in `pom-fixed.xml`
   (resolution verified).
3. **`-XX:MaxPermSize=256m`** in the surefire `argLine` — removed in Java 8,
   emits a warning on every test run. Dropped in `pom-fixed.xml`.
4. **`mainClass` package mismatch.** The pom declares
   `com.bnpparibas.ilt.fresh.str.Circe`; the stack trace shows
   `com.bnpparibas.itg.fresh.str.Circe` (`ilt` vs `itg`). One of the two is
   wrong — worth resolving before the next release build. **Left unchanged**,
   since which one is correct is not determinable from the material here.
5. **`maven-compiler-plugin` version is `${maven.version}` (3.5.1).** A property
   named `maven.version` holding a *plugin* version is a trap; rename it.
6. **`<junit.version>3.8.1</junit.version>` is declared and never used.**

---

## How to apply

`pom-fixed.xml` is a proposal against the copy of the pom captured in this bug
folder. It is not wired into any build. To land it, port items 1–4 of the Fix
section into the real `str_circe` project pom and re-run the job that produced
`error.txt`.
