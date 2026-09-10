# Running Spark locally on a Windows workstation — Step-by-step guide

**Audience** : developers joining the team who need to run a Spark (Scala/Java) job from their own machine, without a cluster.
**Target stack** : Spark 3.5.4 / Scala 2.12.18 / JDK 8 — same versions as CDP 7.1.9 in UAT/PROD.
**Session length** : ~45 min.

> Replace `<UID>` with your Windows login (e.g. `h60327`) everywhere in this document.
> The workstations run **Windows in French**, so UI labels are given as `English (French)`: *Environment Variables (Variables d'environnement)*, *New… (Nouvelle…)*, *Edit… (Modifier…)*.

---

## 0. Why Windows is a special case

Spark is designed for Linux. On Windows, two native Hadoop pieces are missing:

| File | Purpose |
|---|---|
| `winutils.exe` | Emulates the POSIX calls (permissions, `chmod`, file ownership) that Hadoop performs at startup |
| `hadoop.dll` | Native library loaded by Hadoop's `NativeIO` |

Without them, **the job fails before reading a single row**:

```
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
-see https://wiki.apache.org/hadoop/WindowsProblems
```

Older Hadoop builds phrase the same failure differently:

```
java.io.FileNotFoundException: Could not locate executable null\bin\winutils.exe
in the Hadoop binaries.
```

Both mean `HADOOP_HOME` is not set — in the second one, `null` is the unset variable interpolated into the path. This is the number one blocker for newcomers.

---

## 1. Prerequisites

| Tool | Version | Location |
|---|---|---|
| JDK 8 | `jdk-1.8` | `C:\Program Files\Java\jdk-1.8` |
| JDK 17 (optional) | RedHat OpenJDK 17.0.3.0.6-3 | `C:\Program Files\RedHat\java-17-openjdk-17.0.3.0.6-3` |
| IntelliJ IDEA | any recent build + **Scala** plugin | `C:\Users\<UID>\software\JetBrains\IntelliJ` |
| Maven | 3.8+, or the one bundled with IntelliJ | — |
| `winutils.zip` | Hadoop 3.3.x | Team SharePoint (see §2) |

> **JDK 8 is the reference build JDK** — it is what CDP 7.1.9 runs. JDK 17 is installed alongside it for targeted tests only; it is never the default `JAVA_HOME` — see §8.

---

## 2. Getting winutils

The archive is version-controlled on the team SharePoint:

```
https://bnpparibas.sharepoint.com/sites/str/Stress%20Testing/06_PRESENTATIONS_et_DOCUMENTATIONS/01-Engine/04-Install/
```

Navigate to that folder and download `winutils.zip` from there.

> Direct file links copied from the browser look like
> `.../04-Install/winutils.zip?d=wc38b83cc…&csf=1&web=1&e=g1w5yC`.
> Everything from `?d=` onwards is a **per-session download token** — it expires and is tied to the person who generated it. Always share the **folder** path, never the tokenised file link.

> ⚠️ **Do not download a random `winutils.exe` from GitHub.** An unvetted executable will not pass workstation security policy.

> **Which Hadoop version?** Locally, the Hadoop client that loads `winutils.exe` is the one bundled with **Spark 3.5.4 — Hadoop 3.3.x**, not the Hadoop 3.1.x of CDP 7.1.9. Take the 3.3.x build. A 3.1.x one still covers the basics (`ls`, `chmod`), but it is not the matching pair.

### Extraction

Unzip into a path with **no spaces and no accented characters**:

```
C:\Users\<UID>\all\bin\winutils\
```

You must end up with exactly this layout:

```
C:\Users\<UID>\all\bin\winutils\
└── bin\
    ├── hadoop.dll      (~91 KB)
    └── winutils.exe    (~110 KB)
```

> 🔑 **Key point for the demo**: the `bin` sub-folder is **mandatory**. Hadoop builds the path `%HADOOP_HOME%\bin\winutils.exe`. Putting the two files directly under `winutils\` will not work.

> **Antivirus note**: workstations run McAfee Endpoint Security. If `winutils.exe` disappears after extraction, it has been quarantined — raise an exclusion request through workstation support rather than re-downloading it in a loop.

---

## 3. Environment variables

`Win + R` → `sysdm.cpl` → **Advanced system settings (Paramètres système avancés)** → **Environment Variables… (Variables d'environnement…)**
Top section: *User variables for `<UID>` (Variables utilisateur pour `<UID>`)* → **New… (Nouvelle…)**

| Variable | Value |
|---|---|
| `JAVA_HOME` | `C:\Program Files\Java\jdk-1.8` |
| `HADOOP_HOME` | `C:\Users\<UID>\all\bin\winutils` |
| `MAVEN_OPTS` | see below — long single-line value |

Set them as **user** variables, not system variables — no local admin rights required.

### `MAVEN_OPTS` in full

Paste this as **one single line**, with `<UID>` replaced by your own login:

```
-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true -Djavax.net.ssl.trustStore=C:\Users\<UID>\all\security\cacerts -Djavax.net.ssl.trustStorePassword=changeit
```

What each flag does:

| Flag | Why it is there |
|---|---|
| `maven.wagon.http.ssl.insecure=true` | Corporate traffic goes through a TLS-inspecting proxy, so the certificate presented by Artifactory is re-signed by an internal CA |
| `maven.wagon.http.ssl.allowall=true` | Disables hostname verification against that re-signed certificate |
| `maven.wagon.http.ssl.ignore.validity.dates=true` | Tolerates clock skew / expired intermediate certificates |
| `javax.net.ssl.trustStore=…\all\security\cacerts` | Points the JVM at the **company truststore** containing the internal root CA |
| `javax.net.ssl.trustStorePassword=changeit` | Default JKS password — not a secret, it is the JDK default |

> ⚠️ **The `cacerts` file must actually exist at that path.** It is per-user: `C:\Users\<UID>\all\security\cacerts`. Copying a colleague's `MAVEN_OPTS` without copying the file gives you a truststore that cannot be opened, and every Maven download fails.

> The two `ssl.insecure` / `allowall` flags are a workaround, not a fix. The clean alternative is importing the internal root CA into `%JAVA_HOME%\lib\security\cacerts` with `keytool -importcert`, but that requires write access to `C:\Program Files` — hence the per-user truststore.

### ⚠️ The `HADOOP_HOME` trap

```
✅ HADOOP_HOME = C:\Users\<UID>\all\bin\winutils
❌ HADOOP_HOME = C:\Users\<UID>\all\bin\winutils\bin
```

`HADOOP_HOME` points to the **parent** of `bin`, never to `bin` itself.

### Extend `Path`

Select `Path` → **Edit… (Modifier…)** → **New (Nouveau)**, then add:

```
%HADOOP_HOME%\bin
%JAVA_HOME%\bin
```

> **Close and reopen every terminal and IntelliJ afterwards.** Environment variables are read when a process starts: an already-running IntelliJ will never see the new value. This costs people 30 minutes of debugging every single time.

---

## 4. Verification — before writing any code

Open a **new** `cmd` window:

```bat
echo %JAVA_HOME%
echo %HADOOP_HOME%
java -version
mvn -v
%HADOOP_HOME%\bin\winutils.exe ls C:\
```

Expected:
- `java version "1.8.0_xxx"`
- `mvn -v` reports the same Java version as `JAVA_HOME`
- `winutils.exe ls` prints a permission line such as `drwxrwxrwx`

If `winutils.exe` fails with a missing DLL (`VCRUNTIME140.dll`), install the **Microsoft Visual C++ Redistributable x64**.

### Hive temp directory

Some jobs (SQL / Hive support enabled) require an accessible `/tmp/hive`:

```bat
mkdir C:\tmp\hive
%HADOOP_HOME%\bin\winutils.exe chmod -R 777 C:\tmp\hive
```

---

## 5. Opening the project in IntelliJ

1. **File → Open** → select the module `pom.xml` (e.g. `str_file_transform_engine`) → *Open as Project*.
2. **File → Project Structure → SDKs**: check that SDK 1.8 is registered.
3. **Project Structure → Project**: *Language level* = 8.
4. Let Maven resolve dependencies — or run `mvn -U clean install -DskipTests` from the command line, which gives clearer output.

### If Artifactory returns 401

This is **not** a missing artifact — it is an expired token. Fix `~/.m2/settings.xml` (`<servers>` → `<username>` / `<password>`), then re-run with `-U`.

---

## 6. IntelliJ run configuration — the one setting nobody remembers

Our POMs declare Spark, Scala and logging with scope **`provided`**: on the cluster those JARs come from CDP, and bundling them into the fat JAR causes `NoSuchMethodError` / `LinkageError` / `AbstractMethodError` at runtime.

The local consequence: IntelliJ **excludes** those JARs from the run classpath by default, and you get:

```
java.lang.ClassNotFoundException: org.apache.spark.sql.SparkSession
```

**Run → Edit Configurations… → tick "Add dependencies with 'provided' scope to classpath".**

This is the second most frequent error. It must **never** be fixed by switching dependencies to `compile` scope in the POM.

### Recommended VM options

```
-Xmx4g
```

`-Xmx4g` is the only one that matters here: the master is set in the code (`.master("local[*]")`), and that call wins over any `-Dspark.master` passed on the command line.

---

## 7. First Spark job in local mode

Create a throwaway class under `src/main/scala`:

```scala
package com.bnpparibas.itg.fresh.str.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object HelloSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HelloSpark")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      ("FR", "S1", 1200.0),
      ("FR", "S2",  450.0),
      ("IT", "S1",  980.0),
      ("IT", "S3",  120.0)
    ).toDF("country", "stage", "ead")

    df.groupBy("country")
      .agg(sum($"ead").as("total_ead"))
      .orderBy($"total_ead".desc)
      .show(false)

    spark.stop()
  }
}
```

> **Both imports are needed.** `import spark.implicits._` is what turns `$"ead"` into a `Column`; it does **not** bring in `sum`, which lives in `org.apache.spark.sql.functions`. Leaving the second import out gives `not found: value sum` — the classic first compile error on this example.

Run with `Shift + F10`. Expected output:

```
+-------+---------+
|country|total_ead|
+-------+---------+
|FR     |1650.0   |
|IT     |1100.0   |
+-------+---------+
```

If this runs, **the workstation is correctly configured.** Anything that breaks afterwards comes from the code or the data, not from the environment.

### Talking points during the demo

| Item | What to explain |
|---|---|
| `local[*]` | Driver and executors live in a **single JVM**, one thread per core. On the cluster: `--master yarn --deploy-mode cluster`. |
| Spark UI | `http://localhost:4040` while the job runs — walk through stages, DAG, shuffle. |
| `spark.sql.shuffle.partitions` | Defaults to 200, which is absurd for 4 rows locally. Always lower it. |
| No HDFS | Locally you read `file:///C:/...`. Cluster HDFS paths will not resolve. |

---

## 8. JDK 17 specifics

Spark 3.5 runs on Java 17, but JDK 17 closes reflective access to the JVM internals Spark relies on (`sun.nio.ch`, `java.nio`). Without explicit opens:

```
java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$
cannot access class sun.nio.ch.DirectBuffer
```

VM options to add:

```
-XX:+IgnoreUnrecognizedVMOptions
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
```

> Use the **whole** list — it is the one Spark 3.5 passes to its own JVMs (`JavaModuleOptions`). A shorter list gets you past the first `IllegalAccessError` and straight into the next one, typically on `jdk.internal.ref.Cleaner`.

> For now, **stay on JDK 8** — that is what CDP 7.1.9 runs. JDK 17 is there to anticipate, not for daily work.

---

## 9. Common errors — diagnostic table

| Message | Cause | Fix |
|---|---|---|
| `HADOOP_HOME and hadoop.home.dir are unset` — older wording: `Could not locate executable null\bin\winutils.exe` | `HADOOP_HOME` not set | §3, then **restart IntelliJ** |
| `Could not locate executable C:\...\winutils\bin\bin\winutils.exe` | `HADOOP_HOME` already includes `\bin` | Remove `\bin` from the variable |
| `UnsatisfiedLinkError: NativeIO$Windows.access0` | `hadoop.dll` missing or wrong architecture | Restore the x64 DLL in `%HADOOP_HOME%\bin` |
| `ClassNotFoundException: SparkSession` | `provided` scope excluded from run classpath | Tick the box in §6 |
| `IllegalAccessError: … DirectBuffer` | JDK 17 without `--add-opens` | §8, or switch back to JDK 8 |
| `NoSuchMethodError` on commons-math3 | Version clash with the CDP classpath | Relocation via `maven-shade-plugin` (already in place on the affected modules) |
| `Service 'sparkDriver' could not bind` | Hostname resolution / VPN | `.config("spark.driver.host", "localhost")` |
| `OutOfMemoryError: Java heap space` | Default driver heap | `-Xmx4g` in VM options |
| Maven `401 Unauthorized` | Expired Artifactory token | Update `~/.m2/settings.xml` |
| `PKIX path building failed: unable to find valid certification path` | Truststore not set, or internal root CA missing from it | Check `MAVEN_OPTS` and that `…\all\security\cacerts` exists (§3) |
| `java.io.IOException: Invalid keystore format` | `cacerts` path points to a missing or corrupted file | Re-copy the truststore for **your** `<UID>` |
| `PKIX … NotAfter` / certificate expired | Clock skew or expired intermediate | `-Dmaven.wagon.http.ssl.ignore.validity.dates=true` already covers this — check the flag is present |
| `Permission denied: /tmp/hive` | Emulated POSIX permissions | `winutils.exe chmod -R 777 C:\tmp\hive` |
| `winutils.exe` vanishes after unzip | Antivirus quarantine | Request an AV exclusion |

---

## 10. End-of-session checklist

Have each attendee tick these before leaving:

- [ ] `echo %JAVA_HOME%` → `C:\Program Files\Java\jdk-1.8`
- [ ] `echo %HADOOP_HOME%` → path **without** a trailing `\bin`
- [ ] `%HADOOP_HOME%\bin\winutils.exe ls C:\` works
- [ ] `%HADOOP_HOME%\bin` present in `Path`
- [ ] `MAVEN_OPTS` set, on one line, with **their own** `<UID>` in the truststore path
- [ ] `C:\Users\<UID>\all\security\cacerts` exists
- [ ] `mvn -U clean install -DskipTests` succeeds on the module
- [ ] "Add dependencies with provided scope" is ticked
- [ ] `HelloSpark` runs and prints the aggregated table
- [ ] Spark UI reachable at `localhost:4040` during the run

---

## 11. What local mode does **not** reproduce

State this explicitly, otherwise newcomers draw the wrong conclusions from their tests:

- **No YARN**: no scheduling behaviour, no containers, no executor OOM.
- **No Hive metastore**: `spark.sql("SELECT * FROM dbprojection…")` will not work without dedicated configuration.
- **No real HDFS partitioning**: partition-layout defects (`runId=` vs `runid=`) do not surface here.
- **Irrelevant data volumes**: a job that finishes in 3 seconds on 100 rows tells you nothing about the real perimeter.

Local mode exists to **validate business logic and debug with a real debugger**. Performance validation and regulatory-figure equivalence are proven on the cluster, not here.

---

## Suggested agenda (45 min)

| Time | Topic |
|---|---|
| 0–5 min | Why Windows ≠ Linux for Hadoop (§0) |
| 5–15 min | winutils install + environment variables, live on one workstation (§2–3) |
| 15–20 min | Command-line verification (§4) |
| 20–30 min | Project import + IntelliJ run configuration (§5–6) |
| 30–40 min | Run `HelloSpark`, tour of the Spark UI (§7) |
| 40–45 min | Common errors + limits of local mode (§9, §11) |
