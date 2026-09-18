# STR Projection Engine — YARN failure, 2026-09-17

**Application:** `str_projection_engine-7.8.2-RELEASE.jar`
**Runtime:** Spark 3.3.2 on CDP 7.1.9 (`spark-yarn_2.12-3.3.2.3.3.7191000.1-126`), Java 1.8.0_241
**Hive Session ID:** `a255966e-ad5d-41dd-a41f-0b1d485bccd3`
**Failure time:** 2026-09-17 14:11:20 +0200

---

## 1. Root cause — `NoSuchMethodError` on `Encoders.row`

```
2026-09-17 14:11:20,879 [Driver] ERROR org.apache.spark.deploy.yarn.ApplicationMaster:98
 - User class threw exception:
java.lang.NoSuchMethodError: org.apache.spark.sql.Encoders.row(Lorg/apache/spark/sql/types/StructType;)Lorg/apache/spark/sql/Encoder;

java.lang.NoSuchMethodError: org.apache.spark.sql.Encoders.row(Lorg/apache/spark/sql/types/StructType;)Lorg/apache/spark/sql/Encoder;
  at com.bnpp.itg.fresh.str.steps.defaultRates.StepGetMacroVar.getTmpEncoder(StepGetMacroVar.java:205)            ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at com.bnpp.itg.fresh.str.steps.defaultRates.StepGetMacroVar.importVarMacroECoFromCsv(StepGetMacroVar.java:172) ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at com.bnpp.itg.fresh.str.steps.defaultRates.StepGetMacroVar.importVarMacroEco(StepGetMacroVar.java:133)        ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at com.bnpp.itg.fresh.str.steps.defaultRates.StepGetMacroVar.launch(StepGetMacroVar.java:64)                    ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at com.bnpp.itg.fresh.str.runners.impl.ProjectionStepRunner.launchStep(ProjectionStepRunner.java:188)           ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at com.bnpp.itg.fresh.str.runners.impl.ProjectionStepRunner.launchStepWithCheck(ProjectionStepRunner.java:172)  ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at com.bnpp.itg.fresh.str.runners.impl.ProjectionStepRunner.launchDefaultRates(ProjectionStepRunner.java:137)   ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at com.bnpp.itg.fresh.str.runners.impl.ProjectionStepRunner.launchExecutions(ProjectionStepRunner.java:100)     ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at com.bnpp.itg.fresh.str.runners.impl.ProjectionStepRunner.launchWorkflow(ProjectionStepRunner.java:87)        ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at com.bnpp.itg.fresh.str.runners.impl.ProjectionRunner.runProjection(ProjectionRunner.java:62)                 ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at com.bnpp.itg.fresh.str.runners.impl.RunProjection.main(RunProjection.java:60)                                ~[str_projection_engine-7.8.2-RELEASE.jar:?]
  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)                                                  ~[?:1.8.0_241]
  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)                                ~[?:1.8.0_241]
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)                        ~[?:1.8.0_241]
  at java.lang.reflect.Method.invoke(Method.java:498)                                                             ~[?:1.8.0_241]
  at org.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:739)
     ~[spark-yarn_2.12-3.3.2.3.3.7191000.1-126.jar:3.3.2.3.3.7191000.1-126]
```

### Analysis

Version mismatch, not a logic bug. `Encoders.row(StructType)` was introduced in Spark **3.5.0**. The ApplicationMaster jar shows the cluster runtime is **Spark 3.3.2** (CDP 7.1.9). The code was compiled against 3.5.x and deployed onto 3.3.

### Fix

- At `StepGetMacroVar.java:205`, use `org.apache.spark.sql.catalyst.encoders.RowEncoder.apply(schema)` — the 3.3-era API.
- If the jar must run on both 3.3 and 3.5, resolve `Encoders.row` reflectively and fall back to `RowEncoder.apply` when absent.
- Verify `spark-core` / `spark-sql` are pinned to `3.3.2.3.3.7191000.1-126` in **`provided`** scope, not a 3.5.x coordinate.

> Note: this is the mirror image of the 3.5.4 rule — there `RowEncoder.apply` was removed and `Encoders.row()` was required. The two are not interchangeable across the 3.3 / 3.5 boundary.

---

## 2. Secondary issues (not the cause of the failure)

### 2.1 ContainerLocalizer — credentials file unreadable

```
Caused by: java.nio.file.AccessDeniedException: /var/run/cloudera-scm-agent/process/1547319984-yarn.NODEMANAGER/creds.localjceks
  at sun.nio.fs.UnixException.translateToIOException(UnixException.java:84)
  at sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:102)
  at sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:107)
  at sun.nio.fs.UnixFileSystemProvider.newByteChannel(UnixFileSystemProvider.java:214)
  at java.nio.file.Files.newByteChannel(Files.java:361)
  at java.nio.file.Files.newByteChannel(Files.java:407)
  at java.nio.file.spi.FileSystemProvider.newInputStream(FileSystemProvider.java:384)
  at java.nio.file.Files.newInputStream(Files.java:152)
  at org.apache.hadoop.security.alias.LocalKeyStoreProvider.getInputStreamForFile(LocalKeyStoreProvider.java:76)
  at org.apache.hadoop.security.alias.AbstractJavaKeyStoreProvider.locateKeystore(AbstractJavaKeyStoreProvider.java:325)
  at org.apache.hadoop.security.alias.AbstractJavaKeyStoreProvider.<init>(AbstractJavaKeyStoreProvider.java:86)
  at org.apache.hadoop.security.alias.LocalKeyStoreProvider.<init>(LocalKeyStoreProvider.java:56)
  at org.apache.hadoop.security.alias.LocalJavaKeyStoreProvider.<init>(LocalJavaKeyStoreProvider.java:42)
  at org.apache.hadoop.security.alias.LocalJavaKeyStoreProvider.<init>(LocalJavaKeyStoreProvider.java:34)
  at org.apache.hadoop.security.alias.LocalJavaKeyStoreProvider$Factory.createProvider(LocalJavaKeyStoreProvider.java:68)
  at org.apache.hadoop.security.alias.CredentialProviderFactory.getProviders(CredentialProviderFactory.java:91)
  at org.apache.hadoop.conf.Configuration.getPasswordFromCredentialProviders(Configuration.java:2450)
  ... 15 more
```

Preceding frames:

```
  at org.apache.hadoop.security.UserGroupInformation.ensureInitialized(UserGroupInformation.java:514)
  at org.apache.hadoop.security.UserGroupInformation.doSubjectLogin(UserGroupInformation.java:2008)
  at org.apache.hadoop.security.UserGroupInformation.createLoginUser(UserGroupInformation.java:743)
  at org.apache.hadoop.security.UserGroupInformation.getLoginUser(UserGroupInformation.java:693)
  at org.apache.hadoop.security.UserGroupInformation.getCurrentUser(UserGroupInformation.java:604)
  at org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer.main(ContainerLocalizer.java:494)
```

Local OS permissions on the NodeManager jceks file. Usually benign — the localizer proceeds without the credential provider.

### 2.2 NameNode standby

```
2026-09-17 14:10:32,169 INFO [main] org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer: Disk Validator: yarn.nodemanager.disk-validator is loaded.
2026-09-17 14:10:33,028 WARN [ContainerLocalizer Downloader] org.apache.hadoop.ipc.Client: Exception encountered while connecting to the server : org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.ipc.StandbyException): Operation category READ is not supported in state standby
```

The client contacted the standby NameNode first, then failed over. Expected HA behaviour.

### 2.3 Multiple SLF4J bindings

```
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/hadoop/yarn/nm/filecache/318085/log4j-slf4j-impl-2.18.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/hadoop/yarn/nm/usercache/<user>/filecache/18/str_projection_engine-7.8.2-RELEASE.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.9-1.cdh7.1.9.p1069.74330271/jars/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
```

The application fat jar is shipping its own SLF4J binding. It should be excluded — logging dependencies belong in `provided` scope on CDP.

---

## 3. Container log headers

```
Log Type: prelaunch.out
Log Upload Time: Thu Sep 17 14:10:39 +0200 2026
Log Length: 70
Setting up env variables
Setting up job resources
Launching container

Log Type: stderr
Log Upload Time: Thu Sep 17 14:11:20 +0200 2026
Log Length: 3261
```
