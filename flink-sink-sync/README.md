# 实时同步工具 —— Zedow

### 简介

采用Java语言，基于Apache Flink 1.14.4-Scala-2.12实现，致力于新瑞鹏宠物医疗集团的实时数仓建设。

### 特点

以Apache Flink为基础，实现连接OLAP Doris和Kafka等框架的实时同步和计算工具。

主要功能及目标如下：

- 支持灵活、可拓展的实时同步；
- 支持灵活、可拓展的批量同步；
- 支持Jar包提交到任务平台（如Dinky、StreamPark）进行监控；
- 支持批量补全实时同步缺失的数据；
- 支持对敏感数据（身份证、手机号等）实时/批量自定义脱敏处理；
- 支持对接元数据管理库（dw_metadata），与T+1离线共用元数据资源；
- 支持从任务外部编辑TableList、Doris实例、元数据管理库实例的配置信息并存储（主要场景是Mysql和HDFS）；
- 支持自动获取Table List中每个数据表的元数据信息，包括且不限于数据表的连接信息、CREATE DDL SQL以及PRIMARY KEY等；
- 支持自动检测Doris并批量创建Unique表；
- 支持批量自动生成Flink CDC SQL、Flink Kafka SQL、Doris DDL SQL、Flink Doris SQL、insert into等语句；
- 更多功能等待探索开发 ......

### 原理

利用MySQL CDC Connector 2.2.0新增功能Scan Newly Added Tables实现同库多表灵活读取。

Flink任务自动读取数据库的binlog信息，binlog数据流经分流处理后收集到不同侧输出流，最后分发侧输出流信息进入不同Doris Unique表。如果binlog中记录为删除操作，则依据主键信息，删除Doris表中对应的行数据。

### 如何使用

打包获取flink-sink-sync的Jar文件，上传至FLINK_HOME/jobs目录下或上传至StreamPark平台；

在Doris的ods.ods_sync_properties和HDFS的/flink/sync-properties目录下编辑自定义配置。

#### 虚拟机执行任务

- 启动：在FLINK_HOME目录下执行

```shell
./bin/flink run --detached \
-t yarn-per-job \
-Dyarn.application.name=[自定义Yarn任务名称] \
-Djobmanager.memory.process.size=3200mb \
-Dtaskmanager.memory.process.size=5120mb \
-Dtaskmanager.numberOfTaskSlots=4 \
-Denv.java.opts="-Dfile.encoding=UTF-8" \
 jobs/flink-sink-sync.jar --db [数据库名称] --jobName [自定义Flink任务名称]
```
建议[自定义Yarn任务名称]=[自定义Flink任务名称]

- 停止：在FLINK_HOME目录下执行

```shell
./bin/flink cancel -s hdfs://emr-cluster/flink/flink-savepoints/[数据库名称] -Dyarn.application.id=application_xxxxxxxxxxxxx_xxxxxx cca7bc1061d61cf15238e92312c2fc20
```

​		<!--样例：cca7bc1061d61cf15238e92312c2fc20为JobId-->

​		停止之后，界面会显示savepoint路径信息

```
Suspending job "cca7bc1061d61cf15238e92312c2fc20" with a savepoint.
Savepoint completed. Path: file:/flink/flink-savepoints/hospital_db/savepoint-cca7bc-bb1e257f0dab
```

- 修改TableList配置表(位置Doris/ods.ods_sync_properties)的tableList信息，更新同步表名

  注：tableList格式必须为tableName1,tableName2,tableName3...（以此类推）

  <!--样例：pos_orders_tracking,pos_orders_detail,pos_orders_master-->

- 从savepoint重启Flink任务，在FLINK_HOME目录下执行

```shell
./bin/flink run --detached \
-t yarn-per-job \
-Dyarn.application.name=[自定义Yarn任务名称] \
-Djobmanager.memory.process.size=3200mb \
-Dtaskmanager.memory.process.size=5120mb \
-Dtaskmanager.numberOfTaskSlots=4 \
-Denv.java.opts="-Dfile.encoding=UTF-8" \
--fromSavepoint hdfs://emr-cluster/flink/flink-savepoints/hospital_db/savepoint-cca7bc-bb1e257f0dab \
jobs/flink-sink-sync.jar --db [数据库名称] --jobName [自定义任务名称]
```

- 批量补全实时同步的缺失数据，在FLINK_HOME目录下执行（需要以DbFullBatchSyncDoris为主类打包）
```shell
hadoop jar jobs/flink-batch-sync.jar 参数1[数据库名称] 参数2[yyyy-MM-dd] 参数3[HH:mm:ss]
```
  或直接执行 sh batch-sync.sh 参数1[脱敏模式：mask/unmask] 参数2[数据库名称] 参数3[yyyy-MM-dd] 参数4[HH:mm:ss]

  注：1. 有些表Source和Sink字段数量不同，DataX报警不会终止此脚本运行，运行结束后，ctrl+F检索"不相等"，找出同步失败的表，手动改json文件进而同步；
      2. 迁移腾讯云后，我们将字段不相等的表重建，杜绝上述异常；
      3. 批同步自动生成的json文件，会保存在FLINK_HOME/jobs/jsonFiles目录下，以时间戳分区

#### StreamPark平台执行实时任务

- 主页Application -> Add New

### For More
- 打包Jar：注意在依赖中指定主类<mainClass>com.rppet.bigdata.flink.Xxx</mainClass>，FLINK_HOME/lib下包含的依赖要在打包时provided
- 如果从上一次停止的状态重启同步，最好修改配置表ods_sync_properties的dorisLablePrefix信息，建议修改为表名+当前时间戳，防止重复
- 可在配置表Doris/ods.ods_sync_properties中修改同步任务的并行度和checkpointInterval参数
- 可在HDFS：/flink/sync-properties目录下修改Doris连接信息、元数据管理信息以及脱敏关键字等信息
- 执行批量[自动生成Flink SQL操作](https://git.rp-field.com/bigdata/realtime_datawarehouse/-/blob/master/flink-sink-sync/src/main/java/com/rppet/bigdata/flink/app/test/MySqlTransFunctionTest.java)

