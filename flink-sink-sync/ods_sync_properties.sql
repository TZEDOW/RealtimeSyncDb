CREATE TABLE `ods_sync_properties` (
  `mysqlDb` varchar(50) NULL COMMENT "",
  `tableList` text NULL COMMENT "",
  `dorisLablePrefix` text NULL COMMENT "",
  `parallelism` text NULL COMMENT "",
  `checkpointInterval` text NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`mysqlDb`)
COMMENT "TableList配置表（用于Flink同步Doris）"
DISTRIBUTED BY HASH(`mysqlDb`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
)