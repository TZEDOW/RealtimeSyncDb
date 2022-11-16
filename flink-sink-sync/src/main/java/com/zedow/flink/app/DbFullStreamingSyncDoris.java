package com.zedow.flink.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zedow.flink.common.MySinkFunction;
import com.zedow.flink.common.MetaMapOfTable;
import com.zedow.flink.common.MyJsonDebeziumDeserializationSchema;
import com.zedow.flink.common.PropsFromHDFS;
import com.zedow.flink.utils.TableListUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Author: zhujiangtao
 * Date: 2022-09-14
 * Desc: Mysql整库同步Doris，支持Savepoint停止后新增表重启同步
 *      （采用了自定义序列器，支持增删改类型同步）
 */
public class DbFullStreamingSyncDoris {

    private static final Logger log = LoggerFactory.getLogger(DbFullStreamingSyncDoris.class);

    public static void main(String[] args) throws Throwable {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String db = parameterTool.get("db");
        String jobName = parameterTool.get("jobName");
        String maskMode;
        if (parameterTool.get("maskMode") == null){
            maskMode = "unmask";
        } else {
            maskMode = parameterTool.get("maskMode");
        }

        if (db == null || jobName == null) {
            throw new RuntimeException("请加入参数：--db xxx --jobName xxx同步Doris --maskMode mask/unmask(默认)");
        }

        Properties dorisProps = PropsFromHDFS.getProps("doris_druid.properties");
        String driver = dorisProps.getProperty("driverClassName");
        Class.forName(driver);
        String dorisURL = dorisProps.getProperty("url");
        String dorisUsername = dorisProps.getProperty("username");
        String dorisPassword = dorisProps.getProperty("password");

        Map<String, String> mapForJob = TableListUtil.getMapForJob(db, dorisURL, dorisUsername, dorisPassword);

        String tableList = mapForJob.get("tableList");
        String dorisLablePrefix = mapForJob.get("dorisLablePrefix");
        String parallelism = mapForJob.get("parallelism");
        String checkpointInterval = mapForJob.get("checkpointInterval");
        String[] tableArray = tableList.split(",");
        // finalTableList(db.table1,db.table2,db.table3)
        String fianlTableList = db + "." + tableList.replace(",","," + db + ".");

        // 对不同的表数据流进行循环遍历，依表名分配到对应的侧输出流
        Map<String, OutputTag<String>> outputTagHashMap = new HashMap<>();
        for (String tableName : tableArray) {
            outputTagHashMap.put(tableName, new OutputTag<String>(tableName) {
            });
        }

        // 预先设计好将每个table的元数据信息装填
        Map<String, Map> sqlMaps = new MetaMapOfTable().getSqlMaps(tableList);
        Map <String,String> sqlMap0 = sqlMaps.get(tableArray[0]);

        String mysqlHostname = sqlMap0.get("hostname");
        int mysqlPort = Integer.parseInt(sqlMap0.get("port"));
        String mysqlUsername = sqlMap0.get("username");
        String mysqlPassword = sqlMap0.get("password");

        String maskKeywordList;
        String unmaskingTable;
        if (maskMode.equalsIgnoreCase("mask")) {
            // 添加脱敏配置：脱敏列关键词&不对手机号脱敏的表名
            Properties maskingProps = PropsFromHDFS.getProps("masking.properties");
            maskKeywordList = maskingProps.getProperty("keywordList");
            unmaskingTable = maskingProps.getProperty("unmaskingTable");
        } else {
            maskKeywordList = "gfset9hhghdsree12aAwd";
            unmaskingTable = "";
        }

        // 使Debezium接受对DecimalFormat的序列化
        Properties prop = new Properties();
        prop.put("decimal.handling.mode", "string");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname(mysqlHostname)
            .port(mysqlPort)
            .databaseList(db)
            .tableList(fianlTableList)
            .username(mysqlUsername)
            .password(mysqlPassword)
            .scanNewlyAddedTableEnabled(true)
            .startupOptions(StartupOptions.latest())
            .debeziumProperties(prop)
            // 采用自定义序列器,简化binlog,转换时间字段值
            .deserializer(new MyJsonDebeziumDeserializationSchema("Asia/Shanghai",maskKeywordList,unmaskingTable))
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(parallelism));
        env.enableCheckpointing(Long.parseLong(checkpointInterval));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 精准一次
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // sets the checkpoint storage where checkpoint snapshots will be written
        String checkpointStorage = "hdfs://emr-cluster/flink/flink-checkpoints/" + db;
        checkpointConfig.setCheckpointStorage(checkpointStorage);
        // 同一时间只允许一个 checkpoint 进行
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // only two consecutive checkpoint failures are tolerated,最多容忍两个连续的checkpoint失败
        checkpointConfig.setTolerableCheckpointFailureNumber(2);
        // 开启在 job 中止后仍然保留的 externalized checkpoints
        checkpointConfig.isExternalizedCheckpointsEnabled();
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 开启实验性的 unaligned 不对齐checkpoints,来解耦 Checkpoint 机制与反压机制，优化高反压情况下的 Checkpoint 表现
        // 由于要持久化缓存数据，State Size 会有比较大的增长，磁盘负载会加重。 随着 State Size 增长，作业恢复时间可能增长，运维管理难度增加。 目前看来，Unaligned Checkpoint 更适合容易产生高反压同时又比较重要的复杂作业。对于像数据 ETL 同步等简单作业，更轻量级的 Aligned Checkpoint 显然是更好的选择。
        // Unaligned Checkpoint 主要解决在高反压情况下作业难以完成 Checkpoint 的问题，同时它以磁盘资源为代价，避免了 Checkpoint 可能带来的阻塞，有利于提升 Flink 的资源利用率。随着流计算的普及，未来的 Flink 应用大概会越来越复杂，在未来经过实战打磨完善后 Unaligned Checkpoint 很有可能会取代 Aligned Checkpoint 成为 Flink 的默认 Checkpoint 策略。

        checkpointConfig.enableUnalignedCheckpoints();

        DataStreamSource<String> cdcSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");
        SingleOutputStreamOperator<String> process = cdcSource.process(new ProcessFunction<String, String>() {
            @SneakyThrows
            @Override
            public void processElement(String row, ProcessFunction<String, String>.Context context, Collector<String> collector) {
                JSONObject rowJson = JSON.parseObject(row);
                String op = rowJson.getString("op");
                JSONObject source = rowJson.getJSONObject("source");
                String table = source.getString("table");
                Map<String, String> sqlMap = sqlMaps.get(table);
                String odsTableName = sqlMap.get("odsTableName");

                // 增删改操作
                if (!"d".equals(op)) {
                    String value = rowJson.getJSONObject("after").toJSONString();
                    for (String s : tableArray) {
                        if (s.equals(table)) {
                            context.output(outputTagHashMap.get(table), value);
                        }
                    }
                } else {
                    log.info("delete operation...");

                    try {
                        String primaryKey = sqlMap.get("primaryKey");

                        String[] filterList = new String[primaryKey.split(",").length];
                        StringBuilder filterPreContent = new StringBuilder();

                        String valueBefore = trimBothEndsChars(rowJson.getJSONObject("before").toJSONString().split("\\{")[1], "}");
                        String[] valueBeforeSplit = valueBefore.split(",");

                        int fi = 0;
                        for (String s : valueBeforeSplit) {
                            String sKey = s.split("\"")[1];
                            String sValue = s.split(":")[1];

                            if (primaryKey.contains(sKey)) {
                                filterList[fi] = sKey + " = " + sValue.replace("\"", "'");//从这些字段判断删除
                                fi++;
                            }
                        }

                        for (String f : filterList) {
                            filterPreContent.append(f).append(" and ");
                        }
                        // 执行删除
                        String deleteSql = "delete from " + "ods." + odsTableName + " where " + trimBothEndsChars(filterPreContent.toString(), " and ") + ";";

                        Connection connection = DriverManager.getConnection(dorisURL, dorisUsername, dorisPassword);
                        Statement statement = connection.createStatement();
                        int count;
                        count = statement.executeUpdate(deleteSql);
                        if (count != 0) {
                            log.info(deleteSql + "-- deleted failed");
                        } else {
                            log.info(deleteSql + "-- deleted success");
                        }
                        statement.close();
                        connection.close();
                    } catch( Exception ignore){

                        }
                    }
            }
        });

        // 增改数据覆盖进Doris
        for (String s : tableArray) {
            try {
                Map<String,String> sqlMap = sqlMaps.get(s);
                String odsTableName = sqlMap.get("odsTableName");
                process.getSideOutput(outputTagHashMap.get(s)).sinkTo(MySinkFunction.buildDorisSink(odsTableName,dorisLablePrefix));
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }

        env.execute(jobName);
    }

    private static String trimBothEndsChars(String srcStr, String splitter) {
        String regex = "^" + splitter + "*|" + splitter + "*$";
        return srcStr.replaceAll(regex, "");
    }

}
