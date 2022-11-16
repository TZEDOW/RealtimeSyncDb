package com.zedow.flink.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zedow.flink.common.MyJsonDebeziumDeserializationSchema;
/*import MySinkFunctionTest;*/
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Properties;

/**
 * Author: zhujiangtao
 * Date: 2022-09-01
 * Desc: Database full sync sink Kafka
 */
public class MysqldbFullSyncKafkaDev {

    public static void main(String[] args) throws Throwable {

        String path = System.getProperty("user.dir");
        File file1 = new File(path,"/druid_properties/gateway_druid.properties");
        File file2 = new File(path,"/mysqldb_tablelist/gateway_test.properties");

        BufferedReader mBufferedReader = new BufferedReader(new FileReader(file1));
        BufferedReader tBufferedReader = new BufferedReader(new FileReader(file2));

        Properties mysqlProps = new Properties();
        mysqlProps.load(mBufferedReader);

        Properties tableProps = new Properties();
        tableProps.load(tBufferedReader);

        String mysqlHostname = mysqlProps.getProperty("mysqlHostname");
        int mysqlPort = Integer.parseInt(mysqlProps.getProperty("mysqlPort"));
        String mysqlUsername = mysqlProps.getProperty("username");
        String mysqlPassword = mysqlProps.getProperty("password");

        String mysqlDB = tableProps.getProperty("mysqlDB");
        String tableList = tableProps.getProperty("tableList");

        String[] tableArray;
        tableArray = tableList.split(",");
        String[] finalTableArray = new String[tableArray.length];
        HashMap<String, OutputTag<String>> outputTagHashMap = new HashMap<>();

        for (int i = 0; i < tableArray.length; i++) {
            String tableName = tableArray[i].split("\\.")[1];
            finalTableArray[i] = tableName;
            outputTagHashMap.put(tableName,new OutputTag<String>(tableName){});
        }

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .databaseList(mysqlDB)
                .tableList(tableList)
                .username(mysqlUsername)
                .password(mysqlPassword)
                .scanNewlyAddedTableEnabled(true)
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyJsonDebeziumDeserializationSchema("Asia/Shanghai","",""))
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(tableProps.getProperty("parallelism")));
        env.enableCheckpointing(Long.parseLong(tableProps.getProperty("checkpointInterval")));

        DataStreamSource<String> cdcSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");

        SingleOutputStreamOperator<String> process = cdcSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String row, ProcessFunction<String, String>.Context context, Collector<String> collector) {
                JSONObject rowJson = JSON.parseObject(row);
                JSONObject source = rowJson.getJSONObject("source");
                String table = source.getString("table");
                String value = rowJson.getJSONObject("after").toJSONString();

                for (String s : finalTableArray) {
                    if (s.equals(table)) {
                        context.output(outputTagHashMap.get(table), value);
                    }
                }
            }
        });

        /*for (String s : finalTableArray) {
            process.getSideOutput(outputTagHashMap.get(s))
                    .addSink(MySinkFunctionTest.getKafkaSinkFunction(s));
        }*/

        env.execute("Full Database Sync Kafka ");
    }
}
