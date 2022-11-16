package com.zedow.flink.test;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
/*import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;*/

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

/**
 * Author: zhujiangtao
 * Date: 2022-08-25
 * Desc: create my defined sink function
 */
public class MySinkFunctionTest {

    static DorisSink buildDorisSink(String table, String dorisLablePrefix) throws Throwable {

        String path = System.getProperty("user.dir");
        File file = new File(path,"/druid_properties/doris_druid.properties");
        BufferedReader dorisConfReader = new BufferedReader(new FileReader(file));

        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();

        //ForLocalTest
        String odsTableName;
        if (table.equals("users_cdc")){
            odsTableName = "users_cdc";
        }else{
            odsTableName = "orders_test";
        }


        Properties dorisProps = new Properties();
        dorisProps.load(dorisConfReader);

        dorisBuilder.setFenodes(dorisProps.getProperty("dorisFenodes"))
                .setTableIdentifier("ods." + odsTableName)
                .setUsername(dorisProps.getProperty("username"))
                .setPassword(dorisProps.getProperty("password"));

        Properties streamLoadProps = new Properties();
        //json data format
        streamLoadProps.setProperty("format", "json");
        streamLoadProps.setProperty("read_json_by_line", "true");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setDeletable(true)
                .setMaxRetries(3)
                .setLabelPrefix(dorisLablePrefix + odsTableName) //streamload label prefix
                .setStreamLoadProp(streamLoadProps).build();

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer()) //serialize according to string
                .setDorisOptions(dorisBuilder.build());

        return builder.build();
    }


    /*public static FlinkKafkaProducer<String> getKafkaSinkFunction(String table) throws Throwable {

        String path = System.getProperty("user.dir");
        File file = new File(path,"/rppet_properties/rppet_kafka.properties");
        BufferedReader kafkaConfReader = new BufferedReader(new FileReader(file));

        String sinkTopic = new MetaMapOfTableTest().getSqlMap(table).get("odsTableName");

        Properties producerProp = new Properties();
        producerProp.load(kafkaConfReader);
        producerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerProp.getProperty("bootstrapServer"));
        producerProp.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, producerProp.getProperty("transactionTimeout"));
        producerProp.setProperty(ProducerConfig.ACKS_CONFIG, producerProp.getProperty("acks"));

        return new FlinkKafkaProducer<>(table, (KafkaSerializationSchema<String>) (str, timestamp) -> new ProducerRecord<>(sinkTopic, str.getBytes()), producerProp, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }*/
}
