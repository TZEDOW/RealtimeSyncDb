package com.zedow.flink.common;

import com.zedow.flink.common.PropsFromHDFS;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
/*import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;*/

import java.util.Properties;

/**
 * Author: zhujiangtao
 * Date: 2022-08-25
 * Desc: 自定义的Sink类（含Doris、Kafka）
 */
public class MySinkFunction {

    public static DorisSink buildDorisSink(String odsTableName,String dorisLablePrefix) throws Throwable {

        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();

        Properties dorisProps = PropsFromHDFS.getProps("doris_druid.properties");
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


    /*public static FlinkKafkaProducer<String> getKafkaSinkFunction(String odsTableName) throws Throwable {

        String path = System.getProperty("user.dir");
        File file = new File(path,"/rppet_properties/rppet_kafka.properties");
        BufferedReader kafkaConfReader = new BufferedReader(new FileReader(file));

        Properties producerProp = new Properties();
        producerProp.load(kafkaConfReader);
        producerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerProp.getProperty("bootstrapServer"));
        producerProp.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, producerProp.getProperty("transactionTimeout"));
        producerProp.setProperty(ProducerConfig.ACKS_CONFIG, producerProp.getProperty("acks"));

        return new FlinkKafkaProducer<>(odsTableName, (KafkaSerializationSchema<String>) (str, timestamp) -> new ProducerRecord<>(odsTableName, str.getBytes()), producerProp, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }*/
}
