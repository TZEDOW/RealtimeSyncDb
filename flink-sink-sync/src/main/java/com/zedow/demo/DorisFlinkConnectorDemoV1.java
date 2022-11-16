package com.zedow.demo;


import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Author: zhujiangtao
 * Date: 2022-08-25
 * Desc: Flink Doris1.1 Connector Demo
 */
public class DorisFlinkConnectorDemoV1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.milliseconds(30000)));


        DorisSink.Builder<String> builder = DorisSink.builder();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();
        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        pro.setProperty("line_delimiter", "\n");
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("FE_IP:8030")
            .setTableIdentifier("test.test_flink")
            .setUsername("root")
            .setPassword("");
        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
            .setStreamLoadProp(pro)
            .setLabelPrefix("doris_test");


        builder.setDorisReadOptions(readOptionBuilder.build())
            .setDorisExecutionOptions(executionBuilder.build())
            .setSerializer(new SimpleStringSerializer())
            .setDorisOptions(dorisBuilder.build());

        env.fromElements("{\"id\": \"1\",\"name\": \"朱江涛\", \"age\": \"30\"}\n{\"id\": \"2\",\"name\": \"wangwu4\", \"age\": \"30\"}\n{\"id\": \"3\",\"name\": \"wangwu2\", \"age\": \"30\"}\n{\"id\": \"4\",\"name\": \"doris\", \"age\": \"30\"}\n{\"id\": \"5\",\"name\": \"doris1\", \"age\": \"30\"}\n{\"id\": \"6\",\"name\": \"doris2\", \"age\": \"30\"}").sinkTo(builder.build());

        env.execute("flink demo");

    }
}
