package com.zedow.flink.common;

import com.zedow.flink.utils.LineListUtil;
import com.zedow.flink.utils.TransSqlUtil;

import java.util.List;
import java.util.Map;


/**
 * Author: zhujiangtao
 * Date: 2022-08-15
 * Desc: 依据元数据配置表自动生成各类Flink、Doris、Kafka Sql
 *       适用于自动生成sql语句，以及为自动创建Doris表提供方法
 */
public class MySqlTransFunction {

    public static  String getFlinkCdcSql(Map<String, String> map) {
        TransSqlUtil transSqlUtil = new TransSqlUtil();
        String mysqlDDL = map.get("mysqlDDL");
        String mysqlCdcTails = map.get("mysqlCdcTails");
        String tableName = map.get("tableName");
        List<LineListUtil> line = new LineListUtil().getLineList(mysqlDDL.split("PRIMARY")[0]);
        return transSqlUtil.splitSql(line, mysqlCdcTails).replace("tmpTableName1222", "cdc_" + tableName);
    }

    public static  String getFlinkUpsertKafkaSql(Map<String, String> map) {
        TransSqlUtil transSqlUtil = new TransSqlUtil();
        String mysqlDDL = map.get("mysqlDDL");
        List<LineListUtil> line = new LineListUtil().getLineList(mysqlDDL.split("PRIMARY")[0]);

        String odsTableName = map.get("odsTableName");
        String kafkaUpsertTails = map.get("kafkaUpsertTails");
        return transSqlUtil.splitSql(line, kafkaUpsertTails).replace("tmpTableName1222", odsTableName);
    }

    public static  String getFlinkKafkaSql(Map<String, String> map) {
        TransSqlUtil transSqlUtil = new TransSqlUtil();
        String mysqlDDL = map.get("mysqlDDL");
        List<LineListUtil> line = new LineListUtil().getLineList(mysqlDDL.split("PRIMARY")[0]);
        String odsTableName = map.get("odsTableName");
        String kafkaTails = map.get("kafkaTails");
        return transSqlUtil.splitSql(line, kafkaTails).replace("tmpTableName1222", odsTableName).replace(" PRIMARY KEY NOT ENFORCED,", ",");
    }

    public static  String getDorisDdlSql(Map<String, String> map) {
        TransSqlUtil transSqlUtil = new TransSqlUtil();
        String mysqlDDL = map.get("mysqlDDL");
        List<LineListUtil> line = new LineListUtil().getLineList(mysqlDDL.split("PRIMARY")[0]);
        String odsTableName = map.get("odsTableName");
        String dorisConfig = map.get("dorisConfig");
        String tableComment = map.get("tableComment");
        return transSqlUtil.ddlSql(line, dorisConfig)
                .replace("tmpTableName1222", "ods." + odsTableName)
                .replace(" PRIMARY KEY NOT ENFORCED,", ",")
                .replace("UNIQUE KEY(`id`)", "UNIQUE KEY(" + map.get("primaryKey") + ")")
                .replace("BY HASH(`id`)", "BY HASH(" + map.get("primaryKey") + ")")
                .replace("COMMENT \"tableComment\"","COMMENT " + tableComment);
    }

    public static  String getDorisSinkSql(Map<String, String> map) {
        TransSqlUtil transSqlUtil = new TransSqlUtil();
        String mysqlDDL = map.get("mysqlDDL");
        List<LineListUtil> line = new LineListUtil().getLineList(mysqlDDL.split("PRIMARY")[0]);
        String odsTableName = map.get("odsTableName");
        String dorisTails = map.get("dorisTails");
        return transSqlUtil.splitSql(line, dorisTails).replace("tmpTableName1222", odsTableName).replace(" PRIMARY KEY NOT ENFORCED,", ",");
    }

    public static String getDorisAllParal(Map<String, String> map) {
        return map.get("dorisAllParal");
    }

    public static String createKafkaTopic(Map<String, String> map) {
        String odsTableName = map.get("odsTableName");
        String bootstrapServers = map.get("bootstrapServers");
        return "bin/kafka-topics.sh --bootstrap-server " + bootstrapServers + " --create  --replication-factor 1 --partitions 1 --topic " + odsTableName;
    }

}

