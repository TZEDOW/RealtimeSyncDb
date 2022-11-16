package com.zedow.flink.test;

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
public class MySqlTransFunctionTest {

    public static void main(String[] args) throws Throwable {
        String tableList = "orders_test";
//                "hospital_db.erp_out_bill_detail_batch,hospital_db.erp_out_bill_detail,hospital_db.erp_out_bill,hospital_db.erp_inventory,hospital_db.erp_in_bill_detail,hospital_db.erp_in_bill,hospital_db.md_product,hospital_db.md_product_category,hospital_db.crm_cards_records,hospital_db.crm_cards";
        String[] tableArray;
        tableArray = tableList.split(",");

        String odsTableName;

        for (String table : tableArray) {
            String tableName;
            if (table.contains(".")) {
                tableName = table.replace(".", " ").split(" ")[1];
            } else tableName = table;
            Map<String, String> tailsMap = new MetaMapOfTableTest().getSqlMap(tableName);
            odsTableName = tailsMap.get("odsTableName");

            // Doris DDL
            System.out.println(getDorisDdlSql(tailsMap));

            // Per-Job CDC Sync Doris
            System.out.println(getFlinkCdcSql(tailsMap));
            System.out.println(getDorisSinkSql(tailsMap));

            System.out.println("insert into " + odsTableName + " select * from cdc_" + tableName + ";");
        }
    }

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

