package com.zedow.flink.app;

import com.zedow.flink.common.DataxJob;
import com.zedow.flink.common.MetaMapOfTable;
import com.zedow.flink.common.PropsFromHDFS;
import com.zedow.flink.utils.DataXUtil;
import com.zedow.flink.utils.TableListUtil;

import java.util.*;

/**
 * Author: zhujiangtao
 * Date: 2022-09-19
 * Desc: 批量补充TableList同步过程中的缺失数据
 */

public class DbFullBatchSyncDoris {

    public static void main(String[] args) throws Throwable {
        String maskMode = args[0];
        String db = args[1];
        String startTime = args[2] + " " + args[3];
        if (maskMode == null || db == null || startTime == null) {
            throw new RuntimeException("请加入参数，是否脱敏 数据库名 yyyy-MM-dd HH:mm:ss，例如: mask/unmask hospital_db 2022-08-01 00:00:00");
        }

        // 添加脱敏配置：脱敏列关键词&不脱敏手机号电话号的表名
        Properties maskingProps = PropsFromHDFS.getProps("masking.properties");
        String maskKeywordList = maskingProps.getProperty("keywordList");
        String unmaskingTable = maskingProps.getProperty("unmaskingTable");

        Properties dorisProps = PropsFromHDFS.getProps("doris_druid.properties");
        String driver = dorisProps.getProperty("driverClassName");
        Class.forName(driver);
        String dorisURL = dorisProps.getProperty("url");
        String dorisUsername = dorisProps.getProperty("username");
        String dorisPassword = dorisProps.getProperty("password");

        Map<String, String> mapForJob = TableListUtil.getMapForJob(db, dorisURL, dorisUsername, dorisPassword);

        String tableList = mapForJob.get("tableList");
        String[] tableArray = tableList.split(",");
        Map<String, Map> sqlMaps = new MetaMapOfTable().getSqlMaps(tableList);

        Map <String,String> sqlMap0 = sqlMaps.get(tableArray[0]);
        String url = sqlMap0.get("url");
        String mysqlURL;
        if (url.contains("serverTimezone=Hongkong") || url.contains("serverTimezone=Asia/Shanghai") || url.contains("serverTimezone=GMT%2B8")) {
            mysqlURL = url;
        } else {
        mysqlURL = sqlMap0.get("url") + "&serverTimezone=GMT%2B8";}
        String mysqlUsername = sqlMap0.get("username");
        String mysqlPassword = sqlMap0.get("password");

        StringBuilder result = new StringBuilder();
        String filter;
        for (String mysqlTable : tableArray) {
            Map<String, String> sqlMap = sqlMaps.get(mysqlTable);
            String dorisTable = sqlMap.get("odsTableName");
            String mysqlDDL = sqlMap.get("mysqlDDL");

            if (mysqlDDL.contains("last_modify")){
                filter = "last_modify>='" + startTime + "'";
            } else if (!mysqlDDL.contains("time")||!mysqlDDL.contains("date")){
                filter = "1=1";
            } else if (mysqlDDL.contains("update_timestamp")){
                filter = "update_timestamp>='" + startTime + "'";
            } else if (mysqlDDL.contains("update_time")){
                filter = "update_time>='" + startTime + "'";
            } else if (mysqlDDL.contains("update_date")){
                filter = "update_date>='" + startTime + "'";
            } else if (mysqlDDL.contains("operator_time")){
                filter = "operator_time>='" + startTime + "'";
            } else if (mysqlDDL.contains("occur_time")&&mysqlDDL.contains("in_bill_time")){
                filter = "occur_time>='" + startTime + "'" + " or in_bill_time>='" + startTime + "'";
            } else if (mysqlDDL.contains("occur_time")){
                filter = "occur_time>='" + startTime + "'";
            } else if (mysqlDDL.contains("in_bill_time")){
                filter = "in_bill_time>='" + startTime + "'";
            } else {
                throw new Throwable(mysqlTable + "的更新时间字段标识未录入此程序中" + "\n" +
                        "以下Table执行成功：\n" + result.toString());
            }

            String transformerJson;

            if (maskMode.equalsIgnoreCase("mask"))
            transformerJson = DataxJob.getTransforJson(mysqlDDL,mysqlTable,maskKeywordList,unmaskingTable);
            else transformerJson = "";

            String json = DataXUtil.createHeader() + DataXUtil.createReader(1,mysqlUsername,mysqlPassword,mysqlURL,mysqlTable,filter)
                    + DataXUtil.createWriter(1,dorisUsername,dorisPassword,dorisURL,dorisTable) + transformerJson + DataXUtil.createTail();
            String jsonPath = DataXUtil.createJobJson(json,mysqlTable,startTime);

            try {DataXUtil.exeDatax(jsonPath);}
            catch (Exception e)
            {throw new Throwable (e + "\n" + "以下Table执行成功：" + result.toString());
            }

            result.append(mysqlTable).append("\n");
        }

        System.out.println("运行结束");
    }
}
