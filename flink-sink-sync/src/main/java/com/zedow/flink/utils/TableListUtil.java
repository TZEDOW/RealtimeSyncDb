package com.zedow.flink.utils;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: zhujiangtao
 * Date: 2022-09-23
 * Desc: 获取TableList的类
 */
public class TableListUtil {

    public static Map<String, String> getMapForJob(String db, String dorisURL, String dorisUsername, String dorisPassword) throws Throwable {

        Map<String, String> tablePropMap = new HashMap<>();

        Connection propConn = DriverManager.getConnection(dorisURL, dorisUsername, dorisPassword);
        Statement propStatement = propConn.createStatement();
        String tableListSql = "select * from ods.ods_sync_properties where mysqlDB = '" + db + "';";
        ResultSet tableProps = propStatement.executeQuery(tableListSql);

        if (!tableProps.next()) {
            throw new Throwable("Doris的ods.ods_sync_properties表中未配置" + db + "数据库表相关信息，请前往处理");
        } else {
            tablePropMap.put("tableList",tableProps.getString(2));
            tablePropMap.put("dorisLablePrefix",tableProps.getString(3));
            tablePropMap.put("parallelism",tableProps.getString(4));
            tablePropMap.put("checkpointInterval",tableProps.getString(5));
        }
        propConn.close();
        propStatement.close();
        tableProps.close();

        return tablePropMap;
    }
}