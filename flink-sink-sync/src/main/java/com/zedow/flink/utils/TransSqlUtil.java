package com.zedow.flink.utils;

import java.util.List;

/**
 * Author: zhujiangtao
 * Date: 2022-08-15
 * Desc: 自定义 Mysql DDL 转换成 Flink、Doris sql工具类
 */
public class TransSqlUtil {

    //转换成Flink sql等可利用的sql资源
    public String splitSql(List<LineListUtil> line, String tails) {
        StringBuilder sql = new StringBuilder();
        int i = 0;
        String flinkSqlLine;
        String[] dataTypeList;
        String mysqlDataType;
        String flinkDataType;

        while (i < (line.size() - 1)) {
            if (i == 0) {
                String[] splitLine = line.get(i).getCode().split(" ");
                flinkSqlLine = splitLine[0] + " "
                        + splitLine[1] + " tmpTableName1222"
                        + " (";
                sql.append(flinkSqlLine).append("\n");
                i++;
            } else if (i == 1) {
                flinkSqlLine = line.get(i).getCode().split("\\(")[0] + " PRIMARY KEY NOT ENFORCED,";
                sql.append(flinkSqlLine).append("\n");
                i++;
            } else if (i < (line.size() - 2)) {
                dataTypeList = line.get(i).getCode().split(" ");
                mysqlDataType = dataTypeList[3];
                flinkDataType = transforDataType(mysqlDataType);
                sql.append("  ").append(dataTypeList[2]).append(" ").append(flinkDataType).append(",").append("\n");
                i++;
            } else if (i == (line.size() - 2)) {
                dataTypeList = line.get(i).getCode().split(" ");
                mysqlDataType = dataTypeList[3];
                flinkDataType = transforDataType(mysqlDataType);
                sql.append("  ").append(dataTypeList[2]).append(" ").append(flinkDataType).append("\n").append(tails).append("\n");
                i++;
            }
        }
        return sql.toString();
    }

    //转换成Doris DDL sql等可利用的sql资源
    public String ddlSql(List<LineListUtil> line, String tails) {
        StringBuilder sql = new StringBuilder();
        int i = 0;
        String flinkSqlLine;

        while (i < (line.size() - 1)) {
            if (i == 0) {
                String[] splitLine = line.get(i).getCode().split(" ");
                flinkSqlLine = splitLine[0] + " "
                        + splitLine[1] + " tmpTableName1222"
                        + " (";
                sql.append(flinkSqlLine).append("\n");
                i++;
            } else if ((i >= 1) && (i < (line.size() - 2))) {
                String splitLine = line.get(i).getCode();
                String dorisDataType = transforDataType2(splitLine.split(" ")[3]);
                if (splitLine.contains("COMMENT"))
                    flinkSqlLine = splitLine.split(" ")[2] + " "
                            + dorisDataType + " COMMENT"
                            + splitLine.split("COMMENT")[1];
                else flinkSqlLine = splitLine.split(" ")[2] + " "
                        + dorisDataType + ",";
                sql.append(flinkSqlLine).append("\n");
                i++;
            } else if (i == (line.size() - 2)) {
                String splitLine = line.get(i).getCode();
                String dorisDataType = transforDataType2(splitLine.split(" ")[3]);
                if (splitLine.contains("COMMENT"))
                    flinkSqlLine = splitLine.split(" ")[2] + " "
                            + dorisDataType + " COMMENT"
                            + splitLine.split("COMMENT")[1].substring(0, splitLine.split("COMMENT")[1].length() - 1);
                else flinkSqlLine = splitLine.split(" ")[2] + " "
                        + dorisDataType;
                sql.append(flinkSqlLine).append("\n").append(tails).append("\n");
                i++;
            }
        }
        return sql.toString();
    }

    // Mysql DataType 转换成 FlinkSql DataType
    private static String transforDataType(String mysqlDataType) {
        if (mysqlDataType.contains("tinyint")) {
            return "tinyint";
        } else if (mysqlDataType.contains("bigint")) {
            return "bigint";
        } else if (mysqlDataType.contains("mediumint")) {
            return "int";
        } else if (mysqlDataType.contains("text")) {
            return "string";
        } else if (mysqlDataType.contains("int")) {
            return "int";
        } else if (mysqlDataType.contentEquals("datetime") || mysqlDataType.contains("timestamp")) {
            return "timestamp(3)";
        } else
            return mysqlDataType;
    }

    // Mysql DataType 转换成 Doris DataType
    private static String transforDataType2(String mysqlDataType) {
        if (mysqlDataType.contains("medium")) {
            return mysqlDataType.replace("medium", "");
        } else if (mysqlDataType.contains("varchar")) {
            return "string";
        } else if (mysqlDataType.contains("timestamp")) {
            return "datetime";
        } else if ((mysqlDataType.contains("decimal") && (Integer.parseInt(mysqlDataType.split(",")[1].replace(")", "")) > 9))
                || (mysqlDataType.contains("decimal") && (Integer.parseInt(mysqlDataType.split(",")[0].replace("decimal(", "")) > 27))) {
            if ((Integer.parseInt(mysqlDataType.split(",")[0].replace("decimal(", "")) > 27) && (Integer.parseInt(mysqlDataType.split(",")[1].replace(")", "")) > 9)) {
                return "decimal(27,8)";
            } else if ((Integer.parseInt(mysqlDataType.split(",")[0].replace("decimal(", "")) > 27) && (Integer.parseInt(mysqlDataType.split(",")[1].replace(")", "")) <= 9)) {
                return "decimal(27," + mysqlDataType.split(",")[1];
            } else if ((Integer.parseInt(mysqlDataType.split(",")[0].replace("decimal(", "")) <= 27) && (Integer.parseInt(mysqlDataType.split(",")[1].replace(")", "")) > 9)) {
                return mysqlDataType.split(",")[0] + ",8)";
            } else return mysqlDataType;
        }
            return mysqlDataType;
    }
}
