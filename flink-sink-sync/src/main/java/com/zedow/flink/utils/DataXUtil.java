package com.zedow.flink.utils;

import com.zedow.flink.config.GlobalConfig;

import java.io.*;

/**
 * Author: zhujiangtao
 * Date: 2022-09-19
 * Desc: DataXUtil同步工具类
 */
public class DataXUtil {

    private static String jsonFiles = GlobalConfig.getConfig("jsonFiles");
    private static String dataXFilePath = GlobalConfig.getConfig("dataXFilePath");

    public static void exeDatax(String jsonPath) throws IOException {

        //获取datax.py文件的规范化绝对路径
        File dataXFile = new File(dataXFilePath);
        String dataXPath = dataXFile.getCanonicalPath();

        File jsonFile = new File(jsonPath);
        jsonPath = jsonFile.getCanonicalPath();
        System.out.println("------------------start----------------------");
        //name为文件json文件名
        String command = "python " + dataXPath + " " + jsonPath;
        System.out.println(command);
        //通过调用cmd执行python执行 (/usr/bin/python)
        Process pr = Runtime.getRuntime().exec(command);
        BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
        String line;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        in.close();
        try {
            pr.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static String createHeader() {
        String headerJson = "{\n" +
                "    \"setting\": {},\n" +
                "    \"job\": {\n" +
                "        \"setting\": {\n" +
                "            \"speed\": {\n" +
                "                \"channel\": 1\n" +
                "            }\n" +
                "        },\n" +
                "        \"content\": [\n" +
                "            {\n";
        return headerJson;
    }

    public static String createReader(Integer databaseType, String readerUsername, String readerPwd, String readerJdbcUrl, String readerTable ,String filter) {
        String readerName;
        //数据库类型 （1-MySqlOracle，2-Oracle）
        if (databaseType == 1) {
            readerName = "mysqlreader";
        } else if (databaseType == 2) {
            readerName = "oraclereader";
        } else {
            throw new RuntimeException("不支持除Mysql 和 Oracle 外数据库类型!");
        }
        String readerJson =
                "                \"reader\": {\n" +
                "                    \"name\": " + "\"" + readerName + "\"" + ",\n" +
                "                    \"parameter\": {\n" +
                "                        \"username\": " + "\"" + readerUsername + "\"" + ",\n" + // readerUsername
                "                        \"password\": " + "\"" + readerPwd + "\"" + ",\n" + // readerPwd
                "                        \"column\": [\n" +
                "                            \"*\"\n" +
                "                        ],\n" +
                "                        \"connection\": [\n" +
                "                            {\n" +
                "                                \"jdbcUrl\": ["+ "\"" + readerJdbcUrl + "\"" + "],\n" +
                "                                \"table\": ["+ "\"" + readerTable + "\"" + "]\n" + // readerTable
                "                            }\n" +
                "                        ],\n" +
                "                        \"where\": " + "\"" + filter + "\"" + "\n" + // filterName filterTime
                "                    }\n" +
                "                }," + "\n" +
                "";

        return readerJson;
    }

    public static String createWriter(Integer databaseType, String writerUsername, String writerPwd, String writerJdbcUrl, String writerTable) {

        String writerName;
        //数据库类型 （1-MySqlOracle，2-Oracle）
        if (databaseType == 1) {
            writerName = "mysqlwriter";
        } else if (databaseType == 2) {
            writerName = "oraclewriter";
        } else {
            throw new RuntimeException("不支持除Mysql 和 Oracle 外数据库类型!");
        }

        String writerJson = "                \"writer\": {\n" +
                "                    \"name\": " + "\"" + writerName + "\"" + ",\n" +
                "                    \"parameter\": {\n" +
                "                        \"writeMode\": \"insert\",\n" +
                "                        \"password\": " + "\"" + writerPwd + "\"" + ",\n" +
                "                        \"username\": " + "\"" + writerUsername + "\"" + ",\n" +
                "                        \"column\": [\"*\"],\n" +
                "                        \"connection\": [\n" +
                "                            {\n" +
                "                                \"jdbcUrl\": " + "\"" + writerJdbcUrl + "\"" + ",\n" +
                "                                \"table\": ["+ "\"" + writerTable + "\"" + "\n" +
                "                                ]\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    }\n" +
                "                }";

        return writerJson;

    }

    public static String createTransformer(String transformerName, String[] columnIndex, Integer mode ) {
        String transformerJson = "";
        if (columnIndex.length == 1) {
            transformerJson = ",\n" +
                    "                \"transformer\":\n" +
                    "                    {\n" +
                    "                    \"name\": \"" + transformerName + "\",\n" +
                    "                    \"parameter\": \n" +
                    "                        {\n" +
                    "                            \"columnIndex\":" + columnIndex[0] + ",\n" +
                    "                            \"paras\":[\"" + mode + "\"]" + "\n" +
                    "                        }  \n" +
                    "                    }\n";
        } else if (columnIndex.length > 1) {
            String headerTransJson =
                    ",\n" +
                    "                \"transformer\": [\n";
            String tailTransJson =
                    "                ]";
            StringBuilder midTransJson = new StringBuilder();
            for (Object index:columnIndex) {
                midTransJson.append("                    {\n" +
                        "                    \"name\": \"" + transformerName + "\",\n" +
                        "                    \"parameter\": \n" +
                        "                        {\n" +
                        "                            \"columnIndex\":" + index + ",\n" +
                        "                            \"paras\":[\"" + mode + "\"]" + "\n" +
                        "                        }  \n" +
                        "                    },\n");
            }
            transformerJson = headerTransJson + midTransJson.toString().replace("},","}") + tailTransJson;
        }
        return transformerJson;
    }

    public static String createTail() {
        String tailJson = "\n" +
                        "            }\n" +
                        "        ]\n" +
                        "    }\n" +
                        "}";
        return tailJson;
    }

    public static String createJobJson(String json,String table,String time) {

        String timeFileName = time.replace("-", "").replace(" ","").replace(":","");
        String timeFilePath = jsonFiles + timeFileName;
        File timeFile =new File(timeFilePath);
        //如果文件夹不存在则创建
        if  (!timeFile.exists() && !timeFile.isDirectory()) timeFile.mkdir();

        String jsonPath = timeFilePath + "/" + "datax_" + table + ".json";
        File jsonFile = new File(jsonPath);
        if(jsonFile.exists()) jsonFile.delete();

        //将json写入文件中
        byte[] jsonBytes = json.getBytes();
        try {
            FileOutputStream fos = new FileOutputStream(jsonPath);
            fos.write(jsonBytes);
            fos.flush();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jsonPath;
    }
}


