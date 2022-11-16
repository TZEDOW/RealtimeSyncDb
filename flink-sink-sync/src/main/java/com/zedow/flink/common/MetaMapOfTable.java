package com.zedow.flink.common;

import com.zedow.flink.config.GlobalConfig;
import com.zedow.flink.utils.DruidDataSource;
import com.zedow.flink.utils.LineListUtil;
import com.zedow.flink.utils.MyHdfsUtil;
import com.zedow.flink.utils.TransSqlUtil;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: zhujiangtao
 * Date: 2022-09-14
 * Desc: 通过Druid连接池获取元数据配置信息,之后通过JDBC获取源表建表语句，并与Flink、Doris、Kafka的WITH信息一起封装
 */
public class MetaMapOfTable {

    private static String bootstrapServers = GlobalConfig.getConfig("bootstrapServers");
    private static String groupId = GlobalConfig.getConfig("groupId");
    private static String kafkaStartUpMode = GlobalConfig.getConfig("kafkaStartUpMode");

    private static final File META_DRUID_FILE = new MyHdfsUtil().readHDFSFile("metaconn_druid.properties");
    private static final File DORIS_DRUID_FILE = new MyHdfsUtil().readHDFSFile("doris_druid.properties");

    public Map<String, Map> getSqlMaps(String tableList) throws Throwable {

        Map<String, Map> metaMaps = new HashMap<>();
        LineListUtil lineListUtil = new LineListUtil();
        TransSqlUtil transSqlUtil = new TransSqlUtil();
        DruidDataSource druidDataSource = new DruidDataSource();

        DataSource metaDataSource = druidDataSource.getDataSource(META_DRUID_FILE);
        DataSource dorisDataSource = druidDataSource.getDataSource(DORIS_DRUID_FILE);

        String[] tableArray;
        tableArray = tableList.split(",");

        for (String tableName : tableArray) {
            // 访问dw_metadata.meta_import,查询odsTableName和connectId
            String importQuerySql = "select * from dw_metadata.meta_import where table_name = '" + tableName + "' and hive_table like '%" + tableName + "';";
            Connection impCon = metaDataSource.getConnection();
            Statement impSt = impCon.createStatement();
            ResultSet importResultSet = impSt.executeQuery(importQuerySql);

            int connectId;
            String databaseName;
            String odsTableName;
            if (!importResultSet.next()) {
                throw new Throwable("meta_import未配置" + tableName + "信息或配置填写错误，请前往处理");
            } else {
                connectId = importResultSet.getInt(2);
                databaseName = importResultSet.getString(3);
                odsTableName = importResultSet.getString(6).replace("src_", "ods_");
            }
            if (databaseName == null) {
                throw new Throwable("meta_import未配置" + tableName + "信息或配置填写错误，请前往处理");
            }
            impCon.close();
            impSt.close();
            importResultSet.close();

            // 访问dw_metadata.meta_connections,查询连接信息
            String metaQuerySql = "select * from dw_metadata.meta_connections where connection_id = " + connectId + ";";
            Connection metaCon = metaDataSource.getConnection();
            Statement metaConSt = metaCon.createStatement();
            ResultSet metaConResultSet = metaConSt.executeQuery(metaQuerySql);

            String hostname;
            int port;
            String username;
            String password;
            String jdbcExtend;
            String url;
            if (!metaConResultSet.next()) {
                throw new Throwable("meta_connections未配置" + tableName + "所属实例或配置填写错误，请前往处理");
            } else {
                hostname = metaConResultSet.getString(5);
                port = metaConResultSet.getInt(9);
                username = metaConResultSet.getString(7);
                password = metaConResultSet.getString(8);
                jdbcExtend = metaConResultSet.getString(10);
                url = "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName + "?" + jdbcExtend;
            }
            metaCon.close();
            metaConSt.close();
            metaConResultSet.close();

            // 访问数据源，查询DDL
            String showDdlSql = "show create table " + tableName;
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection tableCon = DriverManager.getConnection(url, username, password);
            Statement tableConSt = tableCon.createStatement();
            ResultSet tableConResultSet = tableConSt.executeQuery(showDdlSql);
            tableConResultSet.next();
            String mysqlDDL = tableConResultSet.getString(2);
            String primaryKey = mysqlDDL.split("PRIMARY KEY \\(")[1].split("\\)")[0];
            String tableComment = "";
            try {
                tableComment = mysqlDDL.split(" COMMENT=")[1];
            } catch (Exception ignored) {

            }
            tableCon.close();
            tableConSt.close();
            tableConResultSet.close();

            // 信息装到metaMap
            Map<String,String> metaMap = getTails(hostname, port, databaseName, tableName, username, password, odsTableName, bootstrapServers, groupId, kafkaStartUpMode);
            metaMap.put("mysqlDDL", mysqlDDL);
            metaMap.put("primaryKey", primaryKey);
            metaMap.put("tableComment",tableComment);
            metaMap.put("odsTableName", odsTableName);
            metaMap.put("hostname", hostname);
            metaMap.put("port", Integer.toString(port));
            metaMap.put("url", url);
            metaMap.put("username", username);
            metaMap.put("password", password);
            metaMap.put("databaseName", databaseName);
            metaMap.put("tableName", tableName);
            metaMap.put("bootstrapServers", bootstrapServers);
            metaMap.put("kafkaStartUpMode", kafkaStartUpMode);
            metaMap.put("groupId", groupId);

            // metaMap作为value装到Maps对应的key
            metaMaps.put(tableName, metaMap);

            // 查询Doris表元数据，若不存在则创建对应的Doris ods表
            String checkTableExistSql = "select count(*) from information_schema.TABLES where TABLE_NAME = \"" + odsTableName + "\";";
            Connection dorisCon = dorisDataSource.getConnection();
            Statement dorisConSt = dorisCon.createStatement();
            ResultSet dorisResultSet = dorisConSt.executeQuery(checkTableExistSql);
            dorisResultSet.next();
            if (dorisResultSet.getInt(1) == 0) {
            // Doris表不存在，开始创建
                List<LineListUtil> line = lineListUtil.getLineList(mysqlDDL.split("PRIMARY")[0]);
                String dorisConfig = metaMap.get("dorisConfig");
                String createDorisTable = transSqlUtil.ddlSql(line, dorisConfig)
                        .replace("tmpTableName1222", "ods." + odsTableName)
                        .replace(" PRIMARY KEY NOT ENFORCED,", ",")
                        .replace("UNIQUE KEY(`id`)", "UNIQUE KEY(" + primaryKey + ")")
                        .replace("BY HASH(`id`)", "BY HASH(" + primaryKey + ")")
                        .replace("COMMENT \"tableComment\"","COMMENT " + tableComment);
                Statement dorisCreatSt = dorisCon.createStatement();
                dorisCreatSt.executeUpdate(createDorisTable);
                dorisCreatSt.close();
            }
            dorisResultSet.close();
            dorisCon.close();
            dorisConSt.close();
        }

        return metaMaps;
    }

    private static Map<String, String> getTails (String hostname, Integer port, String databaseName, String tableName, String username, String password, String odsTableName, String bootstrapServers, String groupId, String kafkaStartUpMode) {
        Map<String, String> map = new HashMap<>();
        String cdcStartUpMode = "latest-offset";
        String mysqlCdcTails = " )WITH(\n" +
                "    'connector'= 'mysql-cdc'\n" +
                "   ,'hostname'= '" + hostname + "'\n" +
                "   ,'port'= '" + port + "'\n" +
                "   ,'username'= '" + username + "'\n" +
                "   ,'password'= '" + password + "'\n" +
                "   ,'server-time-zone' = 'Asia/Shanghai'\n" +
                "   ,'scan.startup.mode'= '" + cdcStartUpMode + "'\n" +
                "   ,'scan.incremental.snapshot.chunk.size'= '" + 8096 + "'\n" +
                "   ,'database-name'= '" + databaseName + "'\n" +
                "   ,'table-name'= '" + tableName + "'\n" +
                "   );";
        String kafkaUpsertTails = ")WITH(\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + odsTableName + "',\n" +
                "  'properties.client.id.prefix' = '" + odsTableName + "_'," + "\n" +
                "  'properties.bootstrap.servers' = '" + bootstrapServers + "',\n" +
                "  'key.json.ignore-parse-errors' = 'true',\n" +
                "  'value.json.fail-on-missing-field' = 'false',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json',\n" +
                "  'value.fields-include' = 'ALL'\n" +
                ");";
        String kafkaTails = ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '" + odsTableName + "',\n" +
                "    'properties.client.id.prefix' = '" + odsTableName + "_'," + "\n" +
                "    'properties.bootstrap.servers' = '" + bootstrapServers + "',\n" +
                "    'properties.group.id' = '" + groupId + "',\n" +
                "    'scan.startup.mode'= '" + kafkaStartUpMode + "',\n" +
                "    'format' = 'json',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.fields-include' = 'ALL'";
        String dorisConfig = " ) ENGINE=OLAP\n" +
                " UNIQUE KEY(`id`)\n" +
                " COMMENT \"tableComment\"\n" +
                " DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                " PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"in_memory\" = \"false\",\n" +
                " \"storage_format\" = \"V2\"\n" +
                " );";
        String dorisTails = ")with(\n" +
                "    'connector' = 'doris'\n" +
                "   ,'fenodes' = '172.28.28.109:8030'\n" +
                "   ,'username' = 'dorisapp'\n" +
                "   ,'password' = 'as8a@892nfasd!'\n" +
                "   ,'table.identifier' = 'ods." + odsTableName + "'\n" +
                "   ,'sink.max-retries' = '2'\n" +
                "   ,'sink.batch.interval' = '1s'\n" +
                "   ,'sink.enable-2pc'='true'\n" +
                "   ,'sink.enable-delete' = 'true'\n" +
                "   ,'sink.label-prefix' = '" + odsTableName + "_" + odsTableName.hashCode() + "'\n" +
                ");";
        String dorisAllParal = "EXECUTE CDCSOURCE " + odsTableName.replace("_" + tableName, "_allparal") + " WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname'= '" + hostname + "',\n" +
                "  'port'= '" + port + "',\n" +
                "  'username'= '" + username + "',\n" +
                "  'password'= '" + password + "',\n" +
                "  'source.server-time-zone'= 'Asia/Shanghai',\n" +
                "  'checkpoint' = '3000',\n" +
                "  'scan.startup.mode'= '" + cdcStartUpMode + "',\n" +
                "  'sink.sink.batch.interval' = '1s',\n" +
                "  'parallelism' = '1',\n" +
                "  'table-name' = '" + databaseName + "\\." + tableName + "',\n" +
                "  'sink.connector' = 'doris',\n" +
                "  'sink.fenodes' = '172.28.28.109:8030',\n" +
                "  'sink.username' = 'dorisapp',\n" +
                "  'sink.password' = 'as8a@892nfasd!',\n" +
                "  'sink.sink.max-retries' = '3',\n" +
                "  'sink.sink.enable-delete' = 'true',\n" +
                "  'sink.sink.enable-2pc'='true',\n" +
                "  'sink.sink.db' = 'ods',\n" +
                "  'sink.table.prefix' = '" + odsTableName.replace(tableName, "") + "',\n" +
                "  'sink.sink.label-prefix' = 'doris_${schemaName}_${tableName}_" + databaseName.hashCode() + "',\n" +
                "  'sink.sink.properties.format' = 'json',\n" +
                "  'sink.sink.properties.read_json_by_line' = 'true',\n" +
                "  'sink.table.identifier' = '${schemaName}.${tableName}'\n" +
                ")";
        map.put("mysqlCdcTails", mysqlCdcTails);
        map.put("kafkaUpsertTails", kafkaUpsertTails);
        map.put("kafkaTails", kafkaTails);
        map.put("dorisConfig", dorisConfig);
        map.put("dorisTails", dorisTails);
        map.put("dorisAllParal", dorisAllParal);
        return map;
    }
}
