package com.zedow.flink.test;

import com.jcraft.jsch.Session;
import com.zedow.flink.config.GlobalConfig;
import com.zedow.flink.utils.LineListUtil;
import com.zedow.flink.utils.SshJdbcUtil;
import com.zedow.flink.utils.TransSqlUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Author: zhujiangtao
 * Date: 2022-08-15
 * Desc: 通过SSH连接JDBC，获取元数据配置信息及源表信息，并与Flink、Doris、Kafka的WITH信息一起封装
 */
public class MetaMapOfTableTest {

    private static String path = System.getProperty("user.dir");

    public Map<String, String> getSqlMap(String tableName) throws Throwable {

        LineListUtil lineListUtil = new LineListUtil();
        TransSqlUtil transSqlUtil = new TransSqlUtil();

        String bootstrapServers = GlobalConfig.getConfig("bootstrapServers");
        String groupId = GlobalConfig.getConfig("groupId");
        String kafkaStartUpMode = GlobalConfig.getConfig("kafkaStartUpMode");

        String sshIp = GlobalConfig.getConfig("sshIp");
        String sshUser = GlobalConfig.getConfig("sshUser");
        int sshPort = Integer.parseInt(GlobalConfig.getConfig("sshPort"));
        String sshPwd = GlobalConfig.getConfig("sshPwd");

        String usrHome = System.getProperty("user.home");
        String sshKeyPath = usrHome + "/.ssh/id_rsa";

        int localPort = 7213;
        String localIp = "localhost";

        // meta_connections config
        String metaConnIp = GlobalConfig.getConfig("metaConnIp");
        int metaConnPort = Integer.parseInt(GlobalConfig.getConfig("metaConnPort"));
        String metaConnDriver = GlobalConfig.getConfig("metaConnDriver");
        String metaConnUser = GlobalConfig.getConfig("metaConnUser");
        String metaConnPwd = GlobalConfig.getConfig("metaConnPwd");
        String metaConnDB = GlobalConfig.getConfig("metaConnDB");


        Session metaConnSession = SshJdbcUtil.getSession(sshIp, sshUser, sshPort, sshPwd, sshKeyPath);

        // create local broker port
        int metaConnPortForwardingL = SshJdbcUtil.getPortForwardingL(metaConnSession, localPort, metaConnIp, metaConnPort);

        Connection metaConn = SshJdbcUtil.getConnection(metaConnDriver, localIp, metaConnPortForwardingL, metaConnDB, metaConnUser, metaConnPwd);

        Statement metaConnStatement = metaConn.createStatement();
        String importQuerySql = "select * from " + metaConnDB + ".meta_import where table_name = '" + tableName + "' and hive_table like '%" + tableName + "';";
        ResultSet importResultSet = metaConnStatement.executeQuery(importQuerySql);
        Integer connectId = null;
        String databaseName;
        String odsTableName;
        if (!importResultSet.next()) {
            metaConnSession.disconnect();
        }else
            connectId = importResultSet.getInt(2);
            databaseName = importResultSet.getString(3);
            odsTableName = importResultSet.getString(6).replace("src_", "ods_");
        if (connectId == null || databaseName == null) {
            metaConnSession.disconnect();
            throw new Throwable(tableName+"meta_import的connection_id填写错误或不存在此表，请检查输入表名或修改connection_id");
        }

        String metaQuerySql = "select * from " + metaConnDB + ".meta_connections where connection_id = " + connectId + ";";
        ResultSet metaConnResultSet = metaConnStatement.executeQuery(metaQuerySql);
        String hostname = null;
        if (!metaConnResultSet.next()) {
            metaConnSession.disconnect();
        }else
            hostname = metaConnResultSet.getString(5);
        if (hostname == null) {
            metaConnSession.disconnect();
            throw new Throwable("meta_connections未配置" + tableName + "所属实例或meta_import的connection_id填写错误，请前往处理");
        }
        int port = metaConnResultSet.getInt(9);
        String username = metaConnResultSet.getString(7);
        String password = metaConnResultSet.getString(8);

        /* String defaultDb = metaConnResultSet.getString(6);
        String jdbcExtend = metaConnResultSet.getString(10);
        String db_url = "jdbc:mysql://" + hostname + ":" + port + "/" + defaultDb + "?" + jdbcExtend; */

        Map<String,String> metaMap = getTails(hostname, port, databaseName, tableName, username, password, odsTableName, bootstrapServers, groupId, kafkaStartUpMode);

        // close connection
        if (!metaConnResultSet.isClosed()) metaConnResultSet.close();
        if (!metaConnStatement.isClosed()) metaConnStatement.close();
        if (!metaConn.isClosed()) metaConn.close();
        if (metaConnSession.isConnected()) metaConnSession.disconnect();

        // mysql JDBC config
        String mysqlIp = hostname;
        String mysqlDriver = "com.mysql.cj.jdbc.Driver";
        String mysqlDB = databaseName;

        Session session = SshJdbcUtil.getSession(sshIp, sshUser, sshPort, sshPwd, sshKeyPath);

        int portForwardingL = SshJdbcUtil.getPortForwardingL(session, localPort, mysqlIp, port);

        Connection connection = SshJdbcUtil.getConnection(mysqlDriver, localIp, portForwardingL, mysqlDB, username, password);

        Statement statement = connection.createStatement();
        String sql;
        sql = "show create table " + tableName;
        ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        String mysqlDDL = resultSet.getString(2);
        String primaryKey = mysqlDDL.split("PRIMARY KEY \\(")[1].split("\\)")[0];
        String tableComment = mysqlDDL.split(" COMMENT=")[1];

        if (!resultSet.isClosed()) resultSet.close();
        if (!statement.isClosed()) statement.close();
        if (!connection.isClosed()) connection.close();
        if (session.isConnected()) session.disconnect();

        metaMap.put("mysqlDDL",mysqlDDL);
        metaMap.put("primaryKey",primaryKey);
        metaMap.put("tableComment",tableComment);
        metaMap.put("odsTableName",odsTableName);

        metaMap.put("hostname",hostname);
        metaMap.put("port",Integer.toString(port));
        metaMap.put("username",username);
        metaMap.put("password",password);
        metaMap.put("databaseName",databaseName);
        metaMap.put("tableName",tableName);

        metaMap.put("bootstrapServers",bootstrapServers);
        metaMap.put("kafkaStartUpMode",kafkaStartUpMode);
        metaMap.put("groupId",groupId);

        // 查询Doris表元数据，若不存在则创建对应的Doris ods表
        File file = new File(path,"/sync-properties/doris_druid.properties");
        BufferedReader dorisConfReader = new BufferedReader(new FileReader(file));
        Properties dorisProps = new Properties();
        dorisProps.load(dorisConfReader);

        String dorisConnIp = dorisProps.getProperty("dorisHostname");
        String dorisConnUser = dorisProps.getProperty("username");
        String dorisConnPwd = dorisProps.getProperty("password");
        int dorisConnPort = Integer.parseInt(dorisProps.getProperty("dorisJdbcPort"));
        String dorisConnDriver = "com.mysql.cj.jdbc.Driver";
        String dorisDB = "ods";

        Session dorisConnSession  = DorisOperationUtilTest.getSession();

        int dorisConnPortForwardingL = SshJdbcUtil.getPortForwardingL(dorisConnSession, localPort, dorisConnIp, dorisConnPort);

        Connection dorisConn =  SshJdbcUtil.getConnection(dorisConnDriver, localIp, dorisConnPortForwardingL, dorisDB, dorisConnUser, dorisConnPwd);

        Statement dorisConnStatement = dorisConn.createStatement();
        String checkTableExistSql = "select count(*) from information_schema.TABLES where TABLE_NAME = \"" + odsTableName + "\";";

        ResultSet dorisResultSet = dorisConnStatement.executeQuery(checkTableExistSql);
        dorisResultSet.next();
        if (dorisResultSet.getInt(1) == 0) {
            // Doris表不存在，开始创建
            List<LineListUtil> line = lineListUtil.getLineList(mysqlDDL.split("PRIMARY")[0]);
            String dorisConfig = metaMap.get("dorisConfig");
            String createDorisTable = transSqlUtil.ddlSql(line, dorisConfig)
                    .replace("tmpTableName1222", "ods." + odsTableName)
                    .replace(" PRIMARY KEY NOT ENFORCED,", ",")
                    .replace("UNIQUE KEY(`id`)", "UNIQUE KEY(" + primaryKey + ")")
                    .replace("BY HASH(`id`)", "BY HASH(" + primaryKey + ")");
            Statement dorisCreatPst = dorisConn.createStatement();
            dorisCreatPst.executeUpdate(createDorisTable);
            dorisCreatPst.close();
        }
        dorisResultSet.close();
        dorisConn.close();
        dorisConnStatement.close();
        dorisConnSession.disconnect();

        return metaMap;
    }

    private static Map getTails(String hostname, Integer port, String databaseName, String tableName, String username, String password, String odsTableName, String bootstrapServers, String groupId, String kafkaStartUpMode) {

        Map<String,String> map = new HashMap<>();
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

        String dorisAllParal = "EXECUTE CDCSOURCE " + odsTableName.replace("_" + tableName,"_allparal") + " WITH (\n" +
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
                "  'sink.table.prefix' = '" + odsTableName.replace(tableName,"") + "',\n" +
                "  'sink.sink.label-prefix' = 'doris_${schemaName}_${tableName}_" + databaseName.hashCode() +"',\n" +
                "  'sink.sink.properties.format' = 'json',\n" +
                "  'sink.sink.properties.read_json_by_line' = 'true',\n" +
                "  'sink.table.identifier' = '${schemaName}.${tableName}'\n" +
                ")";

        map.put("mysqlCdcTails", mysqlCdcTails);
        map.put("kafkaUpsertTails",kafkaUpsertTails);
        map.put("kafkaTails",kafkaTails);
        map.put("dorisConfig",dorisConfig);
        map.put("dorisTails",dorisTails);
        map.put("dorisAllParal",dorisAllParal);

        return map;
    }
}
