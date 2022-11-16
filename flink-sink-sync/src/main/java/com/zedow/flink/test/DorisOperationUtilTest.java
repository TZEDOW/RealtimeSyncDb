package com.zedow.flink.test;

import com.jcraft.jsch.Session;
import com.zedow.flink.utils.SshJdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.Properties;

/**
 * Author: zhujiangtao
 * Date: 2022-09-06
 * Desc: 自定义Doris JDBC 操作工具类
 *      适用于同步删除操作,以及自动创建Doris ods表(暂未应用)
 */
public class DorisOperationUtilTest {

    private static final Logger log = LoggerFactory.getLogger(DorisOperationUtilTest.class);
    private static int localPort = 7237;
    private static String localIp = "localhost";
    private static String sshIp = "172.28.28.99";
    private static String sshUser = "zhujiangtao";
    private static int sshPort = 22;
    private static String sshPwd = "xGIFYZq2";
    private static String usrHome = System.getProperty("user.home");
    private static String sshKeyPath = usrHome + "/.ssh/id_rsa";
    private static String path = System.getProperty("user.dir");

    public static Session getSession() throws Exception {

        return SshJdbcUtil.getSession(sshIp, sshUser, sshPort, sshPwd, sshKeyPath);
    }

    public static ResultSet getDorisTable () throws Exception {

        File file = new File(path,"/rppet_properties/rppet_doris.properties");
        BufferedReader dorisConfReader = new BufferedReader(new FileReader(file));
        Properties dorisProps = new Properties();
        dorisProps.load(dorisConfReader);

        String dorisConnIp = dorisProps.getProperty("dorisHostname");
        String dorisConnUser = dorisProps.getProperty("dorisUsername");
        String dorisConnPwd = dorisProps.getProperty("dorisPassword");
        int dorisConnPort = Integer.parseInt(dorisProps.getProperty("dorisJdbcPort"));
        String dorisConnDriver = "com.mysql.cj.jdbc.Driver";
        String dorisDB = "ods";

        Session dorisConnSession  = getSession();

        int dorisConnPortForwardingL = SshJdbcUtil.getPortForwardingL(dorisConnSession, localPort, dorisConnIp, dorisConnPort);

        Connection dorisConn =  SshJdbcUtil.getConnection(dorisConnDriver, localIp, dorisConnPortForwardingL, dorisDB, dorisConnUser, dorisConnPwd);

        Statement dorisConnStatement = dorisConn.createStatement();

        // 检查Doris是否存在对应的Sink表
        String checkTable="show tables in " + dorisDB + ";";

        ResultSet resultSet = dorisConnStatement.executeQuery(checkTable);

        if (!dorisConnStatement.isClosed()) dorisConnStatement.close();
        if (!dorisConn.isClosed()) dorisConn.close();
        if (dorisConnSession.isConnected()) dorisConnSession.disconnect();

        return resultSet;
    }

    public static void deleteFrom(String odsTableName, String filterContent) throws Throwable {

        File file = new File(path,"/druid_properties/doris_druid.properties");
        BufferedReader dorisConfReader = new BufferedReader(new FileReader(file));
        Properties dorisProps = new Properties();
        dorisProps.load(dorisConfReader);

        String dorisConnIp = dorisProps.getProperty("dorisHostname");
        String dorisConnUser = dorisProps.getProperty("username");
        String dorisConnPwd = dorisProps.getProperty("password");
        int dorisConnPort = Integer.parseInt(dorisProps.getProperty("dorisJdbcPort"));
        String dorisConnDriver = "com.mysql.cj.jdbc.Driver";
        String dorisDB = "ods";

        Session dorisConnSession  = getSession();

        int dorisConnPortForwardingL = SshJdbcUtil.getPortForwardingL(dorisConnSession, localPort, dorisConnIp, dorisConnPort);

        Connection dorisConn =  SshJdbcUtil.getConnection(dorisConnDriver, localIp, dorisConnPortForwardingL, dorisDB, dorisConnUser, dorisConnPwd);

        Statement dorisConnStatement = dorisConn.createStatement();

        String deleteSql = "delete from " + "ods." + odsTableName + " where " + filterContent + ";";

        int row = dorisConnStatement.executeUpdate(deleteSql);

        if(row != 0){
            log.info("delete from " + "ods." + odsTableName + " where " + filterContent + ";" + "-- deleted failed");
        }else{
            log.info("delete from " + "ods." + odsTableName + " where " + filterContent + ";" + "-- deleted success");
        }

        if (!dorisConnStatement.isClosed()) dorisConnStatement.close();
        if (!dorisConn.isClosed()) dorisConn.close();
        if (dorisConnSession.isConnected()) dorisConnSession.disconnect();
    }
}

