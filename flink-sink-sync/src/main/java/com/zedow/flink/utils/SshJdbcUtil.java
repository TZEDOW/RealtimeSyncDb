package com.zedow.flink.utils;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Author: zhujiangtao
 * Date: 2022-08-15
 * Desc: SSH connect JDBC 工具类
 *       用于本地连接JDBC
 */
public class SshJdbcUtil {

    public static Session getSession(String sshIp,
                                     String sshUser,
                                     int sshPort,
                                     String sshPwd,
                                     String sshKeyPath) throws JSchException {
        JSch jSch = new JSch();
        if(sshKeyPath != null) jSch.addIdentity(sshKeyPath);
        Session sesion = jSch.getSession(sshUser, sshIp, sshPort);
        sesion.setPassword((sshPwd == null) ? "" : sshPwd);
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        sesion.setConfig(config);
        sesion.connect();
        return sesion;
    }
    // create local broker port
    public static int getPortForwardingL(Session sesion,
                                         int loaclPort,
                                         String mysqlIp,
                                         int mysqlPort) throws JSchException {
        return sesion.setPortForwardingL(loaclPort, mysqlIp, mysqlPort);
    }

    public static Connection getConnection(String driver,
                                           String localIp,
                                           int portForwardingL,
                                           String db,
                                           String user,
                                           String pwd) throws Exception {
        String mysqlUrl = "jdbc:mysql://" + localIp + ":" + portForwardingL + "/" + db + "?characterEncoding=utf8&useSSL=false";
        Class.forName(driver);
        return DriverManager.getConnection(mysqlUrl, user, pwd);
    }
}
