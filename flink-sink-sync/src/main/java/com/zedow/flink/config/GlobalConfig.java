package com.zedow.flink.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: zhujiangtao
 * Date: 2022-10-18
 * Desc: 创建集中的全局配置(没必要也不能配置到远程HDFS)，方便集群迁移后调整
 */
public class GlobalConfig {
    public static String getConfig(String configName) {
        Map<String,String> configMap = new HashMap();

        // HDFS sync-properties文件路径配置（用于实时&离线批量同步）
        configMap.put("hdfsPath","hdfs://emr-cluster/flink/sync-properties/");

        // DataX 同步配置（用于离线批量同步 -> DbFullBatchSyncDoris）
        configMap.put("jsonFiles","/usr/lib/flink-current/jobs/jsonFiles/");
        configMap.put("dataXFilePath","/home/zedow/opt/apps/ecm/service/datax/bin/datax.py");

        //Kafka配置
        configMap.put("bootstrapServers","xxx.xx.xx.xx:9092");
        configMap.put("groupId","big-data");
        configMap.put("kafkaStartUpMode","earliest-offset");

        // ssh登录配置（用于本地生成Sql -> MySinkFunctionTest）
        configMap.put("sshIp","xxx.xx.xx.xx");
        configMap.put("sshUser","");
        configMap.put("sshPort","22");
        configMap.put("sshPwd","");

        // meta_connections配置（用于本地生成Sql -> MySinkFunctionTest）
        configMap.put("metaConnIp","xxx.xx.xx.xxx");
        configMap.put("metaConnPort","3306");
        configMap.put("metaConnDriver","com.mysql.cj.jdbc.Driver");
        configMap.put("metaConnUser","");
        configMap.put("metaConnPwd","");
        configMap.put("metaConnDB","dw_metadata");

        return configMap.get(configName);
    }
}
