package com.zedow.flink.common;

import com.zedow.flink.utils.MyHdfsUtil;

import java.io.*;
import java.util.Properties;

/**
 * Author: zhujiangtao
 * Date: 2022-10-12
 * Desc: 获取放置在HDFS上的配置文件 doris_druid.properties、masking.properties
 */
public class PropsFromHDFS {

    public static Properties getProps(String fileName) throws IOException {

        MyHdfsUtil myHdfsUtil = new MyHdfsUtil();
        File file = myHdfsUtil.readHDFSFile(fileName);

        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        Properties props = new Properties();
        props.load(bufferedReader);

        return props;
    }
}
