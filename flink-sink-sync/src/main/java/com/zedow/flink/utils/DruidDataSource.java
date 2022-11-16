package com.zedow.flink.utils;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Author: zhujiangtao
 * Date: 2022-09-13
 * Desc: 获取Druid Datasource
 */
public class DruidDataSource implements Serializable {

    private static final long serialVersionUID = 1L;

    public DataSource getDataSource(File file) {
        DataSource dataSource = null;
        try {
            InputStream is = new FileInputStream(file);

            Properties prop = new Properties();
            prop.load(is);

            dataSource = DruidDataSourceFactory.createDataSource(prop);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return dataSource;
    }
}
