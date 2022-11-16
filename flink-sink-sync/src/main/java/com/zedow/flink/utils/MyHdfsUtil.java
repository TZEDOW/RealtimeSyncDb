package com.zedow.flink.utils;

import com.zedow.flink.config.GlobalConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.nio.file.StandardCopyOption;
/**
 * Author: zhujiangtao
 * Date: 2022-09-26
 * Desc: 访问、读写HDFS中连接配置文件的工具类
 */

public class MyHdfsUtil {

    private static FileSystem getFiledSystem() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }

    public File readHDFSFile(String fileName){
        FSDataInputStream fsDataInputStream = null;
        String targetFilePath = System.getProperty("user.dir") + fileName;
        File targetFile = new File(targetFilePath);
        if(targetFile.exists()) targetFile.delete();

        try {
            Path path = new Path(GlobalConfig.getConfig("hdfsPath") + fileName);
            fsDataInputStream = getFiledSystem().open(path);
            java.nio.file.Files.copy(
                        fsDataInputStream,
                        targetFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fsDataInputStream != null){
                IOUtils.closeStream(fsDataInputStream);
            }
        }
        return targetFile;
    }

    public void writeHDFS(String localPath, String hdfsPath){
        FSDataOutputStream outputStream = null;
        FileInputStream fileInputStream = null;

        try {
            Path path = new Path(hdfsPath);
            outputStream = getFiledSystem().create(path);
            fileInputStream = new FileInputStream(new File(localPath));
            //输入流、输出流、缓冲区大小、是否关闭数据流，如果为false就在 finally里关闭
            IOUtils.copyBytes(fileInputStream, outputStream,4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fileInputStream != null){
                IOUtils.closeStream(fileInputStream);
            }
            if(outputStream != null){
                IOUtils.closeStream(outputStream);
            }
        }
    }
}
