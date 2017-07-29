package com.zzj.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: 小郑
 * Date: 2017-07-25
 * Time: 15:13
 */
public class HadoopApi {
    FileSystem fileSystem=null;
    public static void main(String[] args) throws IOException {
        //1、跟HDFS系统建立连接
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.16.130:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        //2、打开一个输入点
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/jdk-8u131-linux-x64.tar.gz"));
        //3、打开一个本地的输出流文件
        OutputStream outputStream = new FileOutputStream("d://jdk-8u131-linux-x64.tar.gz");
        //4、拷贝IN->OUT
        IOUtils.copyBytes(fsDataInputStream, outputStream, 1024, true);
    }

    @Before
    public void testinit() throws IOException {
        //1、跟HDFS系统建立连接
        Configuration configuration = new Configuration();
       // configuration.set("fs.defaultFS", "hdfs://192.168.16.130:9000");
        configuration.set("fs.defaultFS", "hdfs://192.168.0.144:9000");
        System.setProperty("HADOOP_USER_NAME","root");
        this.fileSystem = FileSystem.get(configuration);
    }

    @Test
    public void testDown() throws IOException {
        //2、打开一个输入点
//        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/jdk-8u131-linux-x64.tar.gz"));
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/README.txt"));
        //3、打开一个本地的输出流文件
        OutputStream outputStream = new FileOutputStream("d://README.txt");
        //4、拷贝IN->OUT
        IOUtils.copyBytes(fsDataInputStream, outputStream, 1024, true);
    }
    @Test
    public void testUpload() throws IOException {
        InputStream inputStream=new FileInputStream("d://wordcount.xml");
        FSDataOutputStream fsDataOutputStream=fileSystem.create(new Path("/zz01/wordcount03.xml"));
        org.apache.commons.io.IOUtils.copy(inputStream,fsDataOutputStream);
    }

    @Test
    public void testMkdir() throws IOException {
        fileSystem.mkdirs(new Path("/zz01"));
    }

    @Test
    public void testDel() throws IOException {
        fileSystem.delete(new Path("/wordcount1"),true);
    }

    @After
    public void testClos() throws IOException {
        fileSystem.close();
    }


}
