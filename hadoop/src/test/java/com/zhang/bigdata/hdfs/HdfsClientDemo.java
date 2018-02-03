package com.zhang.bigdata.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

public class HdfsClientDemo {
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void init() throws Exception {
        configuration = new Configuration();
        //configuration.set("fs.defaultFS","hdfs://192.168.229.129:9000");
        //fileSystem =  FileSystem.get(configuration);
        fileSystem = FileSystem.get(new URI("hdfs://192.168.229.129:9000"), configuration, "hadoop");
    }

    @Test
    public void upload() throws IOException {
        fileSystem.copyFromLocalFile(new Path("D:\\pictures\\617ea91f.jpg"),new Path("/one.jpg"));
    }

    @Test
    public void download() throws IOException {
        fileSystem.copyToLocalFile(true,new Path("/one.jpg"), new Path("e:\\one.copy.jpg"),true);
    }

    @Test
    public void showConfiguration() {
        Iterator<Map.Entry<String, String>> iterator = configuration.iterator();
        while(iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            System.out.println(next.getKey() + "= =" + next.getValue());
        }
    }

    @Test
    public void uploadByStream() throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/BUILDING.txt"), true);
        FileInputStream fis = new FileInputStream("F:\\迅雷下载\\hadoop-2.7.5-src\\BUILDING.txt");
        IOUtils.copy(fis, fsDataOutputStream);
    }

    @Test
    public void downloadByStream() throws IOException {
        FSDataInputStream open = fileSystem.open(new Path("/uploadByStream.db"));
        FileOutputStream fos = new FileOutputStream("F:\\迅雷下载\\apache-maven-3.3.9-bin.zip.bak");
        IOUtils.copy(open, fos);
    }

    @Test//从文件获取特定范围的部分
    public void downloadByParticularRange() throws IOException {
        FSDataInputStream open = fileSystem.open(new Path("/BUILDING.txt"));
        //open.seek(2097152);
        FileOutputStream fos = new FileOutputStream("F:\\迅雷下载\\BUILDING.txt");
        //IOUtils.copy(open, fos);
        //IOUtils.copyLarge(open, fos, 2097152, 524288);
        IOUtils.copyLarge(open, System.out, 1024, 1024);
    }

    @After
    public void destroy() throws IOException {
        fileSystem.close();
    }
}
