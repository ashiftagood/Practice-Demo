package com.zhang.bigdata.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 相当于一个yarn客户端，需要封装mr程序的相关运行参数，指定jar包，最后提交给yarn
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终结果的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job的原始数据文件
        FileInputFormat.setInputPaths(job, new Path("/home/zhangcun/bigdata/wordcount/input"));

        //指定job的最终输出结果
        FileOutputFormat.setOutputPath(job, new Path("/home/zhangcun/bigdata/wordcount/output"));

        //job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
