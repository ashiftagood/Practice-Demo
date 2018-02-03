package com.zhang.bigdata.mapreduce.washlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogWashDriver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(LogWashDriver.class);
		job.setMapperClass(LogWashStepOne.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path("/home/zhangcun/bigdata/mapreduce/washlog/input"));
		FileOutputFormat.setOutputPath(job, new Path("/home/zhangcun/bigdata/mapreduce/washlog/output"));
		
		job.waitForCompletion(true);
	}
}
