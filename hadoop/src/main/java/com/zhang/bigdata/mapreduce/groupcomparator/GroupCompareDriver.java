package com.zhang.bigdata.mapreduce.groupcomparator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 分组排序
 * @author zhangcun
 *
 */
public class GroupCompareDriver {
	
	static class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
		private OrderBean orderBean = new OrderBean(); 
		@Override
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			orderBean.setOrderBean(fields[0], fields[1], Double.parseDouble(fields[2]));
			context.write(orderBean, NullWritable.get());
		}
		
	}
	
	static class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

		@Override
		protected void reduce(OrderBean arg0, Iterable<NullWritable> arg1,Context arg2) throws IOException, InterruptedException {
			arg2.write(arg0, NullWritable.get());
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(GroupCompareDriver.class);
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setPartitionerClass(OrderPartitioner.class);
		job.setGroupingComparatorClass(BeanGroupComparator.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path("/home/zhangcun/bigdata/mapreduce/groupcomparator/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/zhangcun/bigdata/mapreduce/groupcomparator/output"));

        boolean result = job.waitForCompletion(true);
        //System.exit(result?0:1);
		System.out.println(result);
	}
}
