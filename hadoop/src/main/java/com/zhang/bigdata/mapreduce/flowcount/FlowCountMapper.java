package com.zhang.bigdata.mapreduce.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String line = value.toString();
        String[] split = line.split("\t");
        long upFlow = Long.valueOf(split[split.length - 3]);
        long downFlow = Long.valueOf(split[split.length - 2]);
        context.write(new Text(split[1]),new FlowBean(upFlow, downFlow));
    }
}
