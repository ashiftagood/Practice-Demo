package com.zhang.bigdata.mapreduce.sharedfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SharedFriendMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String line = value.toString();
        String[] split = line.split(":");
        outValue.set(split[0]);
        String[] split1 = split[1].split(",");
        for(String s : split1) {
            outKey.set(s);
            context.write(outKey, outValue);
        }
    }
}
