package com.zhang.bigdata.mapreduce.sharedfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SharedFriendStep2Reducer extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        outKey.set(key);
        StringBuilder sb = new StringBuilder();
        for(Text t : values) {
            sb.append(t.toString()).append(",");
        }
        outValue.set(sb.toString());
        context.write(outKey, outValue);
    }
}
