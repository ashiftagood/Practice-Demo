package com.zhang.bigdata.mapreduce.sharedfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SharedFriendReducer extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        outValue.set(key);

        List<String> list = new ArrayList<>();

        for(Text text : values) {
            list.add(text.toString());
        }
        String pre;
        String suf;
        for(int i = 0; i < list.size(); i ++) {
            pre = list.get(i);
            for(int j = i+1; j < list.size(); j++ ) {
                suf = list.get(j);
                int k = pre.compareTo(suf);
                if(k < 0) {
                    outKey.set(pre+"-"+suf);
                } else {
                    outKey.set(suf+"-"+pre);
                }
                context.write(outKey, outValue);
            }
        }

    }

}
