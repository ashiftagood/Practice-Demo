package com.zhang.bigdata.mapreduce.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        long upFlow = 0L;
        long downFlow = 0L;
        for(FlowBean flowBean : values) {
            upFlow += flowBean.getUpFLow();
            downFlow += flowBean.getDownFLow();
        }
        context.write(key, new FlowBean(upFlow, downFlow));
    }
}
