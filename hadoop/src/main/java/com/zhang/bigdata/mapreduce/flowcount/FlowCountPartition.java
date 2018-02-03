package com.zhang.bigdata.mapreduce.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowCountPartition extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String prefix = text.toString().substring(0,3);
        int intPre = Integer.parseInt(prefix);
        return intPre % 3;
    }
}
