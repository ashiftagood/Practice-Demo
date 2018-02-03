package com.zhang.bigdata.mapreduce.flowcountsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyBean implements WritableComparable<KeyBean> {
    private String key;
    private long sumFlow;

    public KeyBean() {
    }

    public KeyBean(String key, long sumflow) {
        this.key = key;
        this.sumFlow = sumflow;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumflow) {
        this.sumFlow = sumflow;
    }

    public int compareTo(KeyBean o) {
        return Long.compare(o.getSumFlow(), sumFlow);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(key);
        dataOutput.writeLong(sumFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.key = dataInput.readUTF();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return key;
    }
}
