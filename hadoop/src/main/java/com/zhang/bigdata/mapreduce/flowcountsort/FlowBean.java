package com.zhang.bigdata.mapreduce.flowcountsort;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private long upFLow;
    private long downFLow;
    private long sumFlow;

    public FlowBean() {
    }

    public FlowBean(long upFLow, long downFLow) {
        this.upFLow = upFLow;
        this.downFLow = downFLow;
        this.sumFlow = upFLow + downFLow;
    }

    public long getUpFLow() {
        return upFLow;
    }

    public void setUpFLow(long upFLow) {
        this.upFLow = upFLow;
    }

    public long getDownFLow() {
        return downFLow;
    }

    public void setDownFLow(long downFLow) {
        this.downFLow = downFLow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFLow);
        dataOutput.writeLong(downFLow);
        dataOutput.writeLong(sumFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.upFLow = dataInput.readLong();
        this.downFLow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upFLow + "\t" + downFLow + "\t" + sumFlow;
    }
}
