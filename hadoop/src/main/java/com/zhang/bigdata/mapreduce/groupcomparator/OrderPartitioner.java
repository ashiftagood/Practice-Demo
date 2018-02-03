package com.zhang.bigdata.mapreduce.groupcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {

	@Override
	public int getPartition(OrderBean bean, NullWritable arg1, int numPart) {
		
		return (bean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPart;
	}

}
