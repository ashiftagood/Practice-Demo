package com.zhang.bigdata.mapreduce.groupcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * reducer 用来分组
 * @author zhangcun
 *
 */
public class BeanGroupComparator extends WritableComparator {

	public BeanGroupComparator() {
		super(OrderBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;
		return aBean.getOrderId().compareTo(bBean.getOrderId());
	}
	
	
}
