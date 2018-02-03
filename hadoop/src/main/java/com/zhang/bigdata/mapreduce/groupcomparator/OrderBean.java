package com.zhang.bigdata.mapreduce.groupcomparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {
	private String orderId;
	private String goodsId;
	private double amount;
	
	public OrderBean() {
	}

	public void setOrderBean(String orderId, String goodsId, double amount) {
		this.orderId = orderId;
		this.goodsId = goodsId;
		this.amount = amount;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getGoodsId() {
		return goodsId;
	}

	public void setGoodsId(String goodsId) {
		this.goodsId = goodsId;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		this.orderId = input.readUTF();
		this.goodsId = input.readUTF();
		this.amount = input.readDouble();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(orderId);
		output.writeUTF(goodsId);
		output.writeDouble(amount);
	}

	@Override
	public int compareTo(OrderBean o) {
		int r1 = this.orderId.compareTo(o.orderId);
		if(r1 == 0) {
			return Double.compare(o.amount, this.amount);
		}
		return r1;
	}

	@Override
	public String toString() {
		return "OrderBean [orderId=" + orderId + ", goodsId=" + goodsId + ", amount=" + amount + "]";
	}
	
	
}
