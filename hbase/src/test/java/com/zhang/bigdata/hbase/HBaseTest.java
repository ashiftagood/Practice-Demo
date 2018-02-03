package com.zhang.bigdata.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HBaseTest {
	
	private Configuration conf;
	private Connection connection;
	private Table table_user;
	
	@Before
	public void init() throws Exception {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "vm01,vm02,vm03");
		conf.set("hbase.zookeeper.property.client.port", "2181");
		connection = ConnectionFactory.createConnection(conf);
		table_user = connection.getTable(TableName.valueOf("user"));
	}
	
	@Test
	public void createTable() throws Exception {
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf("user2");
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("info1");
		HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("info2");
		hTableDescriptor.addFamily(hColumnDescriptor1);
		hTableDescriptor.addFamily(hColumnDescriptor2);
		admin.createTable(hTableDescriptor);
	}
	
	@Test
	public void deleteTable() throws Exception {
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf("user2");
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}
	
	@Test
	public void insertData() throws Exception {
		List<Put> list = new ArrayList<>();
		Put put = new Put("12343".getBytes());
		put.addColumn("info1".getBytes(), "age".getBytes(), "100".getBytes());
		Put put1 = new Put("22".getBytes());
		put1.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes(233));
		put1.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("name"), Bytes.toBytes("six"));
		list.add(put);
		list.add(put1);
		table_user.put(list);
	}
	
	@Test//单条查询
	public void query01() throws Exception {
		Get get = new Get(Bytes.toBytes("1234"));
		Result result = table_user.get(get);
		byte[] value = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));
		System.out.println(Bytes.toString(value));
	}
	
	@Test//全表扫描
	public void query02() throws Exception {
		Scan scan = new Scan();
		ResultScanner scanner = table_user.getScanner(scan);
		scanner.forEach(sc -> System.out.println(sc.toString()));
	}
}
