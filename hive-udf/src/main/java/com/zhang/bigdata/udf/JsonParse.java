package com.zhang.bigdata.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.alibaba.fastjson.JSONObject;

public class JsonParse extends UDF {
	
	public String evaluate(String jsonLine) {
		
		JSONObject jsonObject = JSONObject.parseObject(jsonLine);
		
		String result = jsonObject.getString("movie") + "\t" + jsonObject.getString("rate") + "\t" + jsonObject.getString("timeStamp") + "\t" + jsonObject.getString("uid");
		
		return result;
	}
}
