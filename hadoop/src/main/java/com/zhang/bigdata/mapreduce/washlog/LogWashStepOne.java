package com.zhang.bigdata.mapreduce.washlog;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogWashStepOne extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private static Map<String, String> sessionCache = new HashMap<>();
	private static long sessionTimeout = 30*60*1000;
	private static SimpleDateFormat parseSdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Text text = new Text();
	private NullWritable nw = NullWritable.get();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String result = washLog(line);
		if(result != null) {
			text.set(result);
			context.write(text, nw);
		}
	}
	
	private static String washLog(String src) {
		String[] fields = src.split(" ");
		String ip = fields[0];
		String cookie = ip + "-cookie";
		Date date = getDateTimeStr(fields[3].substring(1));
		String session = getSession(ip, date);
		String[] urls = getUrls(fields);
		
		if(urls != null) {
			String url = urls[0];
			String referal = urls[1];
			return sdf.format(date) + "\t" + ip + "\t" + cookie + "\t" + session + "\t" + url + "\t" + referal;
		}
		
		return null;
	}
	
	private static String[] getUrls(String[] src) {
		int length = src.length;
		String url = "";
		String referal = "";
		if(length >= 6) {
			for(int i = 5; i < length; i++) {
				String s = src[i];
				if("".equals(url) || "".equals(referal)) {
					if(checkUrl(s)) {
						url = s;
					}
					if(checkReferal(s)) {
						referal = s.substring(1, s.length()-1);
					}
				} else {
					break;
				}
			}
			if("".equals(url)) {
				return null;
			}
			return new String[]{url, referal};
		}
		
		return null;
	} 
	
	private static boolean checkReferal(String s) {
		return s.startsWith("\"http://") || s.startsWith("\"https://");
	}
	
	private static boolean checkUrl(String s) {
		boolean start = s.startsWith("/");
		boolean endJpg = s.contains(".jpg");
		boolean endPng = s.contains(".png");
		boolean endGif = s.contains(".gif");
		boolean endCss = s.contains(".css");
		boolean endJs = s.contains(".js");
		boolean endIco = s.contains(".ico");
		return start && !endJpg && !endPng && !endGif && !endCss && !endJs && !endIco;
	}
	
	private static Date getDateTimeStr(String src) {
		
		Date date = null;
		try {
			date = parseSdf.parse(src);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
	
	private static String getSession(String ip, Date date) {
		String session;
		String sessionInfo = sessionCache.get(ip);
		long timeStamp = date.getTime();
		if(sessionInfo == null) {
			session = ip + "-session-" + 1;
			sessionCache.put(ip, session + "=" + timeStamp);
		} else {
			String[] infos = sessionInfo.split("=");
			long lastTimeStamp = Long.parseLong(infos[1]);
			session = infos[0];
			if(timeStamp - lastTimeStamp > sessionTimeout) {
				String[] sessions = session.split("-");
				int ts = Integer.parseInt(sessions[2]) + 1;
				session = ip + "-session-" +ts;
			}
			sessionCache.put(ip, session + "=" + timeStamp);
		}
		return session;
	}
	
}
