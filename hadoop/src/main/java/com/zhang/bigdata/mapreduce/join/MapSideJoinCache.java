package com.zhang.bigdata.mapreduce.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * 实现map端join
 *
 * 本地运行
 * 输入文件目录：c:/order.txt
 * 缓存文件本地目录: c:/goods.txt
 * 输出结果目录: c:/output
 */
public class MapSideJoinCache {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(MapSideJoinCache.class);
        job.setMapperClass(JoinMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.addCacheFile(new URI("file:/c:/goods.txt"));

        FileInputFormat.setInputPaths(job, new Path("c:/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("c:/output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

    class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        Map<String, String> goodsMap = new HashMap<>();
        Text keText = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //super.setup(context);
            try( BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("c:/goods.txt")))) {
                String line;
                while(StringUtils.isNotEmpty(line = br.readLine())) {
                    String[] fields = line.split("\t");
                    goodsMap.put(fields[0], fields[1]);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            String line = value.toString();
            String[] fields = line.split("\t");
            String goodsName = goodsMap.get(fields[1]);
            String keyOut = line + "\t" + goodsName;
            keText.set(keyOut);
            context.write(keText, NullWritable.get());
        }
    }
}
