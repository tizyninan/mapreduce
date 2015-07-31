package com.tizyninan.reducerfilesmerge;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    IntWritable one = new IntWritable(1);
    Text outputKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while(tokenizer.hasMoreTokens()) {
            outputKey.set(tokenizer.nextToken());
            context.write(new Text(outputKey),one);
        }
    }
}
