package com.tizyninan.reducerfilesmerge;

import com.tizyninan.reducerfilesmerge.constants.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;


public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    MultipleOutputs mos;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values) {
            sum += value.get();
        }
        mos.write(Constants.REDUCER_FILE_NAME,key,sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
