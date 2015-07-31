package com.tizyninan.reducerfilesmerge;

import com.tizyninan.reducerfilesmerge.constants.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class WordcountDriver extends Configured implements Tool {

    private static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        String source = args[1];
        String destination = args[3];
        int result = ToolRunner.run(conf, new WordcountDriver(), args);
        if(result == 0)
            new WordcountDriver().mergeFilesInHDFS(source, destination);
        System.exit(result);
    }

    private void mergeFilesInHDFS(String source, String destination) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path sourcePath = new Path(source);
        Path destPath = new Path(destination);
        FileUtil.copyMerge(fs, sourcePath, fs, destPath, false, conf, null);
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path inputDir = new Path(strings[0]);
        Path outputDir = new Path(strings[1]);

        Job job = Job.getInstance(conf, "SingleFileWrite");

        job.setJarByClass(WordcountDriver.class);
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        MultipleOutputs.addNamedOutput(job, Constants.REDUCER_FILE_NAME, TextOutputFormat.class, Text.class, IntWritable.class);

        return (job.waitForCompletion(true)?0:1);
    }
}
