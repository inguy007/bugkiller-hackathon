package com.bugkillers.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bugkillers.common.utils.ConfigurationUtils;



public class BucketingDriver {

/*	static Path inputPath = new Path("/test/mapreduce/hackathon/input2.csv");
	static Path outputPath = new Path("/test/mapreduce/hackathon/output");
	static String metaFilePath = "/test/mapreduce/hackathon/meta2.json";*/
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
	Path inputPath = new Path(args[0]);
	Path outputPath = new Path(args[1]);
	String metaFilePath = args[2];
	
	Configuration conf = ConfigurationUtils.getConfiguration();
	conf.setInt("frequencyThreshold",2);
	conf.set("field.record.delim", ",");
    conf.set("metadata.file.path", metaFilePath);
	Job job = new Job(conf, "outlier-bucketing");
	job.setJarByClass(BucketingDriver.class);
	job.setMapperClass(BucketingMapper.class);
	job.setReducerClass(BucketingReducer.class);
	job.setMapOutputKeyClass(NormalizedRecord.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);
	job.setNumReduceTasks(1);
	
	FileSystem fs = FileSystem.get(conf);
	if(fs.exists(outputPath)){
		fs.delete(outputPath, true);
	}
	FileInputFormat.setInputPaths(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputPath);
	/*MultipleOutputs.addNamedOutput(job,
            "Error_file", TextOutputFormat.class,
            Text.class, NullWritable.class);*/
	System.out.println("Starting Job");
	job.waitForCompletion(true);
	System.out.println("Job Completed");
	}
	
	
}
