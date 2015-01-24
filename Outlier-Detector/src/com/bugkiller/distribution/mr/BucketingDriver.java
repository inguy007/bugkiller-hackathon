package com.bugkiller.distribution.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bugkiller.common.util.ConfigurationUtils;
import com.bugkiller.distribution.NormalizedRecord;

public class BucketingDriver {

	static Path inputPath = new Path("/test/mapreduce/hackathon/input2.csv");
	static Path outputPath = new Path("/test/mapreduce/hackathon/output");
	static String metaFilePath = "/test/mapreduce/hackathon/meta2.json";
	
	public static void main(String[] args) throws Exception {
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
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NormalizedRecord.class);
		job.setNumReduceTasks(1);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.out.println("Starting Job");
		job.waitForCompletion(true);
		System.out.println("Job Completed");	
	}

}
