package com.bugkiller.distribution.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.bugkiller.distribution.NormalizedRecord;

public class BucketingReducer extends Reducer<NormalizedRecord, Text, IntWritable, NormalizedRecord> {

	 private int frequencyThreshold;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
     	frequencyThreshold = conf.getInt("frequencyThreshold",3);
     } 
	
	public void reduce(NormalizedRecord key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int frequency = 0;
		for(Text value : values){
			frequency++;
		}
		if(frequency <= frequencyThreshold){
			context.write(new IntWritable(frequency), key);
		}
	}

}
