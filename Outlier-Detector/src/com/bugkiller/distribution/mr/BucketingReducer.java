package com.bugkiller.distribution.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.bugkiller.common.util.ComputationUtility;
import com.bugkiller.distribution.NormalizedRecord;

public class BucketingReducer extends Reducer<NormalizedRecord, Text, NullWritable, NormalizedRecord> {

	 private int frequencyThreshold;
	 static List<NormalizedRecord> highFreqBuckets= new ArrayList<NormalizedRecord>();
	 static List<NormalizedRecord> lowFreqBuckets = new ArrayList<NormalizedRecord>();

	 
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
     	frequencyThreshold = conf.getInt("frequencyThreshold",3);
     	highFreqBuckets = new ArrayList<NormalizedRecord>();
     	lowFreqBuckets = new ArrayList<NormalizedRecord>();
     }
		
	@Override
	public void reduce(NormalizedRecord key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int frequency = 0;
		for(Text value : values){
			frequency++;
		}
		if(frequency <= frequencyThreshold){
			lowFreqBuckets.add(key.createClone());
		}else{
			highFreqBuckets.add(key.createClone());
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		for(NormalizedRecord lowFreqRecord : lowFreqBuckets){
			if(computeDistanceWithCorpus(lowFreqRecord) <= 0.8){
				context.write(NullWritable.get(), lowFreqRecord);
			}
		}
	}
	
	private double computeDistanceWithCorpus(NormalizedRecord lowFreqRecord){	
		double similarityScore = 0;
		Map<Integer,String> lowFreqPositionValueMap = new HashMap<Integer, String>();
		for(Object fieldObj : lowFreqRecord.getFields()){
			if(fieldObj instanceof String){
				String fieldValue = (String) fieldObj;
				lowFreqPositionValueMap.put(Integer.parseInt(fieldValue.split("~")[0]), fieldValue.split("~")[1]);
			}
		}
		for(NormalizedRecord highFreqRecord : highFreqBuckets){
			Map<Integer,String> highFreqPositionValueMap = new HashMap<Integer,String>();
			List<Object> highFreqRecordFields = highFreqRecord.getFields();
			for(Object fieldObj : highFreqRecordFields){
				if(fieldObj instanceof String){
					String fieldValue = (String) fieldObj;
					highFreqPositionValueMap.put(Integer.parseInt(fieldValue.split("~")[0]), fieldValue.split("~")[1]);
				}
			}
			double score =0;
			for(Integer ordinalPosition : lowFreqPositionValueMap.keySet()){
				double computedScore = 0;
				String src = lowFreqPositionValueMap.get(ordinalPosition);
				String target = highFreqPositionValueMap.get(ordinalPosition);
				if(!src.matches("\\d+") && !target.matches("\\d+")){
					computedScore = computeStringScore(src, target);
				}else{
					computedScore = computeNumericSimilarityScore(src, target);
				}
				if(score != 0){
					if(computedScore < score){
						score = computedScore;
					}
				}else{
					score = computedScore;
				}
			}
			if(similarityScore == 0 || score > similarityScore){
				similarityScore = score;
			}
		}
		return similarityScore;
	}
	
	private double computeNumericSimilarityScore(String src, String target) {
		Integer srcInt = Integer.parseInt(src);
		Integer targetInt = Integer.parseInt(target);
		Integer min = Math.min(srcInt, targetInt);
		return (1-(double)(Math.abs(srcInt-targetInt))/(double)min);
	}

	private double computeStringScore(String first, String second) {
		int maxLength = Math.max(first.length(), second.length());
		if (maxLength == 0)
			return 1.0d;
		return ((double) (maxLength - ComputationUtility.findStringDistance(first, second)))/ (double) maxLength;
	}

}
