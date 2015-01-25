package com.bugkillers.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.bugkillers.common.utils.ComputationUtility;

public class BucketingReducer extends Reducer<NormalizedRecord, Text, NullWritable, Text> {

	 private int frequencyThreshold;
	 static List<NormalizedRecord> highFreqBuckets= new ArrayList<NormalizedRecord>();
	 static List<NormalizedRecord> lowFreqBuckets = new ArrayList<NormalizedRecord>();

	 
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
     	frequencyThreshold = conf.getInt("frequencyThreshold",2);
     	highFreqBuckets = new ArrayList<NormalizedRecord>();
     	lowFreqBuckets = new ArrayList<NormalizedRecord>();
     }
		
	@Override
	public void reduce(NormalizedRecord key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int frequency = 0;
		List<String> idList = new ArrayList<String>();
		for(Text value : values){
			frequency++;
			idList.add(value.toString());
		}
		if(frequency <= frequencyThreshold){
			key.setIds(idList);
			lowFreqBuckets.add(key.createClone());
		}else{
			highFreqBuckets.add(key.createClone());
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		for(NormalizedRecord lowFreqRecord : lowFreqBuckets){
			if(computeDistanceWithCorpus(lowFreqRecord) < 0.8){
				List<String> ids = lowFreqRecord.getIds();	
				for(String value : ids){
					context.write(NullWritable.get(), new Text(value+"::"+lowFreqRecord));
				}
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
			int n = 0;
			for(Integer ordinalPosition : lowFreqPositionValueMap.keySet()){
				double computedScore = 0;
				String src = lowFreqPositionValueMap.get(ordinalPosition);
				String target = highFreqPositionValueMap.get(ordinalPosition);
				if(!src.matches("\\d+") && !target.matches("\\d+")){
					computedScore = computeStringScore(src, target);
				}else{
					computedScore = computeNumericSimilarityScore(src, target);
				}
				if(n != 0){
					if(computedScore < score){
						score = computedScore;
					}
				}else{
					score = computedScore;
				}
				n++;
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
