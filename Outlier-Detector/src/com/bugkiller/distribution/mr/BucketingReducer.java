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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.bugkiller.common.util.ComputationUtility;
import com.bugkiller.distribution.NormalizedRecord;

public class BucketingReducer extends Reducer<NormalizedRecord, Text, NullWritable, NormalizedRecord> {

	 private int frequencyThreshold;
	 static List<NormalizedRecord> corpus= new ArrayList<NormalizedRecord>();
	 static List<NormalizedRecord> lowFreqBuckets = new ArrayList<NormalizedRecord>();

	 
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
     	frequencyThreshold = conf.getInt("frequencyThreshold",3);
     	corpus = new ArrayList<NormalizedRecord>();
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
			corpus.add(key.createClone());
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
		for(NormalizedRecord highFreqRecord : corpus){
			Map<Integer,String> highFreqPositionValueMap = new HashMap<Integer,String>();
			List<Object> highFreqRecordFields = highFreqRecord.getFields();
			for(Object fieldObj : highFreqRecordFields){
				if(fieldObj instanceof String){
					String fieldValue = (String) fieldObj;
					highFreqPositionValueMap.put(Integer.parseInt(fieldValue.split("~")[0]), fieldValue.split("~")[1]);
				}
			}
			//double stringSimilarityScore = 0;
			//double numericSimilarityScore = 0;
			double score =0;
			double n = 0;
			for(Integer ordinalPosition : lowFreqPositionValueMap.keySet()){
				String src = lowFreqPositionValueMap.get(ordinalPosition);
				String target = highFreqPositionValueMap.get(ordinalPosition);
				if(!src.matches("\\d+") && !target.matches("\\d+")){
					System.out.println("Score between :"+src+" and target :"+target+" is "+computeStringScore(src,target));
					score += computeStringScore(src, target);	
				}else{
					System.out.println("Score between :"+src+" and target :"+target+" is "+computeNumericSimilarityScore(src,target));
					score += computeNumericSimilarityScore(src,target);
				}
				n++;
			}
			score = score/n;
			if(similarityScore == 0 || score > similarityScore){
				if(score >= 0.8){
					System.out.println("BEST MATCHES ::"+lowFreqRecord.createClone()+" and "+highFreqRecord.createClone()+" with score :"+score);	
				}
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
		// Can't divide by 0
		if (maxLength == 0)
			return 1.0d;
		return ((double) (maxLength - ComputationUtility.findStringDistance(first, second)))/ (double) maxLength;
	}

}
