package com.bugkillers.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;

import com.bugkillers.VO.EntityTypeField;
import com.bugkillers.VO.EntityTypeVO;
import com.bugkillers.common.utils.FieldDataTypes;



public class BucketingMapper extends
		Mapper<LongWritable, Text, NormalizedRecord, Text> {

	private Text outVal = new Text();
	private NormalizedRecord outKey = new NormalizedRecord();
	private String fieldDelimRegex;
	private EntityTypeVO entityTypeVO;
	private int numFields;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimRegex = conf.get("field.record.delim", ",");
		String metaFilePath = conf.get("metadata.file.path");
		FileSystem dfs = FileSystem.get(conf);
		Path src = new Path(metaFilePath);
		FSDataInputStream fs = dfs.open(src);
		ObjectMapper mapper = new ObjectMapper();
		entityTypeVO = mapper.readValue(fs, EntityTypeVO.class);
		numFields = entityTypeVO.getEntityTypeFields().size();
	}
	
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		String[] records = valueStr.split(fieldDelimRegex);
		if (numFields != records.length) {
			context.getCounter("Data", "Invalid").increment(1);
			return;
		}
		outKey.initialize();
		List<EntityTypeField> fields = entityTypeVO.getEntityTypeFields();
		for (EntityTypeField field : fields) {
			int fieldIndex = field.getPosition()-1;
			String attributeValue = records[fieldIndex];
			String fieldDataType = field.getDatatype();
			if (field.isId()) {
				outVal.set(attributeValue);
			} else if (field.isCategorical()) {
				if (fieldDataType.equals(FieldDataTypes.INTEGER)) {
					int bucketWidth = field.getBucketWidth() != 0 ? field.getBucketWidth() : 100;
					outKey.add(fieldIndex+"~"+(Integer.parseInt(attributeValue) / bucketWidth));
				} else if (fieldDataType.equals(FieldDataTypes.STRING)) {
					outKey.add(fieldIndex+"~"+attributeValue.toLowerCase());
				}else if(fieldDataType.equals(FieldDataTypes.DOUBLE)) {
					int bucketWidth = field.getBucketWidth() != 0 ? field.getBucketWidth() : 100;
					outKey.add(fieldIndex+"~"+((int)Double.parseDouble(attributeValue) / bucketWidth));
				} 
			}
		}
		System.out.println("OUT PUT FROM MAPPER :"+outKey);
		context.getCounter("Data", "Processed record").increment(1);
		context.write(outKey, outVal);
	}
}
