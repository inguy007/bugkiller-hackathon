package com.bugkiller.distribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class NormalizedRecord implements WritableComparable<NormalizedRecord>{
		
		public static final byte INT = 2;
		public static final byte STRING = 6;
		private List<Object> fields;
		private String delim = ",";
		
		public NormalizedRecord() {
			fields = new ArrayList<Object>();
		}
		
		public List<Object> getFields(){
			return this.fields;
		}
		
		public NormalizedRecord createClone() {
			NormalizedRecord clone = new NormalizedRecord();
			clone.fields.addAll(fields);
			return clone;
		}

		public NormalizedRecord createClone(NormalizedRecord clone) {
			clone.initialize();
			clone.fields.addAll(fields);
			return clone;
		}
		
		
		public void initialize() {
			fields.clear();
		}
		
		public void add(Object... fieldList) {
			for (Object field : fieldList) {
				fields.add(field);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			initialize();
			int numFields = in.readInt();
			
			for(int i = 0;  i < numFields;  ++i) {
				byte type = in.readByte();
				
				if (type ==  INT ) {
					fields.add(in.readInt());
				}else if (type ==  STRING) {
					fields.add(in.readUTF());
				} else {
					throw new IllegalArgumentException("Failed encoding, unknown element type in stream");
				}
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(fields.size());
			for(Object field : fields) {
				if (field instanceof Integer){
					out.writeByte(INT);	
					out.writeInt((Integer)field);
				} else if (field instanceof String){
					out.writeByte(STRING);	
					out.writeUTF((String)field);
				} else {
					throw new IllegalArgumentException("Failed encoding, unknown element type in tuple");
				}
			}
		}

		@Override
		public int hashCode() {
			return fields.hashCode();
		}
		
		@Override
		public boolean equals(Object obj ) {
			boolean isEqual = false;
			if (null != obj && obj instanceof NormalizedRecord){
				isEqual =  ((NormalizedRecord)obj).fields.equals(fields);
			}
			return isEqual;
		}

		@Override
		public int compareTo(NormalizedRecord that) {
			int compared = 0;
			if (fields.size() == that.fields.size()) {
				for(int i = 0; i <  fields.size() && compared == 0; ++i) {
					Object field = fields.get(i);
					if (field instanceof Byte){
						compared = ((Byte)field).compareTo((Byte)that.fields.get(i));	
					} else if (field instanceof Boolean){
						compared = ((Boolean)field).compareTo((Boolean)that.fields.get(i));	
					} else if (field instanceof Integer){
						compared = ((Integer)field).compareTo((Integer)that.fields.get(i));	
					} else if (field instanceof Long){
						compared = ((Long)field).compareTo((Long)that.fields.get(i));	
					} else if (field instanceof Float){
						compared = ((Float)field).compareTo((Float)that.fields.get(i));	
					} else if (field instanceof Double){
						compared = ((Double)field).compareTo((Double)that.fields.get(i));	
					} else if (field instanceof String){
						compared = ((String)field).compareTo((String)that.fields.get(i));	
					}  else {
						throw new IllegalArgumentException("Failed in compare, unknown element type in tuple  ");
					}
				}
			} else {
				throw new IllegalArgumentException("Can not compare tuples of unequal length this:"  + 
						fields.size() + " that:" +  that.fields.size());
			}
			return compared;
		}

		public void setDelim(String delim) {
			this.delim = delim;
		}

		@Override
		public String toString() {
			StringBuilder stBld = new  StringBuilder();
			for(int i = 0; i <  fields.size() ; ++i) {
				if (i == 0){
					stBld.append(fields.get(i).toString());
				} else {
					stBld.append(delim).append(fields.get(i).toString());
				}
			}		
			return stBld.toString();
		}	
	}