package com.bugkillers.mapreduce;

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
		
		private List<String> ids;
		
		public NormalizedRecord() {
			fields = new ArrayList<Object>();
			ids = new ArrayList<String>();
		}
		
		public NormalizedRecord(List<Object> fields) {
			this.fields = fields;
		}
		
		public List<String> getIds() {
			return ids;
		}

		public void setIds(List<String> ids) {
			this.ids = ids;
		}

		public NormalizedRecord createClone() {
			NormalizedRecord clone = new NormalizedRecord();
			clone.fields.addAll(fields);
			clone.ids.addAll(ids);
			return clone;
		}

		public NormalizedRecord createClone(NormalizedRecord clone) {
			clone.initialize();
			clone.fields.addAll(fields);
			clone.ids.addAll(ids);
			return clone;
		}
		
		
		public void initialize() {
			fields.clear();
		}
		
		
		public int getSize() {
			return fields.size();
		}
		
		public void add(Object...  fieldList) {
			for (Object field :  fieldList) {
				fields.add(field);
			}
		}


		public void prepend(Object field) {
			fields.add(0, field);
		}

		public void append(Object field) {
			fields.add( field);
		}

		
		public void add(byte[] types, String[] fields) {
			for (int i = 0; i <  fields.length; ++i) {
				add(types[i],  fields[i]) ;
			}
		}
		
		
		public void add(byte type, String field) {
			Object typedField = null;
			
			if (type ==  INT ) {
				typedField = Integer.parseInt(field);  
			} else if (type ==  STRING) {
				typedField = field;
			
			}  else {
				throw new IllegalArgumentException("Failed adding element to tuple, unknown element type");
			}
			
			if (null != typedField){
				fields.add(typedField);
			}
		}

		
		public void set(int index, Object field) {
			fields.add(index, field);
		}
		
		
		public Object get(int index) {
			return fields.get(index);
		}
		
		
		public String getString(int index) {
			return (String)fields.get(index);
		}

		
		public String getLastAsString() {
			return (String)fields.get(fields.size()-1);
		}


		public int getInt(int index) {
			return (Integer)fields.get(index);
		}

		
		public int getLastAsInt() {
			return (Integer)fields.get(fields.size()-1);
		}

		
		public long getLong(int index) {
			return (Long)fields.get(index);
		}

		public long getLastAsLong() {
			return (Long)fields.get(fields.size()-1);
		}

		public double getDouble(int index) {
			return (Double)fields.get(index);
		}
		
		public double getLastAsDouble() {
			return (Double)fields.get(fields.size()-1);
		}

		
		public boolean isInt(int index) {
			Object obj = fields.get(index);
			return obj instanceof Integer;
		}

		public boolean isString(int index) {
			Object obj = fields.get(index);
			return obj instanceof String;
		}

		/**
		 * return true if the element is boolean
		 * @param index
		 * @return
		 */
		public boolean isDouble(int index) {
			Object obj = fields.get(index);
			return obj instanceof Double;
		}

		
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

		public int hashCode() {
			return fields.hashCode();
		}
		
		public boolean equals(Object obj ) {
			boolean isEqual = false;
			if (null != obj && obj instanceof NormalizedRecord){
				isEqual =  ((NormalizedRecord)obj).fields.equals(fields);
			}
			return isEqual;
		}

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
		
		public int compareToBase(NormalizedRecord other) {
			NormalizedRecord subThis = new NormalizedRecord(fields.subList(0,fields.size()-1));
			NormalizedRecord subThat = new NormalizedRecord(other.fields.subList(0,other.fields.size()-1));
			return subThis.compareTo(subThat);
		}
		
		/**
		 * hash code based on all but the last element
		 * @return
		 */
		public int hashCodeBase() {
			NormalizedRecord subThis = new NormalizedRecord(fields.subList(0,fields.size()-1));
			return subThis.hashCode();
		}
		
		public boolean startsWith(Object obj) {
			return obj.equals(fields.get(0));
		}
		

		public void setDelim(String delim) {
			this.delim = delim;
		}

		public String toString() {
			StringBuilder stBld = new  StringBuilder();
			for(int i = 0; i <  fields.size() ; ++i) {
				if (i == 0){
					stBld.append(fields.get(i).toString());
				} else {
					stBld.append(delim).append(fields.get(i).toString());
				}
			}
			for(String str : ids){
				stBld.append(delim).append(str);
			}
			return stBld.toString();
		}
		
		public String toString(int start) {
			StringBuilder stBld = new  StringBuilder();
			for(int i = start; i <  fields.size() ; ++i) {
				if (i == start){
					stBld.append(fields.get(i).toString());
				} else {
					stBld.append(delim).append(fields.get(i).toString());
				}
			}		
			return stBld.toString();
		}
		
		public List<Object> getFields(){
			return this.fields;
		}


			
	}