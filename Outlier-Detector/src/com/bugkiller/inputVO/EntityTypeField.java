package com.bugkiller.inputVO;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityTypeField {

	private int position;
	private boolean id;
	private String fieldName;
	private String datatype;
	private boolean categorical;
	private int bucketWidth;

	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public boolean isId() {
		return id;
	}

	public void setId(boolean id) {
		this.id = id;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	public boolean isCategorical() {
		return categorical;
	}

	public void setCategorical(boolean categorical) {
		this.categorical = categorical;
	}

	public int getBucketWidth() {
		return bucketWidth;
	}

	public void setBucketWidth(int bucketWidth) {
		this.bucketWidth = bucketWidth;
	}

}
