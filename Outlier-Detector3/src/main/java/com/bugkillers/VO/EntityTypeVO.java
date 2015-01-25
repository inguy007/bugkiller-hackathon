package com.bugkillers.VO;

import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityTypeVO {

	private String entityName;
	private int minimimThresholdFrequency;
	private List<EntityTypeField> entityTypeFields;

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public int getMinimimThresholdFrequency() {
		return minimimThresholdFrequency;
	}

	public void setMinimimThresholdFrequency(int minimimThresholdFrequency) {
		this.minimimThresholdFrequency = minimimThresholdFrequency;
	}

	public List<EntityTypeField> getEntityTypeFields() {
		return entityTypeFields;
	}

	public void setEntityTypeFields(List<EntityTypeField> entityTypeFields) {
		this.entityTypeFields = entityTypeFields;
	}

}
