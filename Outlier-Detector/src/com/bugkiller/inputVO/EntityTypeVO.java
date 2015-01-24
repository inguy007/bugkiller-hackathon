package com.bugkiller.inputVO;

import java.util.List;

public class EntityTypeVO {

	private String entityName;
	private List<EntityTypeField> entityTypeFields;

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public List<EntityTypeField> getEntityTypeFields() {
		return entityTypeFields;
	}

	public void setEntityTypeFields(List<EntityTypeField> entityTypeFields) {
		this.entityTypeFields = entityTypeFields;
	}

}
