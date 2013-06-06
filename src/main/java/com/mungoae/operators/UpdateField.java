package com.mungoae.operators;

import java.util.HashMap;
import java.util.Map;

public enum UpdateField {
	// Increments the value of the field by the specified amount.
	INCREMENT("$inc"),  
	// Renames a field
	RENAME("$rename"),
	// Sets the value of a field upon documentation creation during an upsert. 
	// Has no effect on update operations that modify existing documents.
	SETONINSERT("$setOnInsert"),
	SET("$set"),
	UNSET("$unset");
	private final String command;
	private static final Map<String, UpdateField> lookup
	 	= new HashMap<String,UpdateField>();
	static {
		for (UpdateField c : UpdateField.values()){
			lookup.put(c.command, c); 
		}
	}
	private UpdateField(String command){
		this.command = command;
	}
	private String getCommand(){
		return this.command;
	}
	public static UpdateField get(String command){
		return lookup.get(command);
	}
}
