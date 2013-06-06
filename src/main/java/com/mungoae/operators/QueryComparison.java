package com.mungoae.operators;

import java.util.HashMap;
import java.util.Map;

public enum QueryComparison {
	/**
	 * Matches arrays that contain all elements specified in the query.
	 */
	ALL("$all"),
	/**
	 * Matches values that are greater than the value specified in the query.
	 */
	GREATER_THAN("$gt"),
	/**
	 * Matches values that are equal to or greater than the value specified in the query.
	 */
	GREATER_THAN_OR_EQUAL("$gte"),
	/**
	 * Matches any of the values that exist in an array specified in the query.
	 */
	IN("$in"),
	/**
	 * Matches vales that are less than the value specified in the query.
	 */
	LESS_THAN("$lt"),
	/**
	 * Matches values that are less than or equal to the value specified in the query.
	 */
	LESS_THAN_OR_EQUAL("$lte"),
	/**
	 * Matches all values that are not equal to the value specified in the query.
	 */
	NOT_EQUAL("$ne"),
	/**
	 * Matches values that do not exist in an array specified to the query.
	 */
	NOT_IN("$nin");
	private final String command;
	private static final Map<String, QueryComparison> lookup
	 	= new HashMap<String,QueryComparison>();
	static {
		for (QueryComparison c : QueryComparison.values()){
			lookup.put(c.command, c); 
		}
	}
	private QueryComparison(String command){
		this.command = command;
	}
	private String getCommand(){
		return this.command;
	}
	public static QueryComparison get(String command){
		return lookup.get(command);
	}	
}
