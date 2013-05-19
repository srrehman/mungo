package com.mungods.query;

public interface Operators {
	
	/**
	 * Query Selectors
	 * 
	 * Comparison
	 */
	
	/**
	 * Matches arrays that contain all elements specified in the query.
	 */
	public static String ALL = "$all";
	/**
	 * Matches values that are greater than the value specified in the query.
	 */
	public static String GREATER_THAN = "$gt";
	/**
	 * Matches values that are equal to or greater than the value specified in the query.
	 */
	public static String GREATER_THAN_OR_EQUAL = "$gte";
	/**
	 * Matches any of the values that exist in an array specified in the query.
	 */
	public static String IN = "$in";
	/**
	 * Matches vales that are less than the value specified in the query.
	 */
	public static String LESS_THAN = "$lt";
	/**
	 * Matches values that are less than or equal to the value specified in the query.
	 */
	public static String LESS_THAN_OR_EQUAL = "$lte";
	/**
	 * Matches all values that are not equal to the value specified in the query.
	 */
	public static String NOT_EQUAL = "$ne";
	/**
	 * Matches values that do not exist in an array specified to the query.
	 */
	public static String NOT_IN = "$nin";
	
	/**
	 * Logical
	 */

	public static String AND = "$and";
	
	public static String NOR = "$nor";
	
	public static String NOT = "$not";
	
	public static String OR = "$or";
	
	
	
	
}