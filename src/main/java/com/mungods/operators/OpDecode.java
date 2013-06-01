package com.mungods.operators;

import java.util.logging.Logger;

import com.google.appengine.api.datastore.Query.FilterOperator;
/**
 * Operator decoder. Decodes a given string operator (e.g. "$gte")
 * and returns the corresponding Datastore <code>FilterOperator</code>
 * 
 * @author Kerby Martino <kerbymart@gmail.com>
 *
 */
public class OpDecode {
	private static final Logger logger 
		= Logger.getLogger(OpDecode.class.getName());
	public static FilterOperator get(String filter) {
		logger.info("Get operator [" + filter + "]");
//		if (filter.equals(Operator.GREATER_THAN_OR_EQUAL.toString())){
//			return FilterOperator.GREATER_THAN_OR_EQUAL;
//		}
		if (filter.equals("$gte")){
			return FilterOperator.GREATER_THAN_OR_EQUAL;
		} else if (filter.equals("$gt")) {
			return FilterOperator.GREATER_THAN;
		} else if (filter.equals("$lte")) {
			return FilterOperator.LESS_THAN_OR_EQUAL;
		} else if (filter.equals("$lt")) {
			return FilterOperator.LESS_THAN;
		} else if (filter.equals("$e")) {
			return FilterOperator.EQUAL;
		} else if (filter.equals("$e")) {
			return FilterOperator.EQUAL;
		} else if (filter.equals("$ne")) {
			return FilterOperator.NOT_EQUAL;
		}else {
			throw new IllegalArgumentException("Invalid operator: " + filter);
		}
	}
}
