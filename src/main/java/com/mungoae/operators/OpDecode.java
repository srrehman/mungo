package com.mungoae.operators;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.mungoae.object.ObjectStore;
import com.mungoae.query.UpdateQuery.UpdateOperator;
/**
 * Operator decoder. Decodes a given string operator (e.g. "$gte")
 * and returns the corresponding Datastore <code>FilterOperator</code>
 * 
 * @author Kerby Martino <kerbymart@gmail.com>
 *
 */
public class OpDecode {
	private static Logger LOG = LogManager.getLogger(OpDecode.class.getName());

	public static FilterOperator parseFilterFrom(String filter) {
		LOG.debug("Get operator [" + filter + "]");
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
	
	public static UpdateOperator parseUpdateFilterFrom(String filter){
		LOG.debug("Get update operator [" + filter + "]");
		if (filter.equals("$inc")){
			return UpdateOperator.INCREMENT;
		} else if (filter.equals("$dec")) {
			return UpdateOperator.DECREMENT;
		} else if (filter.equals("$set")) {
			return UpdateOperator.SET;
		} else if (filter.equals("$unset")) {
			return UpdateOperator.UNSET;
		} else if (filter.equals("$setOnInsert")) {
			return UpdateOperator.SET_ON_INSERT;
		} else if (filter.equals("$rename")) {
			return UpdateOperator.RENAME;
		} else {
			throw new IllegalArgumentException("Invalid operator: " + filter);
		}		
	}
}
