package com.mungoae.query;

import java.util.HashMap;
import java.util.Map;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.mungoae.util.Tuple;

public class DBQueryFilter {
	
	// e.g "user" or "user.points"
	private Map<String, Tuple<FilterOperator, Object>> _filters = null;
	private Map<String, com.google.appengine.api.datastore.Query.SortDirection> _sorts;
	private DBQuery _q = null; 
	private String _field; 
	
	public DBQueryFilter(DBQuery q, String field, Map<String, Tuple<FilterOperator, Object>> filters,
			Map<String, com.google.appengine.api.datastore.Query.SortDirection> sorts) {
		_q = q;
		_filters = filters;
		_field = field;
		_sorts = sorts;
	}	
	
	public DBQuery or(Object value) {
		return _q;
	}
	
	public DBQuery and(Object value) {
		return _q;
	}
	
	public DBQuery equalTo(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.EQUAL, value)); 
		return _q;
	}
	public DBQuery notEqualTo(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.NOT_EQUAL, value)); 
		return _q;
	}
	public DBQuery greaterThan(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.GREATER_THAN, value)); 
		return _q;
	}
	public DBQuery lessThan(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.LESS_THAN, value)); 
		return _q;
	}
	public DBQuery greaterThanOrEqualTo(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.GREATER_THAN_OR_EQUAL, value)); 
		return _q;
	}
	public DBQuery lessThanOrEqualTo(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.LESS_THAN_OR_EQUAL, value)); 
		return _q;
	}
	
	public DBQueryFilter filter(String innerField){
		return this;
	}
	
}
