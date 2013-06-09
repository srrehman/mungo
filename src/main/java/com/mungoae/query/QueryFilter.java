package com.mungoae.query;

import java.util.HashMap;
import java.util.Map;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.mungoae.util.Tuple;

public class QueryFilter {
	
	// e.g "user" or "user.points"
	private Map<String, Tuple<FilterOperator, Object>> _filters = null;
	private Map<String, com.google.appengine.api.datastore.Query.SortDirection> _sorts;
	private QueryImpl _q = null; 
	private String _field; 
	
	public QueryFilter(QueryImpl q, String field, Map<String, Tuple<FilterOperator, Object>> filters,
			Map<String, com.google.appengine.api.datastore.Query.SortDirection> sorts) {
		_q = q;
		_filters = filters;
		_field = field;
		_sorts = sorts;
	}	
	
	public QueryImpl or(Object value) {
		return _q;
	}
	
	public QueryImpl and(Object value) {
		return _q;
	}
	
	public QueryImpl equalTo(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.EQUAL, value)); 
		return _q;
	}
	public QueryImpl notEqualTo(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.NOT_EQUAL, value)); 
		return _q;
	}
	public QueryImpl greaterThan(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.GREATER_THAN, value)); 
		return _q;
	}
	public QueryImpl lessThan(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.LESS_THAN, value)); 
		return _q;
	}
	public QueryImpl greaterThanOrEqualTo(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.GREATER_THAN_OR_EQUAL, value)); 
		return _q;
	}
	public QueryImpl lessThanOrEqualTo(Object value){
		if (_filters == null)
			_filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		_filters.put(_field, new Tuple<FilterOperator, Object>(FilterOperator.LESS_THAN_OR_EQUAL, value)); 
		return _q;
	}
	
	public QueryFilter filter(String innerField){
		return this;
	}
	
}
