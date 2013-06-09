package com.mungoae.query;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.mungoae.DBCollection;
import com.mungoae.DBObject;
import com.mungoae.Mungo;
import com.mungoae.object.ObjectStore;
import com.mungoae.util.BoundedIterator;
import com.mungoae.util.Tuple;

public class QueryImpl implements Query {
	
	private DBCollection _collection;
	private Map<String, Tuple<FilterOperator, Object>> _filters;
	private Map<String, Tuple<FilterOperator, Object>> _orFilters;
	private Map<String, Tuple<FilterOperator, Object>> _andFilters;
	
	private Map<String, com.google.appengine.api.datastore.Query.SortDirection> _sorts; 
	private String _field = null;
	private Integer _max = null;
	private Integer _numToSkip = null;
	
	private Iterator<DBObject> _it = null;
	
	public QueryImpl(DBCollection collection){
		_collection = collection;
		_filters = new LinkedHashMap<String, Tuple<FilterOperator, Object>>();
		_sorts = new LinkedHashMap<String, com.google.appengine.api.datastore.Query.SortDirection>();
	}
	
	public class QueryValue {
		public Tuple<Object,Object> value; 
		public Operator operator;
	}

	
	public enum Operator {
		AND, OR, NOT, NOR
	}
	
	/**
	 * Execute the query
	 * 
	 * @return
	 */
	@Override
	public Iterator<DBObject> now() {
		_it = ObjectStore.get(_collection.getDB().getName(), _collection.getName()).queryObjects(_filters, _sorts); 
		if (_max != null){
			if (_numToSkip != null){
				return new BoundedIterator<DBObject>(_numToSkip, _max, _it);
			} else {
				return new BoundedIterator<DBObject>(0, _max, _it);
			}
		} else {
			return _it;
		}
	}
	
	/**
	 * Creates a filter for this query
	 * 
	 * @param field
	 * @return
	 */
	@Override
	public QueryFilter filter(String field) {
		_field = field;
		return new QueryFilter(this, field, _filters, _sorts); 
	}
	
	/**
	 * Sort the result
	 * 
	 * @param direction
	 * @return
	 */
	@Override
	public QueryImpl sort(QueryImpl.SortDirection direction){
		if (_filters.get(_field) == null){
			throw new IllegalArgumentException("Cannot sort if there is no filter for the given field");
		}
		if (direction == QueryImpl.SortDirection.ASCENDING){
			_sorts.put(_field, com.google.appengine.api.datastore.Query.SortDirection.ASCENDING);
		} else if (direction == QueryImpl.SortDirection.DESCENDING) {
			_sorts.put(_field, com.google.appengine.api.datastore.Query.SortDirection.DESCENDING);
		}
		return this;
	}
	
	public QueryImpl sort(String field, QueryImpl.SortDirection direction){
		if (direction == QueryImpl.SortDirection.ASCENDING){
			_sorts.put(field, com.google.appengine.api.datastore.Query.SortDirection.ASCENDING);
		} else if (direction == QueryImpl.SortDirection.DESCENDING) {
			_sorts.put(field, com.google.appengine.api.datastore.Query.SortDirection.DESCENDING);
		}
		return this;		
	}
	
	/**
	 * Limit the result
	 * 
	 * @param max
	 * @return
	 */
	@Override
	public QueryImpl limit(int max){
		_max = max;
		return this;
	}
	
	/**
	 * Skip
	 * 
	 * @param numToSkip
	 * @return
	 */
	@Override
	public QueryImpl skip(int numToSkip){
		if (_max == null){
			throw new IllegalArgumentException("Must limit the result before skip"); 
		}
		_numToSkip = numToSkip;
		return this;
	}
	
	public static void main() {
		Mungo mungo = new Mungo();
		DBCollection coll = mungo.getDB("TestDB").getCollection("TestCollection");
		Iterator<DBObject> curr = new QueryImpl(coll).filter("field1").equalTo(123).filter("field2").greaterThan(456).now();
		
		curr = new QueryImpl(coll).filter("outerfield1")
				.filter("innerField1").equalTo(123).now();
	}

	@Override
	public QueryImpl or(Object value) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
