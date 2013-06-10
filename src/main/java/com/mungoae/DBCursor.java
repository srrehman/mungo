package com.mungoae;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.mungoae.query.DBQueryFilter;
import com.mungoae.util.Tuple;

public abstract class DBCursor implements Iterable<DBObject>, 
	Iterator<DBObject> {
	
	protected DBCollection _collection;
	protected Map<String, Tuple<FilterOperator, Object>> _filters;
	protected Map<String, Tuple<FilterOperator, Object>> _orFilters;
	protected Map<String, Tuple<FilterOperator, Object>> _andFilters;
	
	protected Map<String, com.google.appengine.api.datastore.Query.SortDirection> _sorts; 
	protected String _field = null;
	protected Integer _max = null;
	protected Integer _numToSkip = null;
	
	protected Iterator<DBObject> _it = null;
	
	protected Class _internalClass;
	
	public enum SortDirection { 
		ASCENDING(1), DESCENDING(-1);
		private final int dir;
		SortDirection(int dir){
			this.dir = dir;
		}
		public int getValue() {
			return dir;
		}
	}
	
	public abstract DBCursor or(Object value);
	public abstract DBCursor and(Object value);
	public abstract DBQueryFilter filter(String field);
	public abstract DBCursor sort(DBCursor.SortDirection direction);
	public abstract DBCursor sort(DBObject sort);
	public abstract DBCursor sortAscending(String field);
	public abstract DBCursor sortDescending(String field);
	public abstract DBCursor sort(String field, DBCursor.SortDirection direction);
	public abstract DBCursor limit(int max);
	public abstract DBCursor skip(int numToSkip);
	public abstract DBCursor now();
	
	public DBCursor(DBCollection collection){
		_collection = collection;
		_filters = new LinkedHashMap<String, Tuple<FilterOperator, Object>>();
		_sorts = new LinkedHashMap<String, com.google.appengine.api.datastore.Query.SortDirection>();
	}
	
	@Override
	public Iterator<DBObject> iterator() {
		_check();
		return _it;
	}

	@Override
	public boolean hasNext() {
		_check();
		return _it.hasNext();
	}

	@Override
	public DBObject next() {
		_check();
		return _it.next();
	}

	@Override
	public void remove() {
		_check();
		_it.remove();		
	}	
	
	public abstract <T> Iterable<T> as(Class<T> clazz);
	
	public abstract void _check();


}
