package com.mungoae;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.common.base.Preconditions;
import com.mungoae.object.Mapper;
import com.mungoae.object.ObjectStore;
import com.mungoae.query.DBFilter;
import com.mungoae.util.Tuple;

public class DBCursor implements Iterable<DBObject>, 
	Iterator<DBObject> {
	
	private static Logger LOG = LogManager.getLogger(DBCursor.class.getName());

	
	protected DBCollection _collection;
	
	// Filter and sorts used by wired query
	protected Map<String, Tuple<FilterOperator, Object>> _filters;
	protected Map<String, Tuple<FilterOperator, Object>> _orFilters;
	protected Map<String, Tuple<FilterOperator, Object>> _andFilters;
	protected Map<String, com.google.appengine.api.datastore.Query.SortDirection> _sorts; 
	
	protected String _field = null;
	protected Integer _max = null;
	protected Integer _numToSkip = null;
	protected Integer _fetchSize = null;
	protected Integer _options = null;
	
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
	
	public DBCursor(DBCollection collection){
		LOG.info("Inialize DBCollection: " + collection.getName());
		_collection = collection;
		_filters = new LinkedHashMap<String, Tuple<FilterOperator, Object>>();
		_sorts = new LinkedHashMap<String, com.google.appengine.api.datastore.Query.SortDirection>();
	}
	
	/**
	 * Build DBQuery from query string
	 * 
	 * @param collection
	 * @param query
	 */
	public DBCursor(DBCollection collection, String query){
		LOG.info("Inialize DBCollection: " + collection.getName());
		BasicDBObject dbquery = new BasicDBObject(query);
		_filters = Mapper.createFilterOperatorObjectFrom(dbquery);
		_collection = collection;
		//_check();
	}
	
	
	public DBCursor(DBCollection coll, DBObject query, DBObject field) { 
		LOG.info("Inialize DBCollection: " + coll.getName());
		if (query == null){
			query = new BasicDBObject();
		} 
		_collection = coll;
		_filters = Mapper.createFilterOperatorObjectFrom(query);
		//_check();
	}

	public Iterator<DBObject> iterator() {
		_check();
		return _it;
	}

	
	public boolean hasNext() {
		_check();
		return _it.hasNext();
	}

	
	public DBObject next() {
		_check();
		return _it.next();
	}

	
	public void remove() {
		_check();
		_it.remove();		
	}	
	
	/**
	 * Execute the query, and get the result
	 * 
	 * @return
	 */
	
	public DBCursor now() {
		_it = ObjectStore.get(_collection.getDB().getName(), 
				_collection.getName()).queryObjects(_filters, _sorts, _numToSkip, _max, null, null); 
		return this;
	}
	
	/**
	 * Creates a filter for this query
	 * 
	 * @param field
	 * @return
	 */
	
	public DBFilter filter(String field) {
		_field = field;
		return new DBFilter(this, field, _filters, _sorts); 
	}
	
	/**
	 * Sort the result
	 * 
	 * @param direction
	 * @return
	 */
	
	public DBCursor sort(DBCursor.SortDirection direction){
		if (_filters.get(_field) == null){
			throw new IllegalArgumentException("Cannot sort if there is no filter for the given field");
		}
		if (direction == DBCursor.SortDirection.ASCENDING){
			_sorts.put(_field, com.google.appengine.api.datastore.Query.SortDirection.ASCENDING);
		} else if (direction == DBCursor.SortDirection.DESCENDING) {
			_sorts.put(_field, com.google.appengine.api.datastore.Query.SortDirection.DESCENDING);
		}
		return this;
	}
	
	public DBCursor sort(String field, DBCursor.SortDirection direction){
		Preconditions.checkNotNull(field, "Cannot sort null document field");
		Preconditions.checkNotNull(field, "Cannot sort null sort direction");
		if (_sorts == null){
			_sorts = new HashMap<String, com.google.appengine.api.datastore.Query.SortDirection>();
		}
		if (direction == DBCursor.SortDirection.ASCENDING){
			_sorts.put(field, com.google.appengine.api.datastore.Query.SortDirection.ASCENDING);
		} else if (direction == DBCursor.SortDirection.DESCENDING) {
			_sorts.put(field, com.google.appengine.api.datastore.Query.SortDirection.DESCENDING);
		}
		return this;		
	}
	
	public DBCursor sortAscending(String field) {
		sort(field, SortDirection.ASCENDING);
		return this;
	}

	
	public DBCursor sortDescending(String field) {
		sort(field, SortDirection.DESCENDING);
		return this;
	}

	
	public DBCursor sort(DBObject sort) {
		String key = sort.keySet().iterator().next();
		Object value = sort.get(key);
		return sort(key, 
				value == Integer.valueOf(1) ? 
						SortDirection.ASCENDING : SortDirection.DESCENDING); 
	}
	
	/**
	 * Limit the result
	 * 
	 * @param max
	 * @return
	 */
	
	public DBCursor limit(int max){
		_max = max;
		return this;
	}
	
	/**
	 * Skip
	 * 
	 * @param numToSkip
	 * @return
	 */
	
	public DBCursor skip(int numToSkip){
		if (_max == null){
			throw new IllegalArgumentException("Must limit the result before skip"); 
		}
		_numToSkip = numToSkip;
		return this;
	}

	
	public DBCursor or(Object value) {
		_orFilters.put(_field, null);
		return this;
	}

	
	public DBCursor and(Object value) {
		_andFilters.put(_field, null);
		return this;
	}

	
	public <T> Iterable<T> as(Class<T> clazz) {
		_internalClass = clazz;
		final Iterator<DBObject> it = ObjectStore.get(_collection.getDB().getName(), 
				_collection.getName()).queryObjects(_filters, _sorts, null, null, null, null); 
		
		final Iterator<T> copy = new Iterator<T>() {
			
			public boolean hasNext() {
				return it.hasNext();
			}
			
			public T next() {
				DBObject source = it.next();
				return (T) Mapper.createTObject(_internalClass, source.toMap());
			}
			
			public void remove() {
				it.remove();				
			}
		};
		
		Iterable<T> result = new Iterable<T>() {
			
			public Iterator<T> iterator() {
				return copy;
			}
		};
		return result;
	}

	// If the iterator is null, create 
	// a new iterator to all documents
	public void _check() {
		if (_it == null){ 
			_it = _collection.__find(_filters, _sorts, _numToSkip, _max, _fetchSize, _options); 
		}
	}

}
