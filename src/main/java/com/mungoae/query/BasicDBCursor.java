package com.mungoae.query;

import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.mungoae.BasicDBObject;
import com.mungoae.DBCursor;
import com.mungoae.DBObject;
import com.mungoae.DBCollection;
import com.mungoae.object.Mapper;
import com.mungoae.object.ObjectStore;
import com.mungoae.util.JSON;

/**
 * Simple implementation of the <code>Query</code>
 * interface
 * 
 * @author kerby
 *
 */
public class BasicDBCursor extends DBCursor {
	
	private static Logger LOG = LogManager.getLogger(BasicDBCursor.class.getName());

	public BasicDBCursor(DBCollection collection){
		super(collection);
		LOG.info("Inialize DBCollection: " + collection.getName());
	}
	
	/**
	 * Build DBQuery from query string
	 * 
	 * @param collection
	 * @param query
	 */
	public BasicDBCursor(DBCollection collection, String query){
		super(collection);
		LOG.info("Inialize DBCollection: " + collection.getName());
		BasicDBObject dbquery = new BasicDBObject(query);
		_filters = Mapper.createFilterOperatorObjectFrom(dbquery);
	}
	
	/**
	 * Execute the query, and get the result
	 * 
	 * @return
	 */
	@Override
	public DBCursor now() {
		_it = ObjectStore.get(_collection.getDatabaseName(), 
				_collection.getName()).queryObjects(_filters, _sorts, _numToSkip, _max, null, null); 
		return this;
	}
	
	/**
	 * Creates a filter for this query
	 * 
	 * @param field
	 * @return
	 */
	@Override
	public DBQueryFilter filter(String field) {
		_field = field;
		return new DBQueryFilter(this, field, _filters, _sorts); 
	}
	
	/**
	 * Sort the result
	 * 
	 * @param direction
	 * @return
	 */
	@Override
	public BasicDBCursor sort(DBCursor.SortDirection direction){
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
	
	public BasicDBCursor sort(String field, DBCursor.SortDirection direction){
		if (direction == DBCursor.SortDirection.ASCENDING){
			_sorts.put(field, com.google.appengine.api.datastore.Query.SortDirection.ASCENDING);
		} else if (direction == DBCursor.SortDirection.DESCENDING) {
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
	public BasicDBCursor limit(int max){
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
	public BasicDBCursor skip(int numToSkip){
		if (_max == null){
			throw new IllegalArgumentException("Must limit the result before skip"); 
		}
		_numToSkip = numToSkip;
		return this;
	}

	@Override
	public BasicDBCursor or(Object value) {
		_orFilters.put(_field, null);
		return this;
	}

	@Override
	public BasicDBCursor and(Object value) {
		_andFilters.put(_field, null);
		return this;
	}

	@Override
	public <T> Iterable<T> as(Class<T> clazz) {
		_internalClass = clazz;
		final Iterator<DBObject> it = ObjectStore.get(_collection.getDatabaseName(), 
				_collection.getName()).queryObjects(_filters, _sorts, null, null, null, null); 
		
		final Iterator<T> copy = new Iterator<T>() {
			@Override
			public boolean hasNext() {
				return it.hasNext();
			}
			@Override
			public T next() {
				DBObject source = it.next();
				return (T) Mapper.createTObject(_internalClass, source.toMap());
			}
			@Override
			public void remove() {
				it.remove();				
			}
		};
		
		Iterable<T> result = new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {
				return copy;
			}
		};
		return result;
	}

	// If the iterator is null, create 
	// a new iterator to all documents
	@Override
	public void _check() {
		if (_it == null){ 
			_it = _collection.__find(null, null, 0, 0, 0, 0);
		}
	}

	@Override
	public DBCursor sortAscending(String field) {
		sort(field, SortDirection.ASCENDING);
		return this;
	}

	@Override
	public DBCursor sortDescending(String field) {
		sort(field, SortDirection.DESCENDING);
		return this;
	}

	@Override
	public DBCursor sort(DBObject sort) {
		//sort(JSON.serialize(sort));
		return null;
	}
	
}
