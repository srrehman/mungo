package com.mungoae.query;

import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.mungoae.DBObject;
import com.mungoae.MungoCollection;
import com.mungoae.collection.simple.DBApiLayer;
import com.mungoae.object.ObjectStore;
import com.mungoae.util.BoundedIterator;

/**
 * Simple implementation of the <code>Query</code>
 * interface
 * 
 * @author kerby
 *
 */
public class BasicDBQuery extends DBQuery {
	
	private static Logger LOG = LogManager.getLogger(BasicDBQuery.class.getName());

	
	public BasicDBQuery(MungoCollection collection){
		super(collection);
		LOG.info("Inialize DBCollection: " + collection.getName());
	}
	
	/**
	 * Execute the query, and get the result
	 * 
	 * @return
	 */
	@Override
	public DBQuery now() {
		Iterator<DBObject> it = ObjectStore.get(_collection.getDatabaseName(), _collection.getName()).queryObjects(_filters, _sorts); 
		if (_max != null){
			if (_numToSkip != null){
				_it = new BoundedIterator<DBObject>(_numToSkip, _max, it);
			} else {
				_it = new BoundedIterator<DBObject>(0, _max, it); 
			}
		} else {
			_it = it; 
		}
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
	public BasicDBQuery sort(DBQuery.SortDirection direction){
		if (_filters.get(_field) == null){
			throw new IllegalArgumentException("Cannot sort if there is no filter for the given field");
		}
		if (direction == DBQuery.SortDirection.ASCENDING){
			_sorts.put(_field, com.google.appengine.api.datastore.Query.SortDirection.ASCENDING);
		} else if (direction == DBQuery.SortDirection.DESCENDING) {
			_sorts.put(_field, com.google.appengine.api.datastore.Query.SortDirection.DESCENDING);
		}
		return this;
	}
	
	public BasicDBQuery sort(String field, DBQuery.SortDirection direction){
		if (direction == DBQuery.SortDirection.ASCENDING){
			_sorts.put(field, com.google.appengine.api.datastore.Query.SortDirection.ASCENDING);
		} else if (direction == DBQuery.SortDirection.DESCENDING) {
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
	public BasicDBQuery limit(int max){
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
	public BasicDBQuery skip(int numToSkip){
		if (_max == null){
			throw new IllegalArgumentException("Must limit the result before skip"); 
		}
		_numToSkip = numToSkip;
		return this;
	}

	@Override
	public BasicDBQuery or(Object value) {
		_orFilters.put(_field, null);
		return this;
	}

	@Override
	public BasicDBQuery and(Object value) {
		_andFilters.put(_field, null);
		return this;
	}

	@Override
	public <T> Iterable<T> as(Class<T> clazz) {
		
		return null;
	}

	@Override
	public void _check() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DBQuery sortAscending(String field) {
		sort(field, SortDirection.ASCENDING);
		return this;
	}

	@Override
	public DBQuery sortDescending(String field) {
		sort(field, SortDirection.DESCENDING);
		return this;
	}
	
}
