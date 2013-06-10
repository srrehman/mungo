package com.mungoae.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.mungoae.BasicDBObject;
import com.mungoae.MungoCollection;
import com.mungoae.object.Mapper;
import com.mungoae.object.ObjectStore;
import com.mungoae.util.Tuple;

public class UpdateQuery {

	public enum UpdateOperator {
		INCREMENT, DECREMENT,
		SET, UNSET,
		RENAME, SET_ON_INSERT
	}
	
	private boolean _multi = false;
	private boolean _upsert = false;
	
	private Integer _by = 0;
	private String _field = null;
	
	private MungoCollection _collection = null;
	
	private Map<String, Tuple<UpdateOperator, Object>> _updates = new HashMap<String,Tuple<UpdateOperator, Object>>();
	private Map<String, Tuple<FilterOperator, Object>> _filters = null; // set through query string or wired methods
	
	public UpdateQuery(MungoCollection collection, Map<String, Tuple<FilterOperator, Object>> filters){
		_collection = collection;
		_filters = filters;
	}
	
	/**
	 * Append query
	 * 
	 * Query string like:
	 * 
	 * { '$set' : {'number': 123}}"
	 * 
	 * Where valid operators are
	 * <ul>
	 * 	<li>$set<li>
	 *  <li>$unset<li>
	 * </ul>
	 * 
	 * @param query
	 * @return
	 */
	public UpdateQuery with(String query){ 
		_updates = Mapper.createUpdateOperatorFrom(new BasicDBObject(query));
		return this;
	}
	
	public void now() {
		if (_filters == null || _updates == null)
			return;
		ObjectStore.get(_collection.getDatabaseName(), _collection.getName()).updateObjects(_filters, _updates, true, true); 
	}
	
	public UpdateQuery increment(String field, Object byValue) {
		_field = field;
		_updates.put(_field, new Tuple<UpdateQuery.UpdateOperator, Object>(UpdateOperator.INCREMENT, byValue));
		return this;
	}
	
	public UpdateQuery decrement(String field, Object byValue) {
		_field = field;
		_updates.put(_field, new Tuple<UpdateQuery.UpdateOperator, Object>(UpdateOperator.DECREMENT, byValue));
		return this;
	}
	
	public void by(Integer by){
		_by = by;

	}

	
	public UpdateQuery upsert() {
		_upsert = true;
		return this;
	}
	public UpdateQuery multi() {
		_multi = true;
		return this;
	}
	
	public boolean isMulti() {
		return _multi;
	}
	
	public boolean isUpsert() {
		return _upsert;
	}
}
