package com.mungoae.collection.simple;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.common.collect.Lists;
import com.mungoae.BasicDBObject;
import com.mungoae.DB;
import com.mungoae.DBCursor;
import com.mungoae.DBObject;
import com.mungoae.DBCollection;
import com.mungoae.annotations.Entity;
import com.mungoae.annotations.Id;
import com.mungoae.collection.WriteConcern;
import com.mungoae.collection.WriteResult;
import com.mungoae.object.Mapper;
import com.mungoae.object.ObjectStore;
import com.mungoae.query.Update;
import com.mungoae.util.BoundedIterator;
import com.mungoae.util.JSON;
import com.mungoae.util.Tuple;

public class BasicMungoCollection extends DBCollection {

	private static Logger LOG = LogManager.getLogger(BasicMungoCollection.class.getName());

	private ObjectStore _store = null;
	
	public BasicMungoCollection(DB db, String collection) {
		super(db, collection);
		_store = ObjectStore.get(getDB().getName(), collection);
	}
	
	@Override
	public DBCursor find() {	
		return new DBCursor(this); 
	}
	
	@Override
	public DBCursor find(DBObject query) {
		return find(JSON.serialize(query)); 
	}

	@Override
	public DBCursor find(String query) {
		return new DBCursor(this, query);
	}
	
	@Override
	public DBObject findOne() {
		try {
			Iterator<DBObject> it = find();
			return it.next(); 
		} catch (Exception e) {
			// TODO Handle exception
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public DBObject findOne(String query) {
		try {
			Iterable<DBObject> curr = find(query);
			List<DBObject> asList = Lists.newArrayList(curr);
			if (!asList.isEmpty()){
				return asList.get(0);
			} else {
				return null;
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public DBObject findOne(Object id) {
		// TODO - move direct access to _store
		// into the __insert method
		return _store.queryObjectById(id);
	}
	
	@Override
	public DBObject findOne(ObjectId id) {
		// TODO - move direct access to _store
		// into the __insert method
		return _store.queryObjectById(id);
	}
	
	@Override
	public DBObject findOne(DBObject query) {
		DBCursor curr = find(query);
		if (curr.hasNext()){
			return curr.next();
		}
		return null;
	}

	@Override
	public WriteResult insert(String doc) {
		BasicDBObject o = new BasicDBObject(doc);
		return insert(o);
	}

	@Override
	public <T> WriteResult insert(T doc) {
		return insert(Lists.newArrayList(doc));
	}

	@Override
	public <T> WriteResult insert(T... docs) {
		return insert(Arrays.asList(docs));
	}

	@Override
	public <T> WriteResult insert(List<T> docs) {
		return __insert(docs, true, null);
	}
	
	@SuppressWarnings("unused")
	@Override
	public Update update(String query) {
		Map<String, Tuple<FilterOperator, Object>> filters = null;
		try {
			BasicDBObject queryObject = new BasicDBObject(query);
			Iterator<String> fields = queryObject.keySet().iterator();
			while(fields.hasNext()){
				if (filters == null){
					filters = new HashMap<String, Tuple<FilterOperator, Object>>();
				}
				String field = fields.next();
				Object value = queryObject.get(field);
				//filters = Mapper.createFilterOperatorObjectFrom(filter);
				filters.put(field, new Tuple<FilterOperator, Object>(FilterOperator.EQUAL, value)); 
			}
			return new Update(this, filters);
		} catch (Exception e) {
			// TODO Handle exception
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public Update update(DBObject query) {
		return update(JSON.serialize(query));  
	}

	@Override
	public <T> T save(T doc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public WriteResult remove(Object id) {
		if(_store.deleteObject(id)){
			return new WriteResult(getDB().okResult(), null);
		} else {
			
		}
		return null;
	}

	@Override
	public WriteResult remove(String query) {
		// TODO Auto-generated method stub
		return null;
	}
	

	
//	@Override
//	protected Iterator<DBObject> __find(
//			Map<String, Tuple<FilterOperator, Object>> filters,
//			Map<String, SortDirection> sorts, Integer numToSkip, Integer limit,
//			Integer batchSize, Integer options) {
//		LOG.debug("Applying filters to query="+filters);
//		if (filters.size() == 1 && filters.get("id") != null){
//			return Lists.newArrayList(_store.queryObjectById(filters.get("id").getSecond())).iterator();
//		}
//		return _store.queryObjects(filters, sorts, numToSkip, limit, batchSize, options);
//	}
	
	@Override
	public Iterator<DBObject> __find(final DBObject ref, final DBObject fields,
			Integer numToSkip, Integer limit, Integer batchSize, Integer options) {
		
		// The $query and $orderby keys will be available
		// when the query was built with order by intention
		// otherwise these objects will be null, and the whole 
		// 'ref' will be the filter query
		DBObject query = (DBObject) ref.get("$query"); // filter w/c docs to get
		DBObject orderby = (DBObject) ref.get("$orderby"); // order 
		
		if (query == null){ // the whole ref is the query
			query = ref;
		}
		
		// Remove special fields
		dataCleansing(ref);
		Iterator<DBObject> it = _store.queryObjects(query, orderby, fields, numToSkip, limit, null, null);
		return it;
		
	}

	// Helper method to filter fields to be returned
	// until the low-level can support Datastore projection queries
	private DBObject copyFields(DBObject ref, DBObject fieldsToCopy){
		Iterator<String> it = ref.keySet().iterator();
		while (it.hasNext()){
			String field = it.next();	
			Object fieldToCopy = fieldsToCopy.get(field);
			if (fieldToCopy != null && fieldToCopy == Integer.valueOf(1)){ 
				// retain the field
			} else {
				// ConcurrentModificationException !!!
				//ref.removeField(field);
			}
		}
		return ref;
	}

	
	// TODO: Must implement size check to validate if object exceed maximum datastore entity byte size
	public void putSizeCheck(Object obj) {
		
	}
	
	private void dataCleansing(DBObject obj){
		obj.removeField("$query");
		obj.removeField("$orderby");
	}



	// TODO - Make this run in transaction
	@Override
	protected <T> WriteResult __insert(List<T> list, boolean shouldApply,
			WriteConcern concern) {
		if (_store.persistObjects(convertList(list)) != null){
			return new WriteResult(getDB().okResult(), concern);
		}
		return new WriteResult(getDB().errorResult(), concern);  
	}

	/**
	 * Transform a list
	 * 
	 * @param from
	 * @return
	 */
	private <T> List<DBObject> convertList(List<T> from){
		// Group
		List<DBObject> dbObjects = null;
		for (Object doc : from){
			putSizeCheck(doc);
			if (dbObjects == null){
				dbObjects = new LinkedList<DBObject>();
			}
			if (doc instanceof DBObject){
				_checkObject((DBObject)doc, false, false);
				dbObjects.add((DBObject)doc); // nothing to do
			} else {
				DBObject newObj = Mapper.createFromObject(doc);
				_checkObject(newObj, false, false);
				dbObjects.add(newObj);
			}
		}
		return dbObjects;
	}

	@Override
	public WriteResult update(DBObject q, DBObject o, boolean upsert,
			boolean multi, WriteConcern concern) {
		// TODO Auto-generated method stub
		return null;
	}



}
