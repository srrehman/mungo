package com.mungoae.collection.simple;

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
import com.mungoae.collection.WriteConcern;
import com.mungoae.collection.WriteResult;
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
			// TODO: handle exception
		}
		return null;
	}
	
	@Override
	public DBObject findOne(String query) {
		try {
			DBCursor curr = find(query);
			if (curr.hasNext()){
				return curr.next();
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
			e.printStackTrace();
		}
		return null;
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
	
	/**
	 * Construct a DBObject from a given POJO
	 * 
	 * @param obj
	 * @return
	 */
	private <T> DBObject createFromObject(T obj){
		// Process annotations
		// Get the id
		// Get the fields
		return null;
	}
	
	@Override
	protected Iterator<DBObject> __find(
			Map<String, Tuple<FilterOperator, Object>> filters,
			Map<String, SortDirection> sorts, Integer numToSkip, Integer limit,
			Integer batchSize, Integer options) {
		LOG.debug("Applying filters to query="+filters);
		if (filters.size() == 1 && filters.get("id") != null){
			return Lists.newArrayList(_store.queryObjectById(filters.get("id").getSecond())).iterator();
		}
		return _store.queryObjects(filters, sorts, numToSkip, limit, batchSize, options);
	}
	
	@Override
	public Iterator<DBObject> __find(final DBObject ref, final DBObject fields,
			int numToSkip, int batchSize, int limit, int options) {
		if (ref == null){
			final Iterator<DBObject> _it = _store.queryObjects();
			if (fields != null){
				// Copy iterator to filter fields
				Iterator<DBObject> copy = new Iterator<DBObject>() {
					@Override
					public void remove() {
						_it.remove();
					}
					@Override
					public DBObject next() {
						return copyFields(_it.next(), fields); 
					}
					@Override
					public boolean hasNext() {
						return _it.hasNext();
					}
				};
				return copy;
			}
			return _it; 	
		} else {
			// TODO - Right now this only process the ID and ignores
			// other fields, fix this!
			Object id = ref.get(DBCollection.MUNGO_DOCUMENT_ID_NAME);
			if (id != null){
				return Lists.newArrayList(_store.queryObjectById(id)).iterator();					
			}
		}

		//---------------------------------------------------------------------
		// Migrated from old DBCollection
		
		DBObject query = (DBObject) ref.get("$query"); // filter w/c docs to get
		DBObject orderby = (DBObject) ref.get("$orderby"); // order 
		
		// Remove special fields
		dataCleansing(ref);

		if (query == null){ // no query? return an iterator to all documents
			return _store.queryObjects();
		}
		
		Iterator<DBObject> it = null;
		if (query != null && query.toMap().size() != 0){
			LOG.info("Query object=" + query);
			if (orderby != null && orderby.toMap().size() != 0){
				LOG.info("Sort object=" + orderby);
				it = _store.queryAllObjectsLike(query, orderby);
			} else {
				it = _store.queryObjectsLike(query);
			}		
		} else {
			if (orderby != null && orderby.toMap().size() != 0){
				LOG.info("Sort object=" + orderby);
				it = _store.queryObjectsOrderBy(orderby);
			} else { // query and orderby are both null
				if (ref.get(MUNGO_DOCUMENT_ID_NAME) != null){ 
					DBObject obj = _store.queryObjectById(ref.get(MUNGO_DOCUMENT_ID_NAME));
				}
				it = _store.queryObjects();
			}	
		}
		if (limit > 0 && numToSkip > 0){
			return new BoundedIterator<DBObject>(numToSkip, limit, it);	
		} else if (limit > 0 && numToSkip == 0){
			return new BoundedIterator<DBObject>(0, limit, it);	
		} else if (limit == 0 && numToSkip > 0){
			return new BoundedIterator<DBObject>(numToSkip, 0, it);	
		}
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
				ref.removeField(field);
			}
		}
		return ref;
	}

	
	// TODO
	// Must implement size check to validate if 
	// object exceed maximum datastore entity 
	// byte size
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
				DBObject newObj = createFromObject(doc);
				_checkObject(newObj, false, false);
				dbObjects.add(newObj);
			}
		}
		return dbObjects;
	}

}
