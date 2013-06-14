/**
 * 	
 * Copyright 2013 Pagecrumb
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 */
package com.mungoae.object;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.labs.repackaged.com.google.common.collect.Lists;
import com.google.common.base.Preconditions;
import com.mungoae.BasicDBObject;
import com.mungoae.DBCollection;
import com.mungoae.DBObject;
import com.mungoae.ParameterNames;
import com.mungoae.query.Logical;
import com.mungoae.query.Update.UpdateOperator;
import com.mungoae.util.BoundedIterator;
import com.mungoae.util.Tuple;

/**
 * Interface between DB and the GAE Datastore entities.
 * So DB doesn't have to deal with to/from Entity/Object marshalling
 *  
 * @author Kerby Martino <kerbymart@gmail.com>
 *
 */
public class ObjectStore extends AbstractEntityStore implements ParameterNames {
	
	private static Logger LOG = LogManager.getLogger(ObjectStore.class.getName());
	
	/**
	 * GAE datastore supported types.
	 */
	private static final Set<Class<?>> GAE_SUPPORTED_TYPES =
	        DataTypeUtils.getSupportedTypes();	
	
	public ObjectStore(String namespace, String collection){
		super(namespace, collection);
	}
	
	//--------------------------------------------------------------------------------------------------
	// Interface that should be visible to clients
	//--------------------------------------------------------------------------------------------------	

	public List<Object> persistObjects(List<DBObject> objects){
		Preconditions.checkNotNull(objects, "Cannot persist null object list");
		List<Object> ids = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Transaction tx = _ds.beginTransaction();
		try {
			for (DBObject o : objects){
				if (ids == null){
					ids = new ArrayList<Object>();
				}
				Object id = null;
				String _id = null;
				// Pre-process, the datastore does not accept ObjectId as-is
				Object oid = o.get(DBCollection.MUNGO_DOCUMENT_ID_NAME);
				if (oid == null){
					LOG.debug("No id object found in the object, creating new");
					_id = new ObjectId().toStringMongod();
				} else {
					LOG.debug("ID object found, getting string id");	
					_id = createStringIdFromObject(oid);
				}
				o.put(DBCollection.MUNGO_DOCUMENT_ID_NAME, _id);
				// Persist to datastore, get back the Key
				Key key = persistEntity(null, Mapper.convertToMap(o)); 
				if (key != null){
					id = createIdObjectFromString(key.getName());
					ids.add(id);
				}				
			}
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
		} finally {
			if (tx.isActive())
				tx.rollback();
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		return ids;
	}
	
	/**
	 * Persist a DBObject to this DB under the given collection
	 * 
	 * <br>
	 * Use with: 
	 * <br>
	 * <br>
	 * <code>
	 * Object id = persistEntity(objStore.createObject(dbo));
	 * </code>
	 * @param object
	 * @param collection
	 * @return
	 */
	public Object persistObject(DBObject object){
		Preconditions.checkNotNull(object, "Cannot persist null object");
		Object id = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			String _id = null;
			// Pre-process, the Datastore does not accept ObjectId as is
			Object oid = object.get(DBCollection.MUNGO_DOCUMENT_ID_NAME);
			if (oid == null){
				LOG.debug("No id object found in the object, creating new");
				_id = new ObjectId().toStringMongod();
			} else {
				LOG.debug("ObjectId found, getting string id");
				_id = createStringIdFromObject(oid);
			}
			object.put(DBCollection.MUNGO_DOCUMENT_ID_NAME, _id);
			// Persist to datastore, get back the Key
			Key key = persistEntity(null, Mapper.convertToMap(object)); 
			if (key != null){
				id = createIdObjectFromString(key.getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		return id;
	}	
	
	/**
	 * 
	 * Query like;
	 * 
	 * {name: 'Joe'}
	 * 
	 * {name: { $e, 'Joe'}, age: { $gte, 20 }}
	 * 
	 * Order by like:
	 * 
	 * { age : -1 }
	 * 
	 * Filter query like:
	 * 
	 * { tags: { $all: [ "appliances", "school", "book" ] } 
	 * { qty: { $gt: 20 } 
	 * 
	 * Logical query like: 
	 * <code>
	 * { price:1.99, $or: [ { qty: { $lt: 20 } }, { sale: true } ] }
	 * </code>
	 * 
	 * @param query
	 * @param fields
	 * @param numToSkip
	 * @param batchSize
	 * @param limit
	 * @param options
	 * @return
	 */
	public Iterator<DBObject> queryObjects(DBObject query, DBObject orderby, DBObject fields, 
			Integer numToSkip , Integer limit , Integer batchSize, Integer options){
		Map<String, Tuple<FilterOperator, Object>> filters = Mapper.createFilterOperatorObjectFrom(query); 
		List<Tuple<Logical,List<Tuple<FilterOperator,Object>>>> logicalQuery = null; //Mapper.createLogicalQueryObjectFrom(query);
		Map<String, Query.SortDirection> sorts = Mapper.createSortObjectFrom(orderby); 
		if (filters.size() == 1 && filters.get("id") != null){ 
			return Lists.newArrayList(queryObjectById(filters.get("id") )).iterator();
		}
		return __queryObjects(filters, logicalQuery, sorts, numToSkip, limit, batchSize, options);
	}
	
	//--------------------------------------------------------------------------------------------------


	
	/**
	 * Get a DBObject from this DB and from the given collection.
	 * 
	 * Get by ID.
	 * 
	 * @param id
	 * @param collection
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@Deprecated
	public DBObject queryObject(DBObject id){
		Preconditions.checkNotNull(id, "Null id object");
		Preconditions.checkNotNull(id.get(DBCollection.MUNGO_DOCUMENT_ID_NAME), "ID cannot be null");
		
		DBObject obj = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			Map<String, Object> map = queryEntityBy(KeyStructure.createKey(_collName, 
					buildStringIdFromObject(id)));
			if (map != null){
				obj = new BasicDBObject(map);
			}
		} catch (Exception e) {
			LOG.error("Error: " + e.getMessage());
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		LOG.debug("Returning null DBObject");
		return obj;
	}	
	
	/**
	 * Query single object
	 * 
	 * @param id
	 * @return
	 */
	public DBObject queryObjectById(Object id){
		Preconditions.checkNotNull(id, "Null id object");
		
		DBObject obj = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			Map<String, Object> map = queryEntityBy(KeyStructure.createKey(_collName, 
					createStringIdFromObject(id)));
			if (map != null){
				obj = new BasicDBObject(map);
			}
		} catch (Exception e) {
			LOG.error("Error: " + e.getMessage());
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		LOG.debug("Returning null DBObject");
		return obj;		
	}

	/**
	 * Check if object exist with the given field values.
	 * Compares object as-is, including "_id" field if it exist
	 * 
	 * @param object
	 * @return
	 */
	public boolean containsObject(DBObject object){
		Entity e = Mapper.createEntityFromDBObject(object, _collName);
		return containsEntityWithFieldLike(e);
	}
	
	/**
	 * Checks if an object exist with a given id object. Object can be
	 * String, Number, or ObjectId
	 * 
	 * @param id
	 * @return
	 */
	public boolean containsId(Object id){
		Preconditions.checkNotNull(id, "Object id cannot be null");
		boolean contains = false;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			if (id instanceof String){
				contains = containsEntityKey(KeyStructure.createKey(_collName, (String) id)); 
			} else {
				contains = containsEntityKey(KeyStructure.createKey(_collName, 
						createStringIdFromObject(id))); // Safe?
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}			
		return contains;
	}

	
	/**
	 * Iterator for objects matching the given query. 
	 * Query in form: 
	 * <br>
	 * <br>
	 * <code>
	 * 		DBObject query = new BasicDBObject("type", new BasicDBObject("$e", "books")) 
	 *			.append("pages", new BasicDBObject("$gte", 500));	 	 
	 * </code>
	 *
	 * @param query
	 * @param collection
	 * @return
	 */
	private Iterator<DBObject> queryObjectsLike(DBObject query){
		Preconditions.checkNotNull(query, "Query object cannot be null");
		if (query.keySet().isEmpty()){
			LOG.debug("Empty query object");
		}
		validateQuery(query);
		/**
		 * Map of fields and its matching filter operator and compare value
		 */
		Map<String, Tuple<FilterOperator, Object>> filters
			= Mapper.createFilterOperatorObjectFrom(query);
		
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Iterator<DBObject> it = null;
		try {
			Entity mapped = Mapper.createEntityFromDBObject(query, _collName);
			final Iterator<Entity> eit = queryEntitiesLike(filters); 
			it = new Iterator<DBObject>() {
				public void remove() {
					eit.remove();
				}
				public DBObject next() {
					Entity e = eit.next();
					return Mapper.createDBObjectFromEntity(e);
				}
				public boolean hasNext() {
					return eit.hasNext();
				}
			};	
		} catch (Exception e) {
			e.printStackTrace();
			it = null;
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		if (it == null){
			LOG.debug("Returning null iterator");
		}
		return it;
	}
	

	
	/**
	 * 
	 * @param query
	 * @param orderby
	 * @return
	 */
	private Iterator<DBObject> queryObjectsLike(DBObject query, DBObject orderby){
		Preconditions.checkNotNull(query, "Reference object cannot be null");
		validateQuery(query);
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Iterator<DBObject> it = null;
		try {
			Map<String, Query.SortDirection> sorts = new LinkedHashMap<String,Query.SortDirection>();
			Iterator<String> kit = orderby.keySet().iterator();
			while (kit.hasNext()){
				String key = kit.next();
				int dir = (Integer) orderby.get(key);
				Query.SortDirection direction =  dir == 1 ? 
						Query.SortDirection.ASCENDING : null;
				direction = dir == -1 ? 
						Query.SortDirection.DESCENDING : direction;
				LOG.debug("Adding sort key="+key + " direction=" + direction); 
				sorts.put(key, direction);
			}
			final Iterator<Entity> eit  
				= querySortedEntitiesLike(Mapper.createFilterOperatorObjectFrom(query), sorts); 
			it = new Iterator<DBObject>() {
				public void remove() {
					eit.remove();
				}
				public DBObject next() {
					Entity e = eit.next();
					return Mapper.createDBObjectFromEntity(e);
				}
				public boolean hasNext() {
					return eit.hasNext();
				}
			};	
		} catch (Exception e) {
			e.printStackTrace();
			it = null;
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		if (it == null){
			LOG.debug("Returning null iterator");
		}
		return it;
	}	

	/**
	 * Helper method to get only the first object
	 * 
	 * @param obj
	 * @return
	 */
	public DBObject queryFirstObjectLike(DBObject obj){
		Iterator<DBObject> it = queryObjectsLike(obj);
		if (it != null)
			return it.next();
		return null;
	}
	
	// FIXME - Method is returning null always
	private Iterator<DBObject> queryObjectsOrderBy(DBObject orderby) {
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Iterator<DBObject> it = null;
		try {
			
			Map<String, Query.SortDirection> sorts = Mapper.createSortObjectFrom(orderby);

			final Iterator<Entity> eit = querySortedEntities(sorts);
			
			it = new Iterator<DBObject>() {
				public void remove() {
					eit.remove();
				}
				public DBObject next() {
					Entity e = eit.next();
					return Mapper.createDBObjectFromEntity(e);
				}
				public boolean hasNext() {
					return eit.hasNext();
				}
			};	
		} catch (Exception e) {
			e.printStackTrace();
			it = null;
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		if (it == null){
			LOG.debug("Returning null iterator");
		}
		return it;
	}
	
	public boolean deleteObject(Object _id){
		Preconditions.checkNotNull(_id, "Null ID");
		String id = createStringIdFromObject(_id);
		LOG.debug("Deleting document with id="+ id);
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			Key key = KeyStructure.createKey(_collName, id);
			deleteEntity(key);
			LOG.debug("Object deleted: " + key.getName());
			return containsEntityKey(key); 
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		return false;		
	}

	public static ObjectStore get(String namespace, String kind) {
		return new ObjectStore(namespace, kind);
	}
	
	/**
	 * Iterator for all objects in the datastore
	 * 
	 * @return
	 */
	private Iterator<DBObject> queryObjects() {
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Iterator<DBObject> it = null;
		try {
			final Iterator<Entity> eit = queryEntities();
			it = new Iterator<DBObject>() {
				public void remove() {
					eit.remove();
				}
				public DBObject next() {
					Entity e = eit.next();
					return Mapper.createDBObjectFromEntity(e);
				}
				public boolean hasNext() {
					return eit.hasNext();
				}
			};	
		} catch (Exception e) {
			it = null;
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		if (it == null){
			LOG.debug("Returning null iterator");
		}
		return it;
	}

	
	/**
	 * Update objects matching the query.
	 * 
	 * Query like:
	 * <br>
	 * <br>
	 * "{name: 'Joe', $inc: {age: 1}}"
	 * <br>
	 * <br>
	 * @param query
	 * @param upsert
	 * @param multi
	 */
	public void updateObjects(DBObject query, boolean upsert, boolean multi){
		Map<String, Tuple<FilterOperator, Object>> filters = Mapper.createFilterOperatorObjectFrom(query); 
		Map<String, Tuple<UpdateOperator, Object>> updates = Mapper.createUpdateOperatorFrom(query); 
		
		updateObjects(filters, updates, upsert, multi);
	}
	
	private void updateObjects(Map<String, Tuple<FilterOperator, Object>> filters, // Objects to get
			Map<String, Tuple<UpdateOperator, Object>> updates, // Updates to perform
			boolean insertIfNotExist, boolean updateAllMaching){ 
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			Iterator<Entity> it = queryEntitiesLike(filters);
			if (it != null){
				while(it.hasNext()){
					Entity e = it.next();
					Set<String> properties = e.getProperties().keySet();
					for (String prop : properties){
						if(updates.get(prop) != null){
							// Perform the update
							Object value = e.getProperty(prop);
							Tuple<UpdateOperator, Object> updateOp = updates.get(prop);
							if (updateOp.getFirst() == UpdateOperator.RENAME
									&& updateOp.getSecond() instanceof String) { // Rename field
								Object v = e.getProperty(prop);
								e.removeProperty(prop);
								e.setProperty((String)updateOp.getSecond(), v);
							}
							e.setProperty(prop, evalUpdateOperation(value, updateOp)); 
						}
					}
					_ds.put(e); // update
				}
			} else if (insertIfNotExist) { 
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}
	}

	
}
