package com.mungods.object;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.bson.types.ObjectId;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.CompositeFilter;
import com.google.appengine.api.datastore.Query.CompositeFilterOperator;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortPredicate;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.mungods.BasicDBObject;
import com.mungods.CommandResult;
import com.mungods.DB;
import com.mungods.DBCollection;
import com.mungods.DBObject;
import com.mungods.ParameterNames;
import com.mungods.collection.AbstractDBCollection;
import com.mungods.common.SerializationException;
import com.mungods.operators.Operator;
import com.mungods.serializer.ObjectSerializer;
import com.mungods.serializer.XStreamSerializer;
import com.mungods.util.Tuple;

/**
 * Middle class to interface between DB and the GAE Datastore entities.
 * So DB doesn't have to deal with to/from Entity/Object marshalling
 * 
 * TODO - Remove depenency to AbstractDBCollection
 * 
 * @author Kerby Martino <kerbymart@gmail.com>
 *
 */
public class ObjectStore extends AbstractDBCollection implements ParameterNames {
	
	private static final Logger logger 
		= Logger.getLogger(ObjectStore.class.getName());
	
	private final String _dbName;
	private final String _collName;
	
	private final ObjectSerializer _serializer;
	/**
	 * GAE datastore supported types.
	 */
	private static final Set<Class<?>> GAE_SUPPORTED_TYPES =
	        DataTypeUtils.getSupportedTypes();	
	
	public ObjectStore(String namespace, String collection){
		super(namespace);
		_dbName = namespace;
		_collName = collection;
		_serializer = new XStreamSerializer();
	}
	

	
	/**
	 * Get <code>String</code> id from a <code>Object</code>
	 * 
	 * @param docId
	 * @return
	 */
	private static String createStringIdFromObject(Object docId){
		Preconditions.checkNotNull(docId, "Object doc id cannot be null");
		if (docId instanceof ObjectId){
			logger.info("Create ID String from ObjectID");
			return ((ObjectId) docId).toStringMongod();
		} else {
			if (docId instanceof String
					|| docId instanceof Long){
				return String.valueOf(docId);
			}
//			try {
//				logger.info("Create ID String from arbitrary Object");
//				return serializer.serialize(id);
//			} catch (SerializationException e) {
//				e.printStackTrace();
//			}
		}
		return null;
	}
	

	
	/**
	 * Persist a DBObject to this DB under the given collection
	 * TODO - Rename to persistEntity
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
			Object oid = object.get(ID);
			if (oid == null){
				logger.info("No id object found in the object, creating new");
				_id = new ObjectId().toStringMongod();
			} else {
				logger.info("ObjectId found, getting string id");
//				if (oid instanceof ObjectId){
//					_id = ((ObjectId)oid).toStringMongod();
//				} else {
//					_id = serializer.serialize(oid);
//				}	
				_id = createStringIdFromObject(oid);
			}
			object.put(ID, _id);
			// Persist to datastore, get back the Key
			Key key = createEntity(null, Mapper.convertToMap(object)); 
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
	 * Get a DBObject from this DB and from the given collection.
	 * 
	 * Get by ID.
	 * 
	 * @param id
	 * @param collection
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public DBObject getObject(DBObject id){
		Preconditions.checkNotNull(id, "Null id object");
		
		DBObject obj = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			Map<String, Object> map = getEntityBy(KeyStructure.createKey(_collName, 
					buildStringIdFromObject(id)));
			if (map != null){
				obj = new BasicDBObject(map);
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Error: " + e.getMessage());
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		logger.warning("Returning null DBObject");
		return obj;
	}	

	/**
	 * Check if object exist
	 * 
	 * @param object
	 * @return
	 */
	public boolean containsObject(DBObject object){
		if (object.keySet().isEmpty()){
			throw new IllegalArgumentException("Empty object!");
		}
		if (object.get("_id") == null){
			throw new IllegalArgumentException("Object must have an id");
		}
		Entity e = Mapper.createEntityFromDBObject(object, _collName);
		return containsEntity(e);
	}
	
	private boolean containsEntity(Entity e){
		Preconditions.checkNotNull(e, "Entity cannot be null");
		boolean contains = false;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			Key key = e.getKey();
			logger.info("Fetching Entity with key="+key);
			Entity found = _ds.get(key);
			logger.info("Found entity="+found);
			if( found != null){
				return true;
			}
			//contains = containsEntityKey(key); // Safe?
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}			
		return contains;
	}
	
	
	public boolean containsKey(Object id){
		Preconditions.checkNotNull(id, "Object id cannot be null");
		boolean contains = false;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			contains = containsEntityKey(KeyStructure.createKey(_collName, 
					createStringIdFromObject(id))); // Safe?
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}			
		return contains;
	}
	

	
	/**
	 * Get the <code>DBObject</code>s that matches all
	 * of the fields in the object.
	 * 
	 * @param query
	 * @param collection
	 * @return
	 */
	public Iterator<DBObject> getObjectsLike(DBObject query){
		Preconditions.checkNotNull(query, "Query object cannot be null");
		if (query.keySet().isEmpty()){
			logger.warning("Empty query object");
		}
		
		/**
		 * Map of fields and its matching filter operator and compare value
		 */
		Map<String, Tuple<FilterOperator, Object>> filters
			= Mapper.createOperatorObjectFrom(query);
		
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Iterator<DBObject> it = null;
		try {
			Entity mapped = Mapper.createEntityFromDBObject(query, _collName);
			final Iterator<Entity> eit = getEntitiesLike(filters); 
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
			logger.warning("Returning null iterator");
		}
		return it;
	}
	
	private void validateQuery(DBObject query){
		if (query.keySet().isEmpty()){
			throw new IllegalArgumentException("Empty query");
		}
		for (String field : query.keySet()){
			Object operatorCompareValuePair = query.get(field);
			if (!(operatorCompareValuePair instanceof DBObject)){
				throw new IllegalArgumentException("Invalid query: " 
							+ operatorCompareValuePair);
			}
			logger.info("Query reference field=" 
					+ field 
					+ " reference operator/value=" 
					+ operatorCompareValuePair);
		}
	}
	
	/**
	 * 
	 * @param query
	 * @param orderby
	 * @return
	 */
	public Iterator<DBObject> getSortedObjectsLike(DBObject query, DBObject orderby){
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
				logger.info("Adding sort key="+key + " direction=" + direction); 
				sorts.put(key, direction);
			}
			final Iterator<Entity> eit  
				= getSortedEntitiesLike(Mapper.createOperatorObjectFrom(query), sorts); 
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
			logger.warning("Returning null iterator");
		}
		return it;
	}	
	
	
	// Helper get method
	public DBObject getFirstObjectLike(DBObject obj){
		Iterator<DBObject> it = getObjectsLike(obj);
		if (it != null)
			return it.next();
		return null;
	}
	
	public Iterator<DBObject> getObjects() {
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Iterator<DBObject> it = null;
		try {
			final Iterator<Entity> eit = getEntities();
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
			logger.warning("Returning null iterator");
		}
		return it;
	}
	
	// FIXME - Method is returning null always
	public Iterator<DBObject> getSortedObjects(DBObject orderby) {
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
				logger.warning("Adding sort key="+key + " direction=" + direction);
				sorts.put(key, direction);
			}
			
			final Iterator<Entity> eit = getSortedEntities(sorts);
			
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
			logger.warning("Returning null iterator");
		}
		return it;
	}
	
	/**
	 * Delete the object from this DB under the given collection
	 * 
	 * @param query
	 * @param collection
	 * @return
	 */
	public boolean deleteObject(DBObject query){
		Preconditions.checkNotNull(query, "Reference object cannot be null");
		if (query.get(ID) == null){
			throw new IllegalArgumentException("Can't delete from query object without id");
		} 
		if (query.keySet().isEmpty()){
			throw new IllegalArgumentException("Empty query object");
		}
		String id = createStringIdFromObject(query.get(ID));
		logger.warning("Deleting document with id="+ id);
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			Key key = KeyStructure.createKey(_collName, id);
			deleteEntity(key);
			return containsEntityKey(key); 
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		return false;
	}	

	/**
	 * Get a list of entities that matches the properties of a given 
	 * <code>Entity</code>.
	 * This does not include the id.
	 * 
	 * @param entity
	 * @param kind
	 * @return
	 */
	private Iterator<Entity> getEntitiesLike(Map<String, 
			Tuple<FilterOperator, Object>> query){
		Preconditions.checkNotNull(query, "Null query object");
		
		Query q = new Query(_collName);
		for (String propName : query.keySet()){
			Tuple<FilterOperator, Object> filterAndValue = query.get(propName);
			Filter filter = new FilterPredicate(propName, 
					filterAndValue.getFirst(), filterAndValue.getSecond()); 
			Filter prevFilter = q.getFilter();
			q = new Query(_collName).setFilter(prevFilter).setFilter(filter); 
		}
		PreparedQuery pq = _ds.prepare(q);
		return pq.asIterator();
	}	

	/**
	 * Builds a query filter from the given <code>Entity</code> property names and values and add sorting from
	 * <code>Map</code> sorts.
	 *  
	 * <br>
	 * <code>
	 *  Query q = new Query(kind).filter(f1).filter(f2).filter(fn).addSort(s).addSort(sn); // and so forth
	 * </code>
	 * 
	 * FIXME - Fix query object filter/sort getting wiped out
	 * 
	 * @param entity
	 * @param sorts
	 * @return
	 */
	private Iterator<Entity> getSortedEntitiesLike(
			Map<String, Tuple<FilterOperator, Object>> query, Map<String, SortDirection> sorts){
		Preconditions.checkNotNull(query, "Query object can't be null");
		Preconditions.checkNotNull(sorts, "Sort can't be null");
		logger.info("Query object="+query.toString());
		
		PreparedQuery pq = null;
	
		// Sort
		Iterator<Map.Entry<String, Query.SortDirection>> sortIterator = sorts.entrySet().iterator(); 
		while(sortIterator.hasNext()){
			Map.Entry<String, Query.SortDirection> dir = sortIterator.next();
			logger.info("Sort propName="+dir.getKey() + " direction="+dir.getValue());
		}
		Query q = new Query(_collName);
		Filter compositeFilter = null;
		List<Filter> subFilters = new ArrayList<Filter>();
		for (String propName : query.keySet()){
			logger.info("Filter Property name="+propName);
			Tuple<FilterOperator, Object> filterAndValue = query.get(propName);
			FilterOperator operator = filterAndValue.getFirst();
			Object value = filterAndValue.getSecond();
			Filter filter = new FilterPredicate(propName, operator, value); 
			Filter prevFilter = q.getFilter();
			if (sorts.get(propName) != null){
				q = new Query(_collName).setFilter(prevFilter).setFilter(filter)
						.addSort(propName, sorts.get(propName));
				sorts.remove(propName); // remove it
			} else {
				q = new Query(_collName).setFilter(prevFilter).setFilter(filter);
			}
			subFilters.add(filter);
		}		
		/*
		while(sortIterator.hasNext()){
			Map.Entry<String, Query.SortDirection> entry = sortIterator.next();
			// Get previous sort and append it
			List<SortPredicate> list = q.getSortPredicates();
			if (list != null && !list.isEmpty()){
				SortPredicate sp = list.get(0);
				list.remove(0); // so the sort predicate does not accumulate
				String prevField = sp.getPropertyName();
				SortDirection prevDirection = sp.getDirection();	
				Filter prevFilter = q.getFilter(); // this is the compositeFilter?
				q = new Query(_collName)
					//.setFilter(prevFilter) // this will be wiped out after the next iteration, so it will be copied to prevFilter
					.addSort(prevField, prevDirection)
					.addSort(entry.getKey(), entry.getValue());
			
			} else { // first time
				q = new Query(_collName)
					.setFilter(f)
					.addSort(entry.getKey(), entry.getValue());	
			}
			logger.info("Added sort by " + entry.getKey());
		}
		*/
		pq = _ds.prepare(q);
		return pq.asIterator();
	}	
	
	private <T> List<T> copyIterator(Iterator<T> it){
		List<T> copy = new ArrayList<T>();
		while (it.hasNext()){
		    copy.add(it.next());
		}
		return copy;
	}
	
	
	private Iterator<Entity> getEntities(){
		Query q = new Query(_collName);
		PreparedQuery pq = _ds.prepare(q);
		return pq.asIterator();
	}
	
	private Iterator<Entity> getSortedEntities(Map<String,Query.SortDirection> sorts){
		PreparedQuery pq = null;
		Iterator<Map.Entry<String, Query.SortDirection>> it = sorts.entrySet().iterator();
		Query q = new Query(_collName);
		while(it.hasNext()){
			Map.Entry<String, Query.SortDirection> entry = it.next();
			q = new Query(_collName)
				.addSort(entry.getKey(), entry.getValue());
	
		}
		pq = _ds.prepare(q);
		return pq.asIterator();		
	}

	/**
	 * Fetch a single entity by <code>Key</code>
	 * 
	 * @param k
	 * @return
	 */
	// FIXME - not returning null for non existing entity
	private Map<String,Object> getEntityBy(Key k){
		Preconditions.checkNotNull(k, "Entity key cannot be null");
		
		if (!containsEntityKey(k)){
			return null;
		}
		
		Map<String,Object> doc = null;	
		try {
			Entity e = _ds.get(k);
			doc = new LinkedHashMap<String, Object>();
			Map<String,Object> props = e.getProperties();
			Iterator<Map.Entry<String, Object>> it = props.entrySet().iterator();
			// Preprocess - 
			// Can't putAll directly since List and Map
			while(it.hasNext()){
				Map.Entry<String, Object> entry = it.next();
				String key = entry.getKey();
				Object val = entry.getValue();
				if (val == null){
					doc.put(key, val);
				} else if (val instanceof String
						|| val instanceof Number
						|| val instanceof Boolean
						|| val instanceof Date) {
					// TODO - Check if String is a serialized object
					if (val instanceof String){
						// Try to deserialize
						if(deserializeString((String)val) != null ){
							val = deserializeString((String)val);
						}
					}
					logger.info("Inserting to document key="+key);
					logger.info("Inserting to document value type="+val.getClass().getName());
					doc.put(key, val);
				} else if (val instanceof EmbeddedEntity) { // List and Map are stored as EmbeddedEntity internally
					logger.log(Level.INFO, "Embedded entity found.");
					Object mapOrList = Mapper.getMapOrList((EmbeddedEntity) val);
					if (mapOrList instanceof List){
						logger.log(Level.INFO, "Embedded List="+mapOrList);
					} else if (mapOrList instanceof Map){
						logger.log(Level.INFO, "Embedded Map="+mapOrList);
					}
					doc.put(key, mapOrList);	
				} 
			}
			//json.put(ID, e.getKey().getName());
			Object _id = createIdObjectFromString(e.getKey().getName());
			if (_id instanceof ObjectId){
				doc.put(ID, ((ObjectId)_id).toStringMongod()); 
			} else if (_id instanceof Long) {
				doc.put(ID, (Long)_id);
			} else {
				doc.put(ID, _id); 
			}
			
		} catch (EntityNotFoundException e) {
			// Just return null
			doc = null;
		} finally {
			
		}
		logger.warning("Returning null document");
		return doc;
	}
	
	// Used to try to deserialize a String, if it can't 
	// it will just return null
	private Object deserializeString(String value){
		try {
			//logger.info("Parsing string="+value);
			Object deserialized = _serializer.deserialize(value);
			return deserialized;
		} catch (SerializationException e){
			// do nothing
		} catch (Exception e) {
			// do nothing
		}
		return null;
	}
	
	
	/**
	 * Check if in the current <code>Namespace</code> of the current <code>Kind</code>
	 * contains the entity using a Key.
	 *  
	 * @param key
	 * @return
	 */
	private boolean containsEntityKey(Key key){
		Preconditions.checkNotNull(key, "Entity key cannot be null");
		Transaction tx = _ds.beginTransaction();
		try {
			Query q 
				= new Query(_collName)
					.setFilter(new FilterPredicate(Entity.KEY_RESERVED_PROPERTY, 
							FilterOperator.EQUAL, key)).setKeysOnly(); 
			PreparedQuery pq = _ds.prepare(q);
			Entity e = pq.asSingleEntity();			
			if (e != null)
				return true;
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
		} finally {
		    if (tx.isActive()) {
		        tx.rollback();
		    }
		}		
		return false;
	}
	
	/**
	 * Check whether entity with the given properties exist having equal
	 * field and value. 
	 * 
	 * All fields must be equal
	 * 
	 * @param props
	 * @return
	 */
	private boolean containsEntityLike(Map<String, Tuple<FilterOperator, Object>> query){
		Preconditions.checkNotNull(query, "Query object cannot be null");
		boolean contains = false;
	
		Preconditions.checkNotNull(query, "Null query object");

		Transaction tx = _ds.beginTransaction();
		try {
			Query q = new Query(_collName);
			for (String propName : query.keySet()){
				Tuple<FilterOperator, Object> filterAndValue = query.get(propName);
				Filter filter = new FilterPredicate(propName, 
						filterAndValue.getFirst(), filterAndValue.getSecond()); 
				Filter prevFilter = q.getFilter();
				q = new Query(_collName).setFilter(prevFilter).setFilter(filter); 
				q = q.setKeysOnly();
			}
			PreparedQuery pq = _ds.prepare(q);
			int c = pq.countEntities(FetchOptions.Builder.withDefaults());
			if (c != 0)
				contains = true;
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
		} finally {
		    if (tx.isActive()) {
		        tx.rollback();
		    }
		}		
		return contains;
	}
	
	
	private void deleteEntity(Key key){
		Preconditions.checkNotNull(key, "Entity key cannot be null");
		Transaction tx = _ds.beginTransaction();
		try {
			_ds.delete(key);
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
		} finally {
		    if (tx.isActive()) {
		        tx.rollback();
		    }
		}
	}	

    private void setProperty(Entity entity, String key, Object value){
    	Preconditions.checkNotNull(entity, "Entity can't be null");
    	Preconditions.checkNotNull(key, "String key can't be null");
    	Preconditions.checkNotNull(value, "Value can't be null");
	    if (!GAE_SUPPORTED_TYPES.contains(value.getClass())
        && !(value instanceof Blob) && !(value instanceof EmbeddedEntity)) {
        throw new RuntimeException("Unsupported type[class=" + value.
                getClass().getName() + "] in Latke GAE repository");
	    }
	    if (value instanceof String) {
	        final String valueString = (String) value;
	        if (valueString.length()
	            > DataTypeUtils.MAX_STRING_PROPERTY_LENGTH) {
	            final Text text = new Text(valueString);
	
	            entity.setProperty(key, text);
	        } else {
	            entity.setProperty(key, value);
	        }
	    } else if (value instanceof Number
	               || value instanceof Date
	               || value instanceof Boolean
	               || GAE_SUPPORTED_TYPES.contains(value.getClass())) {
	        entity.setProperty(key, value);
	    } else if (value instanceof EmbeddedEntity) {
	    	entity.setProperty(key, value);
	    } else if (value instanceof Blob) {
	        final Blob blob = (Blob) value;
	        entity.setProperty(key,
	                           new com.google.appengine.api.datastore.Blob(
	                blob.getBytes()));
	    }  
    }

	/**
	 * Create a <clas>Object</code> id from a given string. 
	 * It first try to deserialize the String with XStream if it
	 * fails it creates a new ObjectId with the given String. Since
	 * there is only two type of String id that is stored using Mungo
	 * which is a ObjectId string and a XStream serialized Object.
	 * 
	 * FIXME - Bug, when a String docId is a number string
	 * it is interpreted as Long
	 * 
	 * @param docId
	 * @return
	 */
	private Object createIdObjectFromString(String docId){
		Object id = docId;
		try {
			//id = serializer.deserialize(uniqueId);
			id = Long.valueOf(docId);
		} catch (NumberFormatException e) {
			try {
				id = new ObjectId(docId);
			} catch (IllegalArgumentException e2) {
				id = String.valueOf(docId);
			}
		} 
		return id;
	}
	
	/**
	 * Helper method to convert a list of <code>Entity</code> to
	 * list of <code>Map</code>.
	 * 
	 * @param entities
	 * @return
	 */
	private static List<Map<String,Object>> entitiesToMap(List<Entity> entities){
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		for (Entity e : entities){
			Map<String,Object> m = e.getProperties();
			list.add(m);
		}
		return list;
	}

	
	/**
	 * Create an entity for a a <code>Map</code>
	 * Note that this method does not enforce any rule in persisting an entity. 
	 * It however may call subsequent datastore <code>put</code>s as necessary when
	 * dealing with embedded documents. It also does not restrict the operation on 
	 * a spefic Namespace, it is however should be managed by that method that calls this method.
	 * 
	 * This method expects the id of the Entity to be in the Map with key "_id"
	 * 
	 * This method also expects that the <code>Map</code> entries are already pre-processed to 
	 * GAE types, otherwise those properties will get thrown out. 
	 * 
	 * @param parent when provided becomes the parent key of the created Entity, can be set to null
	 * @param obj
	 */
	private Key createEntity(Key parent, Map obj){	
		Key entityKey = null;
		try {
			Object id = obj.get(ID);
			Entity e = new Entity(
					parent == null ? KeyStructure.createKey(_collName, createStringIdFromObject(id)) : parent);  
			// Clean up the objectId (since the DS have ID field)
			// and since it is already 'copied' into the Entity
			obj.remove(ID);
			Iterator it = obj.keySet().iterator();
			while (it.hasNext()){
				String key = (String) it.next();
				Object value = obj.get(key);
				if (value == null){
					e.setProperty(key, null);
				} else if (value instanceof ObjectId) {
					// FIXME - Check if this is correct
					setProperty(e, key, ((ObjectId) value).toStringMongod());
				} else if (value instanceof String) {
					setProperty(e, key, value);
				} else if(value instanceof Number) {
					setProperty(e, key, value);
				} else if(value instanceof Boolean) {
					setProperty(e, key, value);
				} else if(value instanceof Date) {
					setProperty(e, key, value);
				} else if(value instanceof List) {
					logger.log(Level.INFO, "Processing List value");
					setProperty(e, key, Mapper.createEmbeddedEntityFromList(parent, (List) value));
				} else if(value instanceof Map){
					// TODO: Need to deal with sub-documents Object id
					logger.log(Level.INFO, "Processing Map value");
					setProperty(e, key, Mapper.createEmbeddedEntityFromMap(parent, (Map) value));
				} else {
					//throw new RuntimeErrorException(null, "Unsupported value type: " + value.getClass().getName());
					String serialized = _serializer.serialize(value);
					setProperty(e, key, serialized);
				}
			}	
			logger.log(Level.INFO, "Persisting entity [" 
					+ e.getKey().getName() + "] in [" + e.getNamespace() + "][" + e.getKind() + "]");
			entityKey = _ds.put(e);
			logger.log(Level.INFO, "Persisted entity [" + e + "] with key=" + entityKey);
			logger.log(Level.INFO, "Raw id was="+id);
		} catch (ConcurrentModificationException e){
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return entityKey;
	}	
	
	public static ObjectStore get(String namespace, String kind) {
		return new ObjectStore(namespace, kind);
	}
	
	// Helper method
	private static String buildStringIdFromObject(DBObject obj){
		return createStringIdFromObject(obj.get(ID));
	}
	
}
