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

import org.apache.commons.lang3.ArrayUtils;
import org.bson.types.ObjectId;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.common.base.Preconditions;
import com.mungods.BasicDBObject;
import com.mungods.CommandResult;
import com.mungods.DB;
import com.mungods.DBCollection;
import com.mungods.DBObject;
import com.mungods.ParameterNames;
import com.mungods.collection.AbstractDBCollection;

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
	/**
	 * GAE datastore supported types.
	 */
	private static final Set<Class<?>> GAE_SUPPORTED_TYPES =
	        DataTypeUtils.getSupportedTypes();	
	
	public ObjectStore(String namespace, String collection){
		super(namespace);
		_dbName = namespace;
		_collName = collection;
	}
	

	
	/**
	 * Get <code>String</code> id from a <code>Object</code>
	 * 
	 * @param docId
	 * @return
	 */
	private String createStringIdFromObject(Object docId){
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
		Object id = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			String _id = null;
			// Pre-process, the Datastore does not accept ObjectId as is
			//ObjectId oid = (ObjectId) object.get(ID);
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
				//id = new ObjectId(key.getName());
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
	 * Get by ID.
	 * 
	 * @param object
	 * @param collection
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public DBObject getObject(DBObject object){
		DBObject obj = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			String id = createStringIdFromObject(object.get(ID));
			Map<String, Object> map = getEntity(KeyStructure.createKey(_collName, id));
			obj = new BasicDBObject();
			obj.putAll(map);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		return obj;
	}	
	
	public boolean contains(DBObject object){
		return getObject(object) == null ? false : true;
	}
	
	public boolean containsKey(Object id){
		if (id == null)
			return false;
		boolean contains = false;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			String _id = createStringIdFromObject(id); 
//			if (id instanceof ObjectId){
//				_id = ((ObjectId) id).toStringMongod();
//			} else {
//				_id = id.toString();
//			}
			contains = containsEntityKey(KeyStructure.createKey(_collName, _id)); // Safe?
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
	 * @param object
	 * @param collection
	 * @return
	 */
	public Iterator<DBObject> getObjectsLike(DBObject object){
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Iterator<DBObject> it = null;
		try {
			final Iterator<Entity> eit  
				= getEntitiesLike(Mapper.createEntityFromDBObject(object, _collName)); 
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
		return it;
	}
	
	/**
	 * Delete the object from this DB under the given collection
	 * 
	 * @param object
	 * @param collection
	 * @return
	 */
	public boolean deleteObject(DBObject object){
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		boolean result = false;
		try {
			//ObjectId id = (ObjectId) object.get(ID);
			String id = createStringIdFromObject(object.get(ID));
			result = deleteEntity(KeyStructure.createKey(_collName, id));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		return result;
	}	

	/**
	 * Get a list of entities that matches the properties of a given <code>Entity</code>.
	 * This does not include the id
	 * 
	 * @param entity
	 * @param kind
	 * @return
	 */
	public Iterator<Entity> getEntitiesLike(Entity entity){
		logger.info("Fetching entities like: " + entity);
		Map<String,Object> m = entity.getProperties();
		Iterator<String> it = m.keySet().iterator();
		Query q = new Query(_collName);
		while (it.hasNext()){ // build the query
			String propName = it.next();
			Filter filter = new FilterPredicate(propName, 
				FilterOperator.EQUAL, m.get(propName));
			Filter prevFilter = q.getFilter();
			// Note that the Query object is immutable
			q = new Query(_collName).setFilter(prevFilter).setFilter(filter); 
		}
		PreparedQuery pq = _ds.prepare(q);
		return pq.asIterator();
	}

	public Map<String,Object> getEntity(Key k){
		Map<String,Object> json = null;
		if (k == null)
			return null;		
		try {
			json = new LinkedHashMap<String, Object>();
			Entity e = _ds.get(k);
			Map<String,Object> props = e.getProperties();
			Iterator<Map.Entry<String, Object>> it = props.entrySet().iterator();
			// Preprocess - 
			// Can't putAll directly since List and Map
			while(it.hasNext()){
				Map.Entry<String, Object> entry = it.next();
				String key = entry.getKey();
				Object val = entry.getValue();
				if (val == null){
					json.put(key, val);
				} else if (val instanceof String
						|| val instanceof Number
						|| val instanceof Boolean) {
					json.put(key, val);
				} else if (val instanceof EmbeddedEntity) { // List and Map are stored as EmbeddedEntity internally
					logger.log(Level.INFO, "Embedded entity found.");
					Object mapOrList = Mapper.getMapOrList((EmbeddedEntity) val);
					if (mapOrList instanceof List){
						logger.log(Level.INFO, "Embedded List="+mapOrList);
					} else if (mapOrList instanceof Map){
						logger.log(Level.INFO, "Embedded Map="+mapOrList);
					}
					json.put(key, mapOrList);	
				} 
			}
			//json.put(ID, e.getKey().getName());
			Object _id = createIdObjectFromString(e.getKey().getName());
			if (_id instanceof ObjectId){
				json.put(ID, ((ObjectId)_id).toStringMongod()); 
			} else if (_id instanceof Long) {
				json.put(ID, (Long)_id);
			} else {
				json.put(ID, _id); 
			}
			
		} catch (EntityNotFoundException e) {
			// Just return null
		} finally {
			
		}
		return json;
	}
	
	
	public boolean containsEntityKey(Key key){
		if (key == null)
			return false;
		Transaction tx = _ds.beginTransaction();
		try {
			Entity e = _ds.get(key);
			if (e != null)
				return true;
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			return false;
		} finally {
		    if (tx.isActive()) {
		        tx.rollback();
		    }
		}		
		return false;
	}
	
	
	public boolean deleteEntity(Key key){
		if (key == null)
			return false;
		Transaction tx = _ds.beginTransaction();
		try {
			_ds.delete(key);
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			return false;
		} finally {
		    if (tx.isActive()) {
		        tx.rollback();
		    }
		}
		return true;
	}	

    protected void setProperty(Entity entity, String key, Object value){
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
	public Key createEntity(Key parent, Map obj){	
		Key entityKey = null;
		try {
			String id = (String) obj.get(ID);
			Entity e = new Entity(
					parent == null ? KeyStructure.createKey(_collName, id) : parent);  
			// Clean up the objectId (since the DS have ID field)
			// and since it is already 'copied' into the Entity
			obj.remove(ID);
			Iterator it = obj.keySet().iterator();
			while (it.hasNext()){
				String key = (String) it.next();
				Object value = obj.get(key);
				if (value == null){
					e.setProperty(key, null);
				} else if (value instanceof String) {
					setProperty(e, key, value);
				} else if(value instanceof Number) {
					setProperty(e, key, value);
				} else if(value instanceof Boolean) {
					setProperty(e, key, value);
				} else if(value instanceof List) {
					logger.log(Level.INFO, "Processing List value");
					setProperty(e, key, Mapper.createEmbeddedEntityFromList(parent, (List) value));
				} else if(value instanceof Map){
					// TODO: Need to deal with sub-documents Object id
					logger.log(Level.INFO, "Processing Map value");
					setProperty(e, key, Mapper.createEmbeddedEntityFromMap(parent, (Map) value));
				}
			}	
			logger.log(Level.INFO, "Persisting entity to the datastore");
			entityKey = _ds.put(e);
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
	
}
