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
package com.pagecrumb.joongo.collection;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.types.ObjectId;


import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.common.base.Preconditions;
import com.pagecrumb.joongo.ParameterNames;
import com.pagecrumb.joongo.collection.simple.BasicDBCollection;
import com.pagecrumb.joongo.entity.BasicDBObject;

/**
 * Datastore interface that provides namespacing. 
 * Usually this object should be retrieved from the singleton 
 * <code>Joongo</code> object and should not be instantiated directly 
 * or through <code>SimpleDB</code>.
 * 
 * @author Kerby Martino<kerbymart@gmail.com> 
 *
 */
public abstract class DB extends AbstractDBCollection implements ParameterNames {
	
	private static final Logger logger 
		= Logger.getLogger(DB.class.getName());

	protected final String _dbName;
	
	protected Key _dbkey;
	/**
	 * GAE datastore supported types.
	 */
	private static final Set<Class<?>> GAE_SUPPORTED_TYPES =
	        DataTypeUtils.getSupportedTypes();	

	public DB(Key key, String namespace){
		super(namespace);
		this._dbName = namespace;
		this._dbkey = key;
	}
	
	public String getName(){
		return this._dbName;
	}
	
	public Long getCreated() {		
		return null;
	}
	
	public Long getUpdated() {
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE);
		try {
			
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		return null;
	}
	
	/**
	 * Creates a new collection
	 * 
	 * @param collection
	 * @return
	 */
	public DBCollection createCollection(String collection) {
		DBCollection col = null; 
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE);
		try {
			Entity e = getCollectionEntity(collection);
			if (e == null) {
				e = new Entity(createCollectionKey(collection));
				e.getKey(); // where to inject this to?
				col = new BasicDBCollection(this, collection);
			} else {
				e = getCollectionEntity(collection);
				e.getProperty(DATABASE_NAME); // Sets where this collection belongs
				e.getProperty(COLLECTION_NAME);
				e.getProperty(CREATED);
				e.getProperty(UPDATED);
				col = new BasicDBCollection(this, collection);
			}
			_ds.put(e);
		} catch (Exception e) {
			// TODO: rollback
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}
		return col;
	}
	
	/**
	 * Gets a collection to store/remove object with
	 * 
	 * @param collection
	 * @return
	 */
	public DBCollection getCollection(String collection){
		DBCollection col = null; 
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE); 
		try {
			Entity e = getCollectionEntity(collection);
			if (e == null) {
				col = createCollection(collection);
			} else {
				col = new BasicDBCollection(this, collection);
			}
			// extract the data from the datastore entity
			// unused right now
			Properties props = new Properties();
			props.put(DATABASE_NAME, _dbName);
			props.put(COLLECTION_NAME, collection);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}
		return col;
	}
	
	/**
	 * Deletes a collection and all of the objects in it
	 * 
	 * @param collection
	 * @return
	 */
	public boolean deleteCollection(String collection) {
		boolean result = false;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE);
		try {
			result = deleteCollectionEntity(collection);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		return result;
	}	
	
	public List<DBCollection> getCollections() {
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE);
		List<DBCollection> cols = new ArrayList<DBCollection>();
		try {
			Query q = new Query(COLLECTION_KIND)
				.setAncestor(_dbkey);
			// Filter collections that belongs 
			// to this DB
//			Filter dbNameFilter = new FilterPredicate(DATABASE_NAME, 
//					FilterOperator.EQUAL, _dbName);
//			q.setFilter(dbNameFilter);
			PreparedQuery pq = _ds.prepare(q);
			List<Entity> result = pq.asList(FetchOptions.Builder.withDefaults());
			for (Entity e : result) {
				cols.add(new BasicDBCollection(this, (String) e.getProperty(COLLECTION_NAME)));
			}			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}	
		return cols;
 	}
	
	public long countCollections(){		
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE);
		Preconditions.checkNotNull(_dbkey,"DB key cannot be null");
		try {
			Query q = new Query(COLLECTION_KIND)
				.setAncestor(_dbkey);
			PreparedQuery pq = _ds.prepare(q);
			List<Entity> result = pq.asList(FetchOptions.Builder.withDefaults());
			return result.size();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}	
		return 0;
	}
	
	
	public abstract DBObject command(DBObject cmd);
	
	//
	// internal stuff
	//
	protected void createCollectionEntity(String collection) {
		Entity e = new Entity(createCollectionKey(collection));
		e.setProperty(DATABASE_NAME, _dbName);
		e.setProperty(CREATED, cal.getTime().getTime());
		// persist the entity
		_ds.put(e);
	}	
	
	protected Entity getCollectionEntity(String collection) {
		try {
			Key key = createCollectionKey(collection);
			Entity e = _ds.get(key);
			return e;
		} catch (EntityNotFoundException ex) {
			return null;
		}
	}
	
	protected boolean deleteCollectionEntity(String name) {
		Entity e = getCollectionEntity(name);
		if (e == null)
			return false;

		Transaction tx = _ds.beginTransaction();
		boolean success = true;

		try {
			// delete the kind entity
			_ds.delete(tx, e.getKey());

			// now delete all documents belonging to this kind
			// FIXME not implemented yet !!!

			// done!
			tx.commit();
		} catch (Exception ex) {
			tx.rollback();
			success = false;
		} finally {
		    if (tx.isActive()) {
		        tx.rollback();
		    }
		}

		return success;
	}		
	
	protected int entities(String kind) {
		if (getCollectionEntity(kind) == null)
			return 0;

		// TODO maybe reuse queries...
		Query q = new Query(kind);
		PreparedQuery pq = _ds.prepare(q);
	
		// return the number of entities of this kind
		return pq.countEntities();
	}
	
	/**
	 * Persist a DBObject to this DB under the given collection
	 * 
	 * @param object
	 * @param collection
	 * @return
	 */
	public ObjectId createObject(DBObject object, String collection){
		ObjectId id = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			String _id = null;
			// Pre-process, the Datastore does not accept ObjectId as is
			ObjectId oid = (ObjectId) object.get(ID);
			if (oid == null){
				logger.info("No id object found in the object, creating new");
				_id = new ObjectId().toStringMongod();
			} else {
				logger.info("ObjectId found, getting string id");
				_id = oid.toStringMongod();
			}
			object.put(ID, _id);
			Key key = createEntity(null, collection, object);
			if (key != null)
				id = new ObjectId(key.getName());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		return id;
	}
	
	/**
	 * Get a DBObject from this DB and from the given collection
	 * 
	 * @param object
	 * @param collection
	 * @return
	 */
	public DBObject getObject(DBObject object, String collection){
		DBObject obj = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			ObjectId id = (ObjectId) object.get(ID);
			Map<String, Object> map = getEntity(createKey(collection, id.toStringMongod()));
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
	
	public boolean contains(DBObject object, String collection){
		return getObject(object, collection) == null ? false : true;
	}
	
	public boolean containsKey(Object id, String collection){
		if (id == null)
			return false;
		boolean contains = false;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		try {
			String _id;
			if (id instanceof ObjectId){
				_id = ((ObjectId) id).toStringMongod();
			} else {
				_id = id.toString();
			}
			contains = containsEntityKey(createKey(collection, _id)); // Safe?
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}			
		return contains;
	}
	
	private Entity createEntityFromDBObject(DBObject object, String kind){
		Preconditions.checkNotNull(object, "DBObject cannot be null");
		Preconditions.checkNotNull(kind, "Entity kind cannot be null");
		Entity e = null;
		// Pre-process object id
		if (object.get(ID) != null){
			Object oid = object.get(ID);
			if (oid instanceof ObjectId){
				e = new Entity(createKey(((ObjectId) oid).toStringMongod(), kind));
			} else if (oid instanceof String){
				e = new Entity(createKey((String)oid, kind));
			} else {
				// FIXME This could be really unsafe
				e = new Entity(createKey(oid.toString(), kind));
			}
		}
		Iterator it = object.entrySet().iterator();
		while (it.hasNext()){
			if (e == null) {
				e = new Entity(createKey(new ObjectId().toStringMongod(), kind));
			}
			Object entry = it.next();
			try {
				Map.Entry<Object, Object> mapEntry
					= (Entry<Object, Object>) entry;
				// Key at this point is still raw
				Object key = mapEntry.getKey();
				Object value = mapEntry.getValue();
				if (key instanceof String
						&& !((String) key).equals(ID)){ // skip the object id
					if (value instanceof Map){
						e.setProperty((String)key, createEmbeddedEntityFromMap(null, (Map)value));
					} else if (value instanceof List){
						throw new RuntimeException("List values are not yet supported");
					} else if (value instanceof String 
							|| value instanceof Number
							|| value instanceof Boolean) {
						e.setProperty((String)key, value);
					} else {
						throw new RuntimeException("Unsupported DBObject property type");
					}
				}
			} catch (ClassCastException ex) {
				// Something is wrong here
			}
		}	
		return e;
	}
	
	/**
	 * 
	 * @param object
	 * @param collection
	 * @return
	 */
	public Iterator<DBObject> getObjectsLike(DBObject object, String collection){
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Iterator<DBObject> it = null;
		try {
			final Iterator<Entity> eit  
				= getEntitiesLike(createEntityFromDBObject(object, collection), collection); 
			it = new Iterator<DBObject>() {
				public void remove() {
					eit.remove();
				}
				public DBObject next() {
					Entity e = eit.next();
					return createDBObjectFromEntity(e);
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
	public boolean deleteObject(DBObject object, String collection){
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		boolean result = false;
		try {
			ObjectId id = (ObjectId) object.get(ID);
			result = deleteEntity(createKey(collection, id.toStringMongod()));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		return result;
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
	protected Key createEntity(Key parent, String kind, Map obj){	
		Key entityKey = null;
		try {
			String id = (String) obj.get(ID);
			Entity e = new Entity(
					parent == null ? createKey(kind, id) : parent);  
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
					setProperty(e, key, obj.get(key));
				} else if(value instanceof Number) {
					setProperty(e, key, obj.get(key));
				} else if(value instanceof Boolean) {
					setProperty(e, key, obj.get(key));
				} else if(value instanceof List) {
					// Problem area, right way to store a list? 
					// List may contain JSONObject too!
					logger.log(Level.INFO, "Processing List value");
					setProperty(e, key, createEmbeddedEntityFromList(parent, key, (List) obj.get(key)));
				} else if(value instanceof Map){
					// TODO: Need to deal with sub-documents Object id
					logger.log(Level.INFO, "Processing Map value");
					setProperty(e, key, createEmbeddedEntityFromMap(parent, (Map) obj.get(key)));
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
	
	/**
	 * Process <code>EmbeddedEntity</code> and inner <code>EmbeddedEntity</code>
	 * of this entity. 
	 * 
	 * @param ee
	 * @return
	 */
	private Map<String,Object> getMapFromEmbeddedEntity(final EmbeddedEntity ee){
		Map<String,Object> map = null;
		try {
			map = new HashMap<String, Object>();
			map.putAll(ee.getProperties());
			
			Map<String,Object> newMap = new HashMap<String, Object>(); 
			Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<String, Object> entry = it.next();
				if (entry.getValue() instanceof EmbeddedEntity){
					logger.log(Level.INFO, "Inner embedded entity found with key=" + entry.getKey());
					newMap.put(entry.getKey(), getMapFromEmbeddedEntity( (EmbeddedEntity) entry.getValue()));
					it.remove();
				}
			}
			map.putAll(newMap);
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Error when processing EmbeddedEntity to Map");
		}
		return map;
	}
	
	/**
	 * Create <code>EmbeddedEntity</code> from List
	 * 
	 * TODO: This method is quite the most problematic part, since
	 * there is no list implementation in the datastore, unlike with 
	 * a <code>Map</code>.
	 * 
	 * @param parent
	 * @param jsonKey
	 * @param entity
	 * @return
	 */
	private EmbeddedEntity createEmbeddedEntityFromList(Key parent, String jsonKey, List entity){
		EmbeddedEntity ee = null;
		try {
			Preconditions.checkNotNull(parent, "Parent key cannot be null");
			Preconditions.checkNotNull(jsonKey, "JSON key cannot be null");
			Preconditions.checkNotNull(entity, "List entity cannot be null");
			int index = 0;
			ee = new EmbeddedEntity();
			ee.setKey(parent);
			for (Object o : entity){
				ee.setProperty(jsonKey + "." + index, o);
				index++;
			}
		} catch (Exception e) {

		}
		return ee;
	}
	
	/**
	 * Creates a <code>EmbeddedEntity</code> from a <code>Map</code>
	 * Which may include inner <code>EmbeddedEntity</code>.
	 * 
	 * @param parent
	 * @param jsonKey
	 * @param entity
	 * @return
	 */
	private EmbeddedEntity createEmbeddedEntityFromMap(Key parent,	Map<String,Object> entity){		
		
		EmbeddedEntity ee = null;
		
		Iterator<Map.Entry<String, Object>> it 
			= entity.entrySet().iterator();
		while (it.hasNext()){
			if (ee == null) {
				ee = new EmbeddedEntity();
				if (parent != null)
					ee.setKey(parent);
			}
			Map.Entry<String, Object> entry = it.next();
			String key = entry.getKey();
			Object value = entry.getValue();
			if (value == null){
				ee.setProperty(key, null);
			} else if (value instanceof String) {
				ee.setProperty(key, value);
			} else if(value instanceof Number) {
				ee.setProperty(key, value);
			} else if(value instanceof Boolean) {
				ee.setProperty(key, value);
			} else if(value instanceof List) {
				throw new IllegalArgumentException("List not yet supported");
			} else if(value instanceof Map){
				Map<String, Object> map = (Map<String, Object>) value;
				ee.setProperty(key, createEmbeddedEntityFromMap(ee.getKey(), map));
			}			
		}
		return ee;
	}
	
	/**
	 * Get a list of entities that matches the properties of a given <code>Entity</code>.
	 * This does not include the id
	 * 
	 * @param entity
	 * @param kind
	 * @return
	 */
	protected Iterator<Entity> getEntitiesLike(Entity entity, String kind){
		logger.info("Fetching entities like: " + entity);
		Map<String,Object> m = entity.getProperties();
		Iterator<String> it = m.keySet().iterator();
		Query q = new Query(kind);
		while (it.hasNext()){ // build the query
			String propName = it.next();
			Filter filter = new FilterPredicate(propName, 
				FilterOperator.EQUAL, m.get(propName));
			Filter prevFilter = q.getFilter();
			// Note that the Query object is immutable
			q = new Query(kind).setFilter(prevFilter).setFilter(filter); 
		}
		PreparedQuery pq = _ds.prepare(q);
		return pq.asIterator();
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
	
	protected Map<String,Object> getEntity(Key k){
		Map<String,Object> json = null;
		if (k == null)
			return null;		
		try {
			json = new LinkedHashMap<String, Object>();
			//Entity e = _ds.get(createKey(_collection, id));
			Entity e = _ds.get(k);
			Map<String,Object> props = e.getProperties();
			Iterator<Map.Entry<String, Object>> it = props.entrySet().iterator();
			// Preprocess - 
			// Can't putAll directly since List and Map
			// must be dynamically retrieved for 
			// those that are linked objects
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
					Map<String,Object> ee = getMapFromEmbeddedEntity((EmbeddedEntity) val);
					json.put(key, ee);
				} 
			}
			json.put(ID, e.getKey().getName());
		} catch (EntityNotFoundException e) {
			// Just return null
		} finally {
			
		}
		return json;
	}
	
	@SuppressWarnings("unchecked")
	protected DBObject createDBObjectFromEntity(Entity e){
		if (e == null)
			return null;
		Map<String,Object> map = createMapFromEntity(e);
		BasicDBObject obj = new BasicDBObject();
		obj.putAll(map);
		return obj;
	}
	
	protected Map<String,Object> createMapFromEntity(Entity e){
		Map<String,Object> map = null;
		if (e == null)
			return null;		
		try {
			map = new LinkedHashMap<String, Object>();
			Map<String,Object> props = e.getProperties();
			Iterator<Map.Entry<String, Object>> it = props.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<String, Object> entry = it.next();
				String key = entry.getKey();
				Object val = entry.getValue();
				if (val == null){
					map.put(key, val);
				} else if (val instanceof String
						|| val instanceof Number
						|| val instanceof Boolean) {
					map.put(key, val);
				} else if (val instanceof Text) {
					map.put(key, ((Text) val).getValue());
				} else if (val instanceof EmbeddedEntity) { // List and Map are stored as EmbeddedEntity internally
					// TODO Must identify if the EmbeddedEntity is a List or Map
					logger.log(Level.INFO, "Embedded entity found.");
					Map<String,Object> ee = getMapFromEmbeddedEntity((EmbeddedEntity) val);
					map.put(key, ee);
				} 
			}
			map.put(ID, e.getKey().getName());
		} catch (Exception ex) {
			// Just return null
		} finally {
			
		}
		return map;
	}	
	
	protected boolean containsEntityKey(Key key){
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
	
	
	protected boolean deleteEntity(Key key){
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
    
    
	protected Key createCollectionKey(String name) {
		return KeyFactory.createKey(_dbkey, COLLECTION_KIND, name);
	}
	
	public CommandResult okResult() {
		CommandResult result = new CommandResult();
		result.put("ok", true);
		return result;
	}
	
	
	
}
