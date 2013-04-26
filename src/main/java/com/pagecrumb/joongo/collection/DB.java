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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import com.pagecrumb.joongo.collection.simple.SimpleDBCollection;
import com.pagecrumb.joongo.entity.BasicDBObject;

/**
 * Datastore namespace'd interface. Usually this object should 
 * be retrieved from the singleton <code>Joongo</code> object and should 
 * not be instantiated directly or through <code>SimpleDB</code>.
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
				col = new SimpleDBCollection(this, collection);
			} else {
				e = getCollectionEntity(collection);
				e.getProperty(DATABASE_NAME); // Sets where this collection belongs
				e.getProperty(COLLECTION_NAME);
				e.getProperty(CREATED);
				e.getProperty(UPDATED);
				col = new SimpleDBCollection(this, collection);
			}
			_ds.put(e);
		} catch (Exception e) {
			// TODO: rollback
			e.printStackTrace();
		} finally {
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
				col = new SimpleDBCollection(this, collection);
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
				cols.add(new SimpleDBCollection(this, (String) e.getProperty(COLLECTION_NAME)));
			}			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
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
			ObjectId oid = (ObjectId) object.get(OBJECT_ID);
			if (oid == null){
				logger.info("No id object found in the object, creating new");
				_id = new ObjectId().toStringMongod();
			} else {
				logger.info("ObjectId found, getting string id");
				_id = oid.toStringMongod();
			}
			object.put(OBJECT_ID, _id);
			Key key = createEntity(null, collection, object);
			if (key != null)
				id = new ObjectId(key.getName());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
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
			ObjectId id = (ObjectId) object.get(OBJECT_ID);
			Map<String, Object> map = getEntity(createKey(collection, id.toStringMongod()));
			obj = new BasicDBObject();
			obj.putAll(map);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
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
			NamespaceManager.set(oldNamespace);
		}			
		return contains;
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
			ObjectId id = (ObjectId) object.get(OBJECT_ID);
			result = deleteEntity(createKey(collection, id.toStringMongod()));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
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
	 * @param parent when provided becomes the parent key of the created Entity, can be set to null
	 * @param obj
	 */
	protected Key createEntity(Key parent, String kind, Map obj){	
		Key entityKey = null;
		try {
			String id = (String) obj.get(OBJECT_ID);
			Entity e = new Entity(
					parent == null ? createKey(kind, id) : parent);  
			// Clean up the objectId (since the DS have ID field)
			// and since it is already 'copied' into the Entity
			obj.remove(OBJECT_ID);
			Iterator it = obj.keySet().iterator();
			while (it.hasNext()){
				String key = (String) it.next();
				if (obj.get(key) == null){
					e.setProperty(key, null);
				} else if (obj.get(key) instanceof String) {
					setProperty(e, key, obj.get(key));
				} else if(obj.get(key) instanceof Number) {
					setProperty(e, key, obj.get(key));
				} else if(obj.get(key) instanceof Boolean) {
					setProperty(e, key, obj.get(key));
				} else if(obj.get(key) instanceof List) {
					// Problem area, right way to store a list? 
					// List may contain JSONObject too!
					int index = 0;
					EmbeddedEntity ee = new EmbeddedEntity();
					ee.setKey(createKey(e.getKey(), kind, (String) obj.get(key)));
					for (Object o : (List) obj.get("key")){
						ee.setProperty((String)obj.get(key) + "." + index, o);
						index++;
					}
					e.setProperty((String)obj.get(key), ee);
				} else if(obj.get(key) instanceof Map){
					logger.log(Level.INFO, "Processing Map value");
					// FIXME Doing recursive call to this method
					// throws StackOverflow exception
//					Key pkey = createKey(e.getKey(), _kind, key);
//					e.setProperty(key, pkey);  
					createEntity(e.getKey(), kind, (Map) obj.get(key)); 
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
			// those are linked objects
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
				} else if (val instanceof List) {
					
				} else if (val instanceof Map) { // For embedded Map, the key is stored instead
					
				}
			}
			json.put(OBJECT_ID, e.getKey().getName());
		} catch (EntityNotFoundException e) {
			// Just return null
		} finally {
			
		}
		return json;
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
		}
		return true;
	}	

    protected void setProperty(Entity entity, String key, Object value){
	    if (!GAE_SUPPORTED_TYPES.contains(value.getClass())
        && !(value instanceof Blob)) {
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
