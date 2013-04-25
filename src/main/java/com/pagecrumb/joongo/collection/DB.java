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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;


import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
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

	protected boolean deleteEntity(String kind, String key) {
		Entity e = getEntity(kind, key);
		if (e == null)
			return false;
		Transaction tx = _ds.beginTransaction();
		try {
			_ds.delete(e.getKey());
			tx.commit();
		} catch (Exception e2) {
			tx.rollback();
			return false;
		}
		return true;
	}	
	

	public Properties create(String collection, String id, Map json){
		// Check if collection exist
		if (getCollectionEntity(collection) == null)
			createCollectionEntity(collection);
		return createEntity(collection, id, json);
	}
	
	public boolean delete(String collection, String id)  {
		return deleteEntity(collection, id);
	}	
	
	public Properties get(String kind, String key)  {
		Entity e = getEntity(kind, key);
		if (e == null)
			return null;

		// extract the data from the datastore entity
		Properties props = new Properties();
		props.putAll(e.getProperties());
		props.put(COLLECTION_NAME, kind);

		return props;
	}	
	
	public Properties put(String kind, String key, Map data)
			 {
		return updateEntity(kind, key, data);
	}
	
	

	protected Entity getEntity(String kind, String key) {
		try {
			return _ds.get(createKey(kind, key));
		} catch (EntityNotFoundException ex) {
			return null;
		}
	}

	public Properties createMultiple(String kind, List<Map> json)
			 {
		if (getCollectionEntity(kind) == null)
			createCollectionEntity(kind);
		List<Entity> entities = new LinkedList<Entity>();
		// Add first in the list
		for (Map j : json){
			// TODO Check what is the change of collision
			Long uuid = UUID.randomUUID().getLeastSignificantBits();
			Entity e = new Entity(createKey(kind, String.valueOf(uuid)));
			//String rev = getRevision(j);
			long now = cal.getTime().getTime();
			
			Set keys = dataCleansing(j).keySet();
			for (Object jsonMapKey : keys){
				String entityKey = String.valueOf(jsonMapKey);
				Object entityValue = j.get(jsonMapKey);
				setProperty(e, entityKey, entityValue);
			} 
			// add some stuff
			e.setProperty(COLLECTION_NAME, kind);
			e.setProperty(ID, String.valueOf(uuid));
			e.setProperty(UPDATES, new Integer(0));
			//e.setProperty(REVISION, rev);
			e.setProperty(CREATED, now);
			e.setProperty(UPDATED, now);
			entities.add(e);
 		}
		List<String> ids = new ArrayList<String>();
		Transaction tx = _ds.beginTransaction(options);
		boolean success = false;
		try {
			for (Entity e : entities){
				_ds.put(e);
				ids.add((String)e.getProperty(ID));
			}
			tx.commit();
			success = true;
		} catch (Exception e2) {
			tx.rollback();
		}
		// prepare the response
		Properties props = new Properties();
		props.put(TOTAL_ROWS, new Integer(ids.size()));
		props.put(ROWS, ids);
		
		return success == true ? props : null;
	}
	
	/**
	 * 
	 * 
	 * @param kind
	 * @param key
	 * @param json
	 * @return 
	 */
	protected Properties createEntity(String kind, String key, Map json) {
		if (get(kind, key) != null)
			return null; // the entity already exists, busted !
		try {
			Entity e = new Entity(createKey(kind, key));
			//String rev = getRevision(json);
			long now = cal.getTime().getTime();

			Set keys = dataCleansing(json).keySet();
			for (Object jsonMapKey : keys){
				String entityKey = String.valueOf(jsonMapKey);
				Object entityValue = json.get(jsonMapKey);
				//logger.log(Level.INFO, "Setting entity property name:" + entityKey + " value: " + entityValue);
				setProperty(e, entityKey, entityValue);
			} 
			// add some stuff
			e.setProperty(ID, key);
			e.setProperty(UPDATES, new Integer(0));
			//e.setProperty(REVISION, rev);
			e.setProperty(CREATED, now);
			e.setProperty(UPDATED, now);

			// persist the entity
			//_ds.put(e);
			Transaction tx = _ds.beginTransaction();
			boolean success = false;
			try {
				_ds.put(e);
				tx.commit();
				success = true;
			} catch (Exception e2) {
				tx.rollback();
			}
			// prepare the response
			Properties props = new Properties();
			props.put(COLLECTION_NAME, kind);
			props.put(ID, key);
			//props.put(REVISION, rev);

			return success == true ? props : null;
			
		} catch (Exception e) {
			return null;
		}
	}

	protected Properties updateEntity(String kind, String key, Map data) {
		Entity e = getEntity(kind, key);
		if (e == null)
			return null; // the entity must already exists, busted !

		try {
			Long _updates = (Long) e.getProperty(UPDATES);
			int updates = _updates.intValue() + 1;
			//String rev = getRevision(data);

			// add the new or updated data to the entity
			Set keys = dataCleansing(data).keySet();
			for (Object jsonMapKey : keys){
				setProperty(e, String.valueOf(jsonMapKey), data.get(jsonMapKey));
			} 
			
			// add some admin stuff
			e.setProperty(UPDATES, new Integer(updates));
			//e.setProperty(REVISION, rev);
			e.setProperty(UPDATED, cal.getTime().getTime());

			// persist the entity
			//_ds.put(e);
			
			Transaction tx = _ds.beginTransaction();
			boolean success = true;
			
			try {
				_ds.put(e);
				tx.commit();
			} catch (Exception e2) {
				tx.rollback();
				success = false;
			}
			

			// prepare the response
			Properties props = new Properties();
			props.put(COLLECTION_NAME, kind);
			props.put(ID, key);
			//props.put(REVISION, "" + updates + "-" + rev);

			return success == true ? props : null;
		} catch (Exception ex) {
			return null;
		}
	}	
	
	protected Map dataCleansing(Map data) {

		// remove managed attributes...
		data.remove(ID);
		data.remove(COLLECTION_NAME);
		data.remove(REVISION);
		data.remove(UPDATES);

		return data;
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
	
	
}
