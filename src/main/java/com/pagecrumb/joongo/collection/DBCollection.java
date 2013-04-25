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

import java.util.Calendar;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
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
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.TransactionOptions;
import com.pagecrumb.joongo.ParameterNames;
import com.pagecrumb.joongo.entity.BasicDBObject;
/**
 * Collections class for GAE stored JSON objects
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 *
 */
public abstract class DBCollection implements ParameterNames {
	
	private static final Logger LOG 
		= Logger.getLogger(DBCollection.class.getName());
	
	private final String _namespace;
	private final String _collection;
	private final DB _store;
	
	protected static DatastoreService _ds;
	protected static TransactionOptions options;
	protected Calendar cal;
	/**
	 * GAE datastore supported types.
	 */
	private static final Set<Class<?>> GAE_SUPPORTED_TYPES =
	        DataTypeUtils.getSupportedTypes();	

	public DBCollection(final DB db, final String collection){
		if (_ds == null) {
			_ds = DatastoreServiceFactory.getDatastoreService();
			options = TransactionOptions.Builder.withXG(true);
			LOG.log(Level.INFO, "Create a new DatastoreService instance");
		}
		cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));			
		this._collection = collection;
		this._namespace = db.getName();
		this._store = db;
	}
	
	public String getName(){
		return _collection;
	}
	
	public String createObject(DBObject obj){
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_namespace);
		if (obj.get(OBJECT_ID) == null
				|| !(obj.get(OBJECT_ID) instanceof String)){ 
			obj.put(OBJECT_ID, new ObjectId().toStringMongod());
		}
		try {
			String id = (String) obj.get(OBJECT_ID);
			createEntity(null, obj);
			return id;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			NamespaceManager.set(oldNamespace);
		}
		return null; 
	}

	public DBObject getObject(String id){
		DBObject result = null;
		Map<String,Object> json;
		if (id == null)
			return null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_namespace);
		try {
			json = new LinkedHashMap<String, Object>();
			Entity e = _ds.get(createKey(_collection, id));
			Map<String,Object> props = e.getProperties();
			Iterator<Map.Entry<String, Object>> it = props.entrySet().iterator();
			result = new BasicDBObject();
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
			json.put("_id", e.getKey().getName());
			result.putAll(json);
		} catch (EntityNotFoundException e) {
			// Just return null
		} finally {
			NamespaceManager.set(oldNamespace);
		}
		return result;
	}
	
	public boolean updateObject(DBObject obj){
		return false;
	}
	
	public boolean deleteObject(String id){
		return false;
	}

	public List<String> getDocIds() {
		return null;
	}
	
	public long getDocCount() {
		return _store.countCollections();
	}

	/**
	 * Create an entity  
	 * 
	 * @param parent 
	 * @param obj
	 */
	protected void createEntity(Key parent, Map obj){	
		try {
			Entity e = new Entity(
					parent == null ? createKey(_collection, (String) obj.get(OBJECT_ID)) : parent);  
			// Clean up the objectId (since the DS have ID field)
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
					ee.setKey(createKey(e.getKey(), _collection, (String) obj.get(key)));
					for (Object o : (List) obj.get("key")){
						ee.setProperty((String)obj.get(key) + "." + index, o);
						index++;
					}
					e.setProperty((String)obj.get(key), ee);
				} else if(obj.get(key) instanceof Map){
					LOG.log(Level.INFO, "Processing Map value");
					// FIXME Doing recursive call to this method
					// throws StackOverflow exception
//					Key pkey = createKey(e.getKey(), _kind, key);
//					e.setProperty(key, pkey); 
//					createEntity(pkey, (Map) obj.get(key)); 
				}
			}	
			LOG.log(Level.INFO, "Persisting entity to the datastore");
			_ds.put(e);
		} catch (ConcurrentModificationException e){
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void createListEntity(Key parentList, Object obj){
		
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
	        entity.setProperty(key, new com.google.appengine.api.datastore.Blob(blob.getBytes()));
	    }  
    }
    
	/**
	 * Internal stuff
	 * 
	 * @param kind
	 * @param key
	 * @return
	 */
	protected Key createKey(String kind, String key) {
		return KeyFactory.createKey(kind, key);
	}

	protected Key createKey(Key parent, String kind, String key) {
		return KeyFactory.createKey(parent, kind, key);
	}
	
}
