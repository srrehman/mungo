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
package com.mungoae;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.common.base.Preconditions;
import com.mungoae.collection.AbstractDBCollection;
import com.mungoae.collection.simple.BasicMungoCollection;
import com.mungoae.collection.simple.BasicMungoCollection;
import com.mungoae.object.KeyStructure;
import com.mungoae.serializer.ObjectSerializer;
import com.mungoae.serializer.XStreamSerializer;

/**
 * Datastore interface that provides namespacing. 
 * Usually this object should be retrieved from the singleton 
 * <code>Mungo</code> object and should not be instantiated directly 
 * or through <code>SimpleDB</code>.
 * 
 * @author Kerby Martino<kerbymart@gmail.com> 
 *
 */
public abstract class DB extends AbstractDBCollection implements ParameterNames {
	
	private static Logger LOG = LogManager.getLogger(DB.class.getName());

	protected final String _dbName;
	protected final ObjectSerializer serializer; 
		
	protected final Set<DBCollection> _seenCollections = new HashSet<DBCollection>();
	
	protected Key _dbkey;
	/**
	 * GAE datastore supported types.
	 */
	private static final Set<Class<?>> GAE_SUPPORTED_TYPES =
	        DataTypeUtils.getSupportedTypes();	

	public DB(String namespace){
		super(namespace);
		this._dbName = namespace;
		//this._dbkey = key;
		this.serializer = new XStreamSerializer();
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
				e.setProperty(DATABASE_NAME, this.getName());
				e.setProperty(CREATED, new Date().getTime());
				e.setProperty(UPDATED, new Date().getTime());
				col = new BasicMungoCollection(this, collection);
			} else {
				e = getCollectionEntity(collection);
				e.getProperty(DATABASE_NAME); // Sets where this collection belongs
				e.getProperty(COLLECTION_NAME);
				e.getProperty(CREATED);
				e.getProperty(UPDATED);
				col = new BasicMungoCollection(this, collection);
			}
			_ds.put(e);
		} catch (Exception e) {
			LOG.error("Error creating collection");
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
        DBCollection c = doGetCollection( collection );
        if ( c != null ){
            _seenCollections.add( c );
        }
        return c;
		
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
				cols.add(new BasicMungoCollection(this, (String) e.getProperty(COLLECTION_NAME)));
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
		//Preconditions.checkNotNull(_dbkey,"DB key cannot be null");
		try {
			Query q = new Query(COLLECTION_KIND)
				.setAncestor(KeyStructure.createKey(DATABASE_KIND, _dbName)); 
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

	//
	// internal stuff
	//
	protected void createCollectionEntity(String collection) {
		Transaction tx = _ds.beginTransaction();
		boolean success = true;
		try {
			Entity e = new Entity(createCollectionKey(collection));
			e.setProperty(DATABASE_NAME, _dbName);
			e.setProperty(CREATED, cal.getTime().getTime());
			// persist the entity
			_ds.put(e);			
		} catch (Exception e) {
			tx.rollback();
			success = false;
		} finally {
			if (tx.isActive())
				tx.rollback();
		}
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
	

	protected Key createCollectionKey(String name) {
		return KeyFactory.createKey(KeyStructure.createKey(DATABASE_KIND, _dbName), COLLECTION_KIND, name);
	} 
	
	public CommandResult okResult() {
		CommandResult result = new CommandResult();
		result.put("ok", true);
		return result;
	}
	
	public CommandResult errorResult(){
		CommandResult result = new CommandResult();
		result.put("err", "Error");
		return result;	
	}

	public DBCollection getCollectionFromString(String _ns) {
		throw new IllegalArgumentException("Not yet implemented");
	}	
	
	
	/** Returns the collection represented by the string <code>dbName</code>.<code>collectionName</code>.
	 * 
     * @param fullNameSpace the string
     * @return the collection
     */
    protected abstract DBCollection doGetCollection( String name );	
    
    
	public abstract CommandResult command(DBObject cmd);
	public abstract CommandResult command(DBObject cmd, int options);

}
