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
package com.pagecrumb.joongo;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
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
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.appengine.api.utils.SystemProperty.Environment;
import com.google.apphosting.api.ApiProxy;
import com.google.inject.Singleton;
import com.pagecrumb.joongo.collection.DB;
import com.pagecrumb.joongo.collection.simple.SimpleDB;
/**
 * 
 * The "Joongo" class, pronounced as 'jungo.' Provides the
 * interface to do <code>DB</code> operations.
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 * @since 0.0.1
 * @version 0.0.1
 */
@Singleton
public class Joongo implements ParameterNames {
	private static final Logger logger 
		= Logger.getLogger(Joongo.class.getName());
	
    protected AppIdentityService _appIdentity; protected static DatastoreService _ds;
	protected static TransactionOptions options;
	protected Calendar cal;
	/**
	 * GAE datastore supported types.
	 */
	private static final Set<Class<?>> GAE_SUPPORTED_TYPES =
	        DataTypeUtils.getSupportedTypes();	
	
	/**
	 * Creates a new object that access 'local' datastore
	 */
	public Joongo() {
		if (_appIdentity == null){
			_appIdentity = AppIdentityServiceFactory.getAppIdentityService();
		}
		if (_ds == null) {
			_ds = DatastoreServiceFactory.getDatastoreService();
			options = TransactionOptions.Builder.withXG(true);
			logger.log(Level.INFO, "Create a new DatastoreService instance");
		}
		cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));				
	}
	
	/**
	 * Creates new instance to access remote datastore through
	 * Joongo-style rest API
	 * <code>
	 * 		Joongo joongo = new Joongo("app-id.appspot.com/api");
	 * </code> 
	 * @param serverName
	 */
	public Joongo(String serverName){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public DB createDB(String dbName) {
		DB db = null;
		if (dbName.equalsIgnoreCase(ADMIN_NAMESPACE))
			return null; // cannot do this
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE);	
		try {
			Key key = createDatabaseKey(dbName);
			Entity e = new Entity(key);
			e.setProperty(DATABASE_NAME, dbName); // this will be the 'namespace' of its collections
			e.setProperty(CREATED, new Date().getTime());
			e.setProperty(UPDATED, new Date().getTime());
			_ds.put(e);
			db = new SimpleDB(this, e.getKey(), dbName);
		} catch (Exception e) {
			// TODO: Rollback
		} finally {
			NamespaceManager.set(oldNamespace);
		}	
		return db;		
	}
	
	public DB getDB(String dbName){
		DB db = null;
		if (dbName.equalsIgnoreCase(ADMIN_NAMESPACE))
			return null; // cannot do this
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE);	
		Key key = createDatabaseKey(dbName);
		Transaction tx = _ds.beginTransaction();
		try {
			Entity e = _ds.get(key);
			e.getProperty(DATABASE_NAME);
			e.getProperty(CREATED);
			e.getProperty(UPDATED);			
			db = new SimpleDB(this, e.getKey(), dbName);
			tx.commit();
		} catch(EntityNotFoundException ex) {
			Entity e = new Entity(key);
			e.setProperty(DATABASE_NAME, dbName);
			e.setProperty(CREATED, new Date().getTime());
			e.setProperty(UPDATED, new Date().getTime());			
			_ds.put(e);
			db = new SimpleDB(this, e.getKey(), dbName);
			tx.commit();
		} catch (Exception e) {
			// TODO: Rollback
			e.printStackTrace();
			tx.rollback();
		} finally {
			NamespaceManager.set(oldNamespace);
		}	
		return db;
	}
	public Collection<DB> getUsedDatabases(){
		List<DB> cols = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE);
		try {
			cols = new ArrayList<DB>();
			Query q = new Query(DATABASE_KIND);
			PreparedQuery pq = _ds.prepare(q);
			List<Entity> result = pq.asList(FetchOptions.Builder.withDefaults());
			for (Entity e : result) {
				cols.add(new SimpleDB(this, e.getKey(), (String) e.getProperty(DATABASE_NAME)));
			}			
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			NamespaceManager.set(oldNamespace);
		}	
		return cols;
	}
	public List<String> getDatabaseNames(){
		List<String> cols = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE);
		try {
			cols = new ArrayList<String>();
			Query q = new Query(DATABASE_KIND);
			PreparedQuery pq = _ds.prepare(q);
			List<Entity> result = pq.asList(FetchOptions.Builder.withDefaults());
			for (Entity e : result) {
				cols.add((String) e.getProperty(DATABASE_NAME));
			}			
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			NamespaceManager.set(oldNamespace);
		}	
		return cols;
	}
	public void dropDatabase(String dbName){
		if (dbName.equalsIgnoreCase(ADMIN_NAMESPACE))
			return; // cannot do this
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE);	
		try {
			Key key = createDatabaseKey(dbName);
			_ds.delete(key);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			NamespaceManager.set(oldNamespace);
		}			
	}
	@Override
	public String toString(){
		return "Joongo";
	}
	
	protected Key createKey(String kind, String key) {
		return KeyFactory.createKey(kind, key);
	}

	protected Key createDatabaseKey(String name) {
		return KeyFactory.createKey(DATABASE_KIND, name);
	}

	protected Key createKey(Key parent, String kind, String key) {
		return KeyFactory.createKey(parent, kind, key);
	}	
	
	public String getServerIP() {
		return "";
	}
}
