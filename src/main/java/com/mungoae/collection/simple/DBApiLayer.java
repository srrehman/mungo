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
package com.mungoae.collection.simple;

import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.mungoae.CommandResult;
import com.mungoae.DB;
import com.mungoae.DBCollection;
import com.mungoae.DBObject;
import com.mungoae.Mungo;


/**
 * Database API layer
 * This cannot be directly instantiated, but the functions are available
 * through the instances for Mungo.
 * <br>
 * 
 * @author Kerby Martino <kerbymart@gmail.com>
 *
 */
public class DBApiLayer extends DB {

	private static Logger LOG = LogManager.getLogger(DBApiLayer.class.getName());

	private final Mungo _mungo;
	private DatastoreService _ds;
	
	public DBApiLayer(Mungo mungo, String namespace, DatastoreService connector) { 
		super(namespace);
		_mungo = mungo;
		_ds = connector;
	}

	@Override
	protected DBCollection doGetCollection(String collection) {
		DBCollection col = null; 
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(ADMIN_NAMESPACE); 
		try {
			Entity e = getCollectionEntity(collection);
			if (e == null) {
				col = createCollection(collection);
			} else {
				col = new BasicMungoCollection(this, collection);
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
	
	@Override
	public CommandResult command(DBObject cmd) { 
		return command(cmd, 0);
	}

	@Override
	public CommandResult command(DBObject cmd, int options) {
		LOG.debug("Mungo got command: " + cmd);
	    if (cmd.containsKey("getlasterror")) {
	    	return okResult();
	    } else if (cmd.containsKey("drop")) {
	    	return okResult();
	    } else if(cmd.containsKey("create")) {
	    	String collectionName = (String) cmd.get("create");
	        return okResult();
	    }		
	    CommandResult errorResult = new CommandResult();
	    errorResult.put("err", "undefined command: " + cmd);
	    return errorResult;
	}
	
}
