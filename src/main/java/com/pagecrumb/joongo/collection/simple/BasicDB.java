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
package com.pagecrumb.joongo.collection.simple;

import java.util.logging.Logger;

import com.google.appengine.api.datastore.Key;
import com.pagecrumb.joongo.Joongo;
import com.pagecrumb.joongo.collection.CommandResult;
import com.pagecrumb.joongo.collection.DB;
import com.pagecrumb.joongo.collection.DBCollection;
import com.pagecrumb.joongo.collection.DBObject;

public class BasicDB extends DB {

	private static final Logger LOG 
		= Logger.getLogger(BasicDB.class.getName());
	private final Joongo joongo;
	
	public BasicDB(Joongo joongo, Key key, String namespace) {
		super(key, namespace);
		this.joongo = joongo;
	}

	@Override
	public DBObject command(DBObject cmd) {
		LOG.info("Joongo got command: " + cmd);
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
