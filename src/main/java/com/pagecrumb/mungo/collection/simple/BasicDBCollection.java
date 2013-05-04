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
package com.pagecrumb.mungo.collection.simple;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.types.ObjectId;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.Entity;
import com.pagecrumb.mungo.collection.CommandResult;
import com.pagecrumb.mungo.collection.DB;
import com.pagecrumb.mungo.collection.DBCollection;
import com.pagecrumb.mungo.collection.DBObject;
import com.pagecrumb.mungo.collection.WriteConcern;
import com.pagecrumb.mungo.collection.WriteResult;
import com.pagecrumb.mungo.common.MungoException;

public class BasicDBCollection extends DBCollection {
	
	private static final Logger LOG 
		= Logger.getLogger(DBCollection.class.getName());
	
	private final boolean nonIdCollection = false;
	
	public BasicDBCollection(DB ds, String collection) {
		super(ds, collection);
	}

	@Override
	public void doapply(DBObject o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public WriteResult insert(List<DBObject> toInsert, WriteConcern concern) {
	    for (DBObject obj : toInsert) {
	        LOG.log(Level.INFO,"insert: " + obj);
	        //filterLists(obj);
	        Object id = putIdIfNotPresent(obj);
	        if (getDB().containsKey(id, _collection)) {
	          if (enforceDuplicates(concern)) {
	            //throw new MungoException().DuplicateKey(0, "Attempting to insert duplicate _id: " + id);          
	          } else {
	            // TODO(jon) log          
	          }
	        } else {
	          putSizeCheck(id, obj);        
	        }
	      }
	    return new WriteResult(insertResult(toInsert.size()), concern);
	}
	
	public Object putIdIfNotPresent(DBObject obj) {
		if (obj.get(ID) == null) {
			ObjectId id = new ObjectId();
		    if (!nonIdCollection){
		    	obj.put(ID, id);
		    }
		    return id;
		} else {
			return obj.get(ID);
		}
	}
	
	public void putSizeCheck(Object id, DBObject obj) {
//		    if (objects.size() > 100000) {
//		      throw new FongoException("Whoa, hold up there.  Fongo's designed for lightweight testing.  100,000 items per collection max");
//		    }
//		    objects.put(id, obj);
		getDB().createObject(obj, _collection);
	}	
	
	boolean enforceDuplicates(WriteConcern concern) {
		//return !(WriteConcern.NONE.equals(concern) || WriteConcern.NORMAL.equals(concern));
		return !(WriteConcern.NONE.equals(concern));
	}	
	
	private CommandResult insertResult(int updateCount) {
		CommandResult result = getDB().okResult();
		result.put("n", updateCount);
		return result;
	}
		  
	private CommandResult updateResult(int updateCount, boolean updatedExisting) {
		CommandResult result = getDB().okResult();
		result.put("n", updateCount);
		result.put("updatedExisting", updatedExisting);
		return result;
	}		
	
	public DBObject filterLists(DBObject dbo){
//		if (dbo == null) {
//			return null;
//		}
//		for (String key : dbo.keySet()) {
//			Object value = dbo.get(key);
//		    Object replacementValue = replaceListAndMap(value);
//		    dbo.put(key, replacementValue);
//		}
//		return dbo;
		return null;
	}	
}
