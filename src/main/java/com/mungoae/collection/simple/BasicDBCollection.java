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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.google.common.base.Preconditions;
import com.mungoae.CommandResult;
import com.mungoae.DB;
import com.mungoae.DBCollection;
import com.mungoae.DBObject;
import com.mungoae.collection.WriteConcern;
import com.mungoae.collection.WriteResult;
import com.mungoae.util.BoundedIterator;

@Deprecated
public class BasicDBCollection extends DBCollection {
	
	private static Logger LOG = LogManager.getLogger(BasicDBCollection.class.getName());

	private final boolean nonIdCollection = false;
	
	public BasicDBCollection(DB ds, String collection) {
		super(ds, collection);
	}

	@Override
	public void doapply(DBObject o) {
		// TODO Auto-generated method stub
		
	}

	
	/**
	 * Put ID if not present
	 * 
	 * @param obj
	 * @return
	 */
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
	
	/**
	 * Put the object. Check the size of the object before persisting.
	 * 
	 * @param obj
	 */
	public void putSizeCheck(DBObject obj) {
//		    if (objects.size() > 100000) {
//		      throw new FongoException("Whoa, hold up there.  Mungo's designed for lightweight testing.  100,000 items per collection max");
//		    }
//		    objects.put(id, obj);
		_store.persistObject(obj);	
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

	@Override
	public WriteResult insert(DBObject[] arr, WriteConcern concern) {
		Preconditions.checkNotNull(_collection, "Cannot insert when collection is null");
		List<DBObject> objects = Arrays.asList(arr);
		for (DBObject o : objects){
			LOG.debug("Creating object: " + o.get(ID) + " in collection: " + _collection);
			Object id = _store.persistObject(o);
		}
		return new WriteResult(getDB(), null, concern); // Is this correct?
	}	

	@Override
	public WriteResult insert(List<DBObject> toInsert, WriteConcern concern) {
	    for (DBObject obj : toInsert) {
	        LOG.debug("insert: " + obj);
	        //filterLists(obj);
	        Object id = putIdIfNotPresent(obj);
	        putSizeCheck(obj);
//	        if (_store.containsKey(id, _collection)) {
//	          if (enforceDuplicates(concern)) {
//	            //throw new MungoException().DuplicateKey(0, "Attempting to insert duplicate _id: " + id);          
//	          } else {
//	            // TODO(jon) log          
//	          }
//	        } else {
//	          putSizeCheck(obj);        
//	        }
	      }
	    return new WriteResult(insertResult(toInsert.size()), concern);
	}	
	
	/**
	 * Find DBObject(s) 
	 * 
	 * @param ref
	 * @param fields
	 * @param numToSkip
	 * @param batchSize
	 * @param limit
	 * @param options
	 * @return
	 */
	@Override
	public Iterator<DBObject> __find(DBObject ref, DBObject fields, 
			int numToSkip , int batchSize , int limit, int options){
		Preconditions.checkNotNull(ref, "Query object can't be null");
		
		LOG.info("Collection find with query object=" + ref);
		
		Object id = ref.get("_id");
		DBObject query = (DBObject) ref.get("$query");
		DBObject orderby = (DBObject) ref.get("$orderby");
		
		// Remove special fields
		dataCleansing(ref);
		
		Iterator<DBObject> it = null;
		if (query != null && query.toMap().size() != 0){
			LOG.info("Query object=" + query);
			if (orderby != null && orderby.toMap().size() != 0){
				LOG.info("Sort object=" + orderby);
				it = _store.getSortedObjectsLike(query, orderby);
			} else {
				it = _store.getObjectsLike(query);
			}		
		} else {
			if (orderby != null && orderby.toMap().size() != 0){
				LOG.info("Sort object=" + orderby);
				it = _store.getSortedObjects(orderby);
			} else { // query and orderby are both null
				if (id != null){
					// TODO
					DBObject obj = _store.getObject(ref);
				}
				it = _store.getObjects();
			}	
		}
		if (limit > 0 && numToSkip > 0){
			return new BoundedIterator<DBObject>(numToSkip, limit, it);	
		} else if (limit > 0 && numToSkip == 0){
			return new BoundedIterator<DBObject>(0, limit, it);	
		} else if (limit == 0 && numToSkip > 0){
			return new BoundedIterator<DBObject>(numToSkip, 0, it);	
		}
		return it;
	}
	
	
	private void dataCleansing(DBObject obj){
		obj.removeField("$query");
		obj.removeField("$orderby");
	}
}
