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
package com.mungods;

import java.util.Arrays;
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
import com.google.common.base.Preconditions;

import com.mungods.collection.WriteConcern;
import com.mungods.collection.WriteResult;
import com.mungods.object.GAEObject;
import com.mungods.object.ObjectStore;
/**
 * Collections class for GAE stored JSON objects
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 *
 */
public abstract class DBCollection implements ParameterNames {
	
	private static final Logger LOG 
		= Logger.getLogger(DBCollection.class.getName());
	
	protected final String _namespace;
	protected final String _collection;
	protected final DB _db;
	
	protected static DatastoreService _ds;
	protected static TransactionOptions options;
	protected Calendar cal;	
	protected Class _objectClass = null;
	
	protected ObjectStore _store;
	
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
		_collection = collection;
		_namespace = db.getName();
		_db = db;
		_store = ObjectStore.get(_namespace, _collection);
	}
	
	protected DBObject _checkObject(DBObject o, boolean canBeNull, boolean query){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public DB getDB() {
		return this._db;
	}
	
	public String getName(){
		return _collection;
	}
	

	
	protected boolean checkReadOnly(){
		return false;
	}
	
	/**
	 * 
	 * @return the number of documents in this collection
	 */
	public long count() {
		return _db.countCollections();
	}
	/**
	 * 
	 * @param query
	 * @return the number of documents that match a query
	 */
	public long count(DBObject query){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Calls {@link createIndex(DBObject keys, DBObject options)} 
	 * @param keys
	 */
	public void createIndex(DBObject keys){
		throw new IllegalArgumentException("Not yet implemented");
	}
	/**
	 * Forces creation of an index on a set of fields, 
	 * if one does not already exist
	 * 
	 * @param keys
	 * @param options
	 */
	public void createIndex(DBObject keys, DBObject options){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Find distinct values for a key
	 * 
	 * @param key
	 * @return
	 */
	public List distinct(String key){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Find distinct values for a key
	 * @param key
	 * @param query
	 * @return
	 */
	public List distinct(String key, DBObject query){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Adds any necessary fields to a given object before saving it to the 
	 * collection.
	 * 
	 * @param o
	 */
	public abstract void doapply(DBObject o);
	
	/**
	 * Drops(deletes) this collection
	 */
	public void drop() {
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Drops an index from this collection
	 * 
	 * @param keys
	 */
	public void dropIndex(DBObject keys){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Drops an index from this collecction
	 * 
	 * @param name
	 */
	public void dropIndex(String name){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Drops all indexes from this collection
	 */
	public void dropIndexes(){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public void ensureIndex(DBObject keys){
		throw new IllegalArgumentException("Not yet implemented");
	}	
	
	public void ensureIndex(DBObject keys, String name){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public void ensureIndex(DBObject keys, DBObject optionsIN){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public void ensureIndex(DBObject keys, String name, boolean unique){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public boolean equals() {
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Queries for all object in this collection
	 * 
	 * @return
	 */
	public DBCursor find() {
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Queries for an object in this collection
	 * 
	 * @param ref
	 * @return
	 */
	public DBCursor find(DBObject ref){
		// TODO - Right now the underlying find implementation
		// builds a query based on all of the fields of 'ref'
		// which more likely is a '$all' query.
		// Need to add a way to deal with other query operators
		if (ref == null)
			return null;
		
//		GAEObject xobj 
//			= new GAEObject(_db.getName(), _collection)
//				.setCommand(GAEObject.FIND)
//				.setQuery(ref)
//				.justOne(false)
//				.execute();
		
		Iterator<DBObject> it = ObjectStore.get(_namespace, _collection).getObjectsLike(ref);
		if (it.hasNext()){
			return new DBCursor(it);
		}
		return null;
//		return xobj.getResult();
	}
	
	/**
	 * Queries for an object in this collection
	 * 
	 * @param ref
	 * @param keys
	 * @return
	 */
	public DBCursor find(DBObject ref, DBObject keys){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public DBCursor find(DBObject quer, DBObject fields, int numToSkip, int batchSize){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public DBCursor find(DBObject quer, DBObject fields, 
			int numToSkip, int batchSize, int options){
		throw new IllegalArgumentException("Not yet implemented");
	}	
	
	public DBObject findAndModify(DBObject query, DBObject fields,
			DBObject sort, boolean remove, DBObject modifier,
			boolean returnNew, boolean upsert) {
		throw new IllegalArgumentException("Not yet implemented");	
	}	
	
	public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort,
			DBObject update, boolean returnNew, boolean upsert){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public DBObject findAndModify(DBObject query, DBObject sort, DBObject update){
		DBObject fields = null;
		boolean returnNew = false;
		boolean upsert = false;
		return findAndModify(query, fields, sort, update, returnNew, upsert);
	}	
	
	public DBObject findAndModify(DBObject query, DBObject update){
		DBObject fields = null;
		DBObject sort = null;
		boolean returnNew = false;
		boolean upsert = false;		
		return findAndModify(query, fields, sort, update, returnNew, upsert);
	}
	
	public DBObject findAndRemove(DBObject query){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Returns a single object from this collection
	 * 
	 * @return
	 */
	public DBObject findOne(){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Returns a single object from this collection matching this query
	 * 
	 * @param o
	 * @return
	 */
	public DBObject findOne(DBObject o){
		Iterator<DBObject> it = find(o).iterator();
		while (it.hasNext()){
			DBObject temp = it.next(); 
			// Is this the right approach?
			Object id = new ObjectId((String)temp.get("_id"));
			LOG.info("Finding object with ID="+id);
			return findOne(id);
		}
		return null;
	}
	
	/**
	 * Returns a single object from this collection matching this query
	 * 
	 * @param o
	 * @param fields
	 * @return
	 */
	public DBObject findOne(DBObject o, DBObject fields){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Returns a single object from this collection matching this query
	 * 
	 * @param o
	 * @param fields
	 * @return
	 */
	public DBObject findOne(DBObject o, DBObject fields, DBObject orderBy){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Finds an object by its id
	 * 
	 * @param o
	 * @return
	 */
	public DBObject findOne(Object id){
//		if (_store.containsKey(id, _collection)){
//			BasicDBObject obj = new BasicDBObject();
//			obj.put(ID, id);
//			return _store.getObject(obj, _collection);
//		}
		
		GAEObject xobj 
			= new GAEObject(_db.getName(), _collection)
				.setCommand(GAEObject.FIND)
				.setQuery(new BasicDBObject("_id", id))
				.justOne(true)
				.execute();
		
		return xobj.getSingleResult();
	}
	
	/**
	 * Finds an object by its id
	 * 
	 * @param o
	 * @return
	 */
	public DBObject findOne(Object o, DBObject fields){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Returns the full name of this collection with the database name as prefix
	 * @return
	 */
	public String getFullName() {
		return _namespace + "." + _collection;
	}
	
	
	/**
	 * Convenience method to generate an index name from the set of fields it is over.
	 * 
	 * @param keys
	 * @return
	 */
	public static String getIndexName(DBObject keys){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public long getCount(){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public long getCount(DBObject query, DBObject fields, long limit, long skip) {
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	public long getCount(DBObject query, DBObject fields){
		throw new IllegalArgumentException("Not yet implemented");
	}	
	
	public long getCount(DBObject query){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Returns a list of the indexes for this collection
	 * @return
	 */
	public List<DBObject> getIndexInfo() {
		throw new IllegalArgumentException("Not yet implemented");
	}

	
	public int getOptions() {
		throw new IllegalArgumentException("Not yet implemented");
	}
	
//	public int hashCode() {
//		throw new IllegalArgumentException("Not yet implemented");
//	}
	
	public WriteResult insert(DBObject... arr){
		return insert(arr, WriteConcern.NONE);
	}
	
//	public WriteResult insert(DBObject[] arr, WriteConcern concern){
//		Preconditions.checkNotNull(_collection, "Cannot insert when collection is null");
//		List<DBObject> objects = Arrays.asList(arr);
//		for (DBObject o : objects){
//			LOG.log(Level.INFO, "Creating object: " + o.get(ID) + " in collection: " + _collection);
//			Object id = getDB().createObject(o, _collection); 
//		}
//		return new WriteResult(getDB(), null, concern); // Is this correct?
//	}	 
	
	public abstract WriteResult insert(DBObject[] arr, WriteConcern concern);
	
	/**
	 * Inserts a document into the database
	 * 
	 * @param o
	 * @param concern
	 * @return
	 */
	public WriteResult insert(DBObject o, WriteConcern concern){
		throw new IllegalArgumentException("Not yet implemented");
	}		
	
	/**
	 * Saves document(s) to the database
	 * 
	 * @param o
	 * @param concern
	 * @return
	 */
	public WriteResult insert(List<DBObject> list){
		throw new IllegalArgumentException("Not yet implemented");
	}	
	
	/**
	 * Saves document(s) to the database
	 * 
	 * @param o
	 * @param concern
	 * @return
	 */
	public abstract WriteResult insert(List<DBObject> list, WriteConcern concern);

	public WriteResult insert(WriteConcern concern, DBObject... arr){
		throw new IllegalArgumentException("Not yet implemented");
	}		
	
	public boolean isCapped() {
		return false;
	}
	
	public WriteResult remove(DBObject o){
//		if (_store.deleteObject(o, _collection)){
//			return new WriteResult(getDB().okResult(), WriteConcern.NONE);
//		};
		
		GAEObject xobj 
			= new GAEObject(_db.getName(), _collection)
				.setCommand(GAEObject.REMOVE)
				.setDoc(o)
				.execute();
		
		if (xobj.getLastError().get("ok").equals(true)){
			return new WriteResult(getDB().okResult(), WriteConcern.NONE); 
		} 
		
		CommandResult result = new CommandResult();
		result.put("ok", false);	
		result.put("code", 1000); // TODO create a error list!
		return new WriteResult(result, WriteConcern.NONE);
	}
	
	/**
	 * Clears all indices that have not yet been applied to this collection.
	 * 
	 */
	public void resetIndexCache() {
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Resets the default query options
	 */
	public void resetOptions() {
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Calls save(DBObject, WriteConcern) with default WriteConcern
	 * @param jo
	 * @return
	 */
	public WriteResult save(DBObject jo){
		return insert(jo, WriteConcern.NONE);
	}
	
	/**
	 * Saves an object to this collection (does insert or update based on the object _id).
	 * 
	 * @param jo
	 * @param concern
	 * @return
	 */
	public WriteResult save(DBObject jo, WriteConcern concern){
		throw new IllegalArgumentException("Not yet implemented");
	}
	
	/**
	 * Performs an update operation.
	 * 
	 * @param q
	 * @param o
	 * @param upsert
	 * @param multi
	 * @param concern
	 * @return
	 */	
	public WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi){
		throw new IllegalArgumentException("Not yet implemented");	
	}
	
	/**
	 * Performs an update operation.
	 * 
	 * @param q
	 * @param o
	 * @param upsert
	 * @param multi
	 * @param concern
	 * @return
	 */
	public WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi, WriteConcern concern){
		throw new IllegalArgumentException("Not yet implemented");	
	}
	
	/**
	 * Calls (update(DBObject q, DBObject o, boolean upsert, boolean multi) with upsert=false
	 * and multi=true
	 * 
	 * @param q
	 * @param o
	 * @param upsert
	 * @param multi
	 * @param concern
	 * @return
	 */	
	public WriteResult updateMulti(DBObject q, DBObject o){
		return update(q, o, false, true);
	}	
    
	
	/** Set a default class for objects in this collection
	 * 
     * @param c the class
     * @throws IllegalArgumentException if <code>c</code> is not a DBObject
     */
    public void setObjectClass( Class c ){
    	if (!DBObject.class.isAssignableFrom(c)){
    		throw new IllegalArgumentException( c.getName() + " is not a DBObject" );
    	}
    	_objectClass = c;
    }	
    
	/**
	 * Gets the default class for object in the collection
	 * @return
	 */
	public Class getObjectClass() {
		return _objectClass;
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

//	public DBObject findOne(DBObject dbObject, DBObject fieldsAsDBObject,
//			ReadPreference readPreference) {
//		throw new IllegalArgumentException("Not yet implemented");	
//	}

}

