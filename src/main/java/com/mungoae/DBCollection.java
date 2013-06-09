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

import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.TransactionOptions;

import com.mungoae.collection.WriteConcern;
import com.mungoae.collection.WriteResult;
import com.mungoae.object.ObjectStore;

/**
 * Collections class for GAE stored JSON objects
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 *
 */
public abstract class DBCollection implements ParameterNames {
	
	private static Logger LOG = LogManager.getLogger(DBCollection.class.getName());

	protected final String _namespace;
	protected final String _collection;
	protected final DB _db;
	
	protected static DatastoreService _ds;
	protected static TransactionOptions options;
	protected Calendar cal;	
	
	protected Class _objectClass = null;
	private Map<String,Class> _internalClass = Collections.synchronizedMap( new HashMap<String,Class>() );
    private ReflectionDBObject.JavaWrapper _wrapper = null;
	
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
			LOG.info("Create a new DatastoreService instance");
		}
		cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));			
		_collection = collection;
		_namespace = db.getName();
		_db = db;
		_store = ObjectStore.get(_namespace, _collection);
	}
	
	protected DBObject _checkObject(DBObject o, boolean canBeNull, boolean query){
		if ( o == null ){
            if ( canBeNull )
                return null;
            throw new IllegalArgumentException( "can't be null" );
        }

        if ( o.isPartialObject() && ! query )
            throw new IllegalArgumentException( "can't save partial objects" );

        if ( ! query ){
            _checkKeys(o);
        }
        return o;
	}
	
	/**
     * Checks key strings for invalid characters.
     */
    private void _checkKeys( DBObject o ) {
//        if ( o instanceof LazyDBObject || o instanceof LazyDBList )
//            return;
        if ( o instanceof BasicDBObject || o instanceof BasicDBList )
            return;    	

        for ( String s : o.keySet() ){
            validateKey ( s );
            Object inner = o.get( s );
            if ( inner instanceof DBObject ) {
                _checkKeys( (DBObject)inner );
            } else if ( inner instanceof Map ) {
                _checkKeys( (Map<String, Object>)inner );
            }
        }
    }	
    
    /**
     * Checks key strings for invalid characters.
     */
    private void _checkKeys( Map<String, Object> o ) {
        for ( String s : o.keySet() ){
            validateKey ( s );
            Object inner = o.get( s );
            if ( inner instanceof DBObject ) {
                _checkKeys( (DBObject)inner );
            } else if ( inner instanceof Map ) {
                _checkKeys( (Map<String, Object>)inner );
            }
        }
    }
    
    /**
     * Check for invalid key names
     * @param s the string field/key to check
     * @exception IllegalArgumentException if the key is not valid.
     */
    private void validateKey(String s ) {
        if ( s.contains( "." ) )
            throw new IllegalArgumentException( "fields stored in the db can't have . in them. (Bad Key: '" + s + "')" );
        if ( s.startsWith( "$" ) )
            throw new IllegalArgumentException( "fields stored in the db can't start with '$' (Bad Key: '" + s + "')" );
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
	 * TODO - No test for this method yet
	 * 
	 * @return the DBCursor
	 */
	public DBCursor find() {
		return new DBCursor(this, null, null);
	}

	/**
	 * Queries for an object in this collection
	 * 
	 * @param ref
	 * @return the DBCursor
	 */
	public DBCursor find(DBObject ref){
		// TODO - Right now the underlying find implementation
		// builds a query based on all of the fields of 'ref'
		// which more likely is a '$all' query.
		// Need to add a way to deal with other query operators
		if (ref == null)
			return null;	
		return new DBCursor(this, ref, null); 
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
	
	public DBCursor find(DBObject query, DBObject fields, int numToSkip, int batchSize){
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
//		try {
//			DBCursor cur = find();
//			return cur.iterator().next();
//		} catch (NoSuchElementException e) {
//			throw new RuntimeException("Collection is empty");
//		}
		return findOne(new BasicDBObject());
	}
	
	/**
	 * Returns a single object from this collection matching this query
	 * 
	 * @param query
	 * @return
	 */
	public DBObject findOne(DBObject query){
		return findOne(query, null);
	}

	
	/**
	 * Returns a single object from this collection matching this query
	 * 
	 * @param o
	 * @param fields
	 * @return
	 */
	public DBObject findOne(DBObject id, DBObject fields){
		Iterator<DBObject> iterator = __find( new BasicDBObject("_id", id), fields, 0, -1, 0, getOptions());
        return (iterator.hasNext() ? iterator.next() : null);
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
		if (_store.containsId(id)){
			BasicDBObject obj = new BasicDBObject();
			obj.put(ID, id);
			return _store.getObject(obj);
		}
		return null;
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
		return 0;
		//throw new IllegalArgumentException("Not yet implemented");
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
		if (_store.deleteObject(o)){
			return new WriteResult(getDB().okResult(), WriteConcern.NONE);
		};
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
		return save(jo, WriteConcern.NONE);
	}
	
	/**
	 * Saves an object to this collection (does insert or update based on the object _id).
	 * 
	 * @param jo
	 * @param concern
	 * @return
	 */
	public WriteResult save(DBObject jo, WriteConcern concern){
		_checkObject( jo , false , false );

        Object id = jo.get( "_id" );

        if ( id == null || ( id instanceof ObjectId && ((ObjectId)id).isNew() ) ){
            if ( id != null && id instanceof ObjectId )
                ((ObjectId)id).notNew();
            if ( concern == null )
            	return insert( jo );
            else
            	return insert( jo, concern );
        }

        DBObject q = new BasicDBObject();
        q.put( "_id" , id );
        if ( concern == null )
        	return update( q , jo , true , false );
        else
        	return update( q , jo , true , false , concern );
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
    	if ( ! DBObject.class.isAssignableFrom( c ) )
            throw new IllegalArgumentException( c.getName() + " is not a DBObject" );
        _objectClass = c;
        if ( ReflectionDBObject.class.isAssignableFrom( c ) )
            _wrapper = ReflectionDBObject.getWrapper( c );
        else
            _wrapper = null;
    }	
    
    
    
	/**
	 * Gets the default class for object in the collection
	 * @return
	 */
	public Class getObjectClass() {
		return _objectClass;
	}   
	
	/**
     * sets the internal class
     * @param path
     * @param c
     */
    public void setInternalClass( String path , Class c ){
        _internalClass.put( path , c );
    }

    /**
     * gets the internal class
     * @param path
     * @return
     */
    protected Class getInternalClass( String path ){
        Class c = _internalClass.get( path );
        if ( c != null )
            return c;

        if ( _wrapper == null )
            return null;
        return _wrapper.getInternalClass( path );
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
	
	/**
	 * Find objects
	 * 
	 * @param ref reference object
	 * @param fields fields to include in the resulting document(s)
	 * @param numToSkip Not yet supported
	 * @param batchSize Not yet supported
	 * @param limit Not yet supported
	 * @param options Not yet supported
	 * @return
	 */
    public abstract Iterator<DBObject> __find( DBObject ref , DBObject fields , int numToSkip , int batchSize , int limit, int options);


}

