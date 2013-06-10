package com.mungoae;

import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.TransactionOptions;
import com.mungoae.query.Result;
import com.mungoae.query.DBQuery;
import com.mungoae.query.UpdateQuery;
/**
 * 
 * @author Kerby Martino <kerbymart@gmail.com>
 *
 */
public abstract class MungoCollection implements ParameterNames {
		
	protected static DatastoreService _ds;
	protected static TransactionOptions options;
	
	private DB _db;
	protected final String _databaseName;
	protected final String _collectionName;
	protected Calendar cal;	
	
	public MungoCollection(String database, String collection) {
		_databaseName = database;
		_collectionName = collection;
	}

	// TODO - Renmae DBQuery to DBCursor
	public abstract DBQuery find();
	public abstract DBQuery find(String query);
	public abstract DBObject findOne(Object id);
	public abstract void insert(String doc);
	public abstract <T> void insert(T doc);
	public abstract <T> void insert(T... docs); 
	public abstract <T> void insert(List<T> docs); 
	public abstract UpdateQuery update(String query);
	public abstract <T> T save(T doc);
	public abstract boolean remove(Object id);
	public abstract boolean remove(String query);
	
	//public abstract DBQuery __find();
	
	public String getName(){
		return _collectionName;
	}
	
	public String getDatabaseName() {
		return _databaseName;
	}
	
	public DB getDB(){
		return _db;
	}
	
    /**
     * Check for invalid key names
     * @param s the string field/key to check
     * @exception IllegalArgumentException if the key is not valid.
     */
    private void validateKey(String s) {
        if ( s.contains( "." ) )
            throw new IllegalArgumentException( "fields stored in the db can't have . in them. (Bad Key: '" + s + "')" );
        if ( s.startsWith( "$" ) )
            throw new IllegalArgumentException( "fields stored in the db can't start with '$' (Bad Key: '" + s + "')" );
    }  	
	protected DBObject _checkObject(DBObject o, boolean canBeNull, boolean query){
		if ( o == null ){
            if ( canBeNull )
                return null;
            throw new IllegalArgumentException( "can't be null" );
        }
        if ( ! query ){
            _checkKeys(o);
        }
        return o;
	}
	
	/**
     * Checks key strings for invalid characters.
     */
    private void _checkKeys( DBObject o ) {
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
	 * Find objects
	 * 
	 * @param ref reference object
	 * @param fields fields to include in the result
	 * @param numToSkip 
	 * @param batchSize 
	 * @param limit 
	 * @param options
	 * @return
	 */
    public abstract Iterator<DBObject> __find(DBObject ref , DBObject fields , int numToSkip , int batchSize , int limit, int options);
}
