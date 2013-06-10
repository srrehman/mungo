package com.mungoae;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mungoae.query.UpdateQuery;

public abstract class MungoCollection {
	
	private DB _db;
	protected final String _databaseName;
	protected final String _collectionName;
	
	public MungoCollection(String database, String collection) {
		_databaseName = database;
		_collectionName = collection;
	}

	public abstract DBCursor find();
	public abstract DBCursor find(String query);
	public abstract DBObject findOne(Object id);
	public abstract void insert(String doc);
	public abstract <T> void insert(T doc);
	public abstract <T> void insert(T... docs); 
	public abstract <T> void insert(List<T> docs); 
	public abstract UpdateQuery update(String query);
	public abstract <T> T save(T doc);
	public abstract boolean remove(Object id);
	public abstract boolean remove(String query);
	
	public abstract Iterator<DBObject> __find(DBObject ref, DBObject fields, 
			int numToSkip , int batchSize , int limit, int options);
	
	public String getName(){
		return _collectionName;
	}
	
	public String getDatabaseName() {
		return _databaseName;
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
}
