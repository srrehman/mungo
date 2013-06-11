package com.mungoae;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.mungoae.collection.WriteConcern;
import com.mungoae.collection.WriteResult;
import com.mungoae.query.Update;
import com.mungoae.util.Tuple;

public abstract class DBCollection {
	
	public static final String MUNGO_DOCUMENT_ID_NAME = "_id";
	public static final String MUNGO_QUERY_OID = "$oid";
    protected static final Object[] NO_PARAMETERS = {};
    protected static final String ALL = "{}";
    
	private DB _db;
	protected final String _databaseName;
	protected final String _collectionName;
	
	protected Class _objectClass = null;
	
	public DBCollection(DB db, String collection) {
		_db = db;
		_databaseName = _db.getName();
		_collectionName = collection;
	}

	public abstract DBCursor find();
	public abstract DBCursor find(String query);
	public abstract DBCursor find(DBObject query);
	public abstract DBObject findOne();
	public abstract DBObject findOne(String query);
	public abstract DBObject findOne(Object id);
	public abstract DBObject findOne(ObjectId id);
	public abstract DBObject findOne(DBObject query);
	public abstract WriteResult insert(String doc);
	public abstract <T> WriteResult insert(T doc);
	public abstract <T> WriteResult insert(T... docs); 
	public abstract <T> WriteResult insert(List<T> docs); 
	public abstract Update update(String query);
	public abstract <T> T save(T doc);
	public abstract WriteResult remove(Object id);
	public abstract WriteResult remove(String query);
	
	public abstract Iterator<DBObject> __find(DBObject ref, DBObject fields, 
			int numToSkip , int batchSize , int limit, int options);

	protected abstract Iterator<DBObject> __find(
			Map<String, Tuple<FilterOperator, Object>> filters, 
			Map<String, com.google.appengine.api.datastore.Query.SortDirection> sorts,
			Integer numToSkip , Integer batchSize , Integer limit, Integer options);
	
	protected abstract <T> WriteResult __insert(List<T> list, boolean shouldApply , WriteConcern concern);
	
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
    
    public void setObjectClass(Class clazz) {
    	_objectClass = clazz;
    }
    
	public String getName(){
		return _collectionName;
	}
    
    public DB getDB() {
    	return _db;
    }
}
