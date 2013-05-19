package com.mungods;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.mungods.common.MungoException;

public class DBCursor implements Iterator<DBObject>,
	Iterable<DBObject>, Closeable {
	
	private static final Logger LOG 
		= Logger.getLogger(DBCursor.class.getName());
	
    private Iterator<DBObject> _it = null;
    private DBObject _orderBy = null;
    
    public DBCursor(Iterator<DBObject> it){
		Preconditions.checkNotNull(it, "DBObject iterator cannot be null");
		this._it = it;
	}

//	public DBCursor(DBCollection collection, DBObject dbObject,
//			DBObject fieldsAsDBObject, ReadPreference readPreference) {
//		throw new IllegalArgumentException("Not yet implemented");
//	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	public Iterator<DBObject> iterator() {
		return _it;
	}

	public boolean hasNext() {
		return _it.hasNext();
	}

	public DBObject next() {
		return _it.next();
	}

	public void remove() {
		_it.remove();
	}
	
    /**
     * Sorts this cursor's elements.
     * This method must be called before getting any object from the cursor.
     * @param orderBy the fields by which to sort
     * @return a cursor pointing to the first element of the sorted results
     */
    public DBCursor sort( DBObject orderBy ){
        if ( _it != null )
            throw new IllegalStateException( "can't sort after executing query" );
        _orderBy = orderBy;
        return this;
    }	
    
    // ----  internal stuff ------

    private void _check()
        throws MungoException {
        if ( _it != null )
            return;
    }    
    
    public void limit(Integer limit){
    	
    }
    
    public void skip(Integer skip){
    	
    }

	public void hint(DBObject dbObject) {
		
	}
}
