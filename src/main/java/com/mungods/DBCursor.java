package com.mungods;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.mungods.common.MungoException;
import com.mungods.object.ObjectStore;
import com.mungods.util.BoundedIterator;

public class DBCursor implements Iterator<DBObject>,
	Iterable<DBObject> {
	
	private static final int DEFAULT_FETCH_OFFSET = 0;
	private static final int DEFAULT_FETCH_LIMIT = 5;
	
	private static final Logger LOG 
		= Logger.getLogger(DBCursor.class.getName());
	
	private DBCollection _collection;
	private DBObject _query;
	private DBObject _keysWanted;
	
    private Iterator<DBObject> _it = null;
    private DBObject _orderBy = null;
    
    private int _batchSize = 5;
    private int _skip = DEFAULT_FETCH_OFFSET;
    private int _limit = DEFAULT_FETCH_LIMIT;
    
//    public DBCursor(Iterator<DBObject> it){
//		Preconditions.checkNotNull(it, "DBObject iterator cannot be null");
//		this._it = it;
//	}
//    
//    public DBCursor(Iterator<DBObject> it, DBCollection coll){
//		Preconditions.checkNotNull(it, "DBObject iterator cannot be null");
//		this._it = it;
//		this._collection = coll;
//	}
    
    
    /**
     * Create a new DBCursor, a bounded iterator with a given offset and maximum
     *
     * @param skip  offset to start iteration at. Must be non negative
     * @param limit  maximum elements this iterator should return. Set to -1 for all
     * @param iterator  the underlying iterator
     * @throws  IllegalArgumentException  if offset is negative
     */
//    public DBCursor(Iterator<DBObject> it, long skip, long limit){
//		Preconditions.checkNotNull(it, "DBObject iterator cannot be null");
//        if (skip < 0) {
//            throw new IllegalArgumentException("Offset must not be negative");
//        }
//        this.offset = skip;
//        this.max = limit;
//		this._it = it; 
//	}
    
    public DBCursor(DBCollection coll, DBObject q, DBObject k){
    	_collection = coll;
    	_query = q == null ? new BasicDBObject() : q;
    	_keysWanted = k;
    }

	public Iterator<DBObject> iterator() {
		_check();
		return _it;
	}

	public boolean hasNext() {
		_check();
		return _it.hasNext();
	}

	public DBObject next() {
		_check();
		return _it.next();
	}

	public void remove() {
		_check();
		_it.remove();
	}
	
    /**
     * Sorts this cursor's elements.
     * This method must be called before getting any object from the cursor.
     * <br>
     * <br>
     * <code>
     * 	BasicDBObjectBuilder.start("points", 1).get()
     * </code>
     * <br>
     * <br>
     * Where 'points' is the field in the document and 1 = Ascending, -1 = Descending
     * 
     * @param orderBy the fields by which to sort
     * 
     * @return a cursor pointing to the first element of the sorted results
     */
    public DBCursor sort(DBObject orderBy){
        if (_it != null)
            throw new IllegalStateException( "Can't sort after executing query" );
        _orderBy = orderBy;
        return this;
    }	
    
    // ----  internal stuff ------

    private void _check()
        throws MungoException {
        if ( _it != null )
            return;
        
        LOG.info("Raw Query="+_query);
        LOG.info("Raw OrderBy="+_orderBy);
        
        QueryOpBuilder builder = new QueryOpBuilder()
        	.addQuery(_query)
        	.addOrderBy(_orderBy);
 
        LOG.info("Built Query="+builder.get().get("$query"));
        LOG.info("Built OrderBy="+builder.get().get("$orderby"));
        
        _it = _collection.__find(builder.get(), _keysWanted, _skip, _batchSize, _limit, 0);
        Preconditions.checkNotNull(_it, "Checked Iterator is null");
    }    
    
    public DBCursor limit(Integer limit){
    	this._limit = limit;
    	this._it = new BoundedIterator<DBObject>(this._skip, this._limit, this._it);
    	return this;
    }
    
    public DBCursor skip(Integer skip){
    	this._skip = skip;
//    	int i;
//    	for (i = 0; i < skip ; i++){
//    		this._it.next();
//    	}
    	this._it = new BoundedIterator<DBObject>(this._skip, this._limit, this._it);
    	return this;
    }

	public void hint(DBObject dbObject) {
		
	}	
	
	private List<DBObject> copyIterator(Iterator<DBObject> it){
		List<DBObject> copy = new ArrayList<DBObject>();
		while (it.hasNext()){
		    copy.add(it.next());
		}
		return copy;
	}
	
}
