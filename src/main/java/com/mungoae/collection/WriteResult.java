package com.mungoae.collection;

import com.mungoae.CommandResult;
import com.mungoae.DB;
/**
 * This class lets you access the results of the previous write. 
 * if you have STRICT mode on, this just stores the result of that 
 * getLastError call if you don't, then this will actually do the getlasterror call. 
 * if another operation has been done on this connection in the interim, calls will fail
 *  
 * @author kerby
 *
 */
public class WriteResult {
	
	private DB _db;
	private WriteConcern _lastConcern;
	private CommandResult _lastErrorResult;
	private boolean _lazy;
	
    public WriteResult( CommandResult o , WriteConcern concern ){
        _lastErrorResult = o;
        _lastConcern = concern;
        _lazy = false;
        _db = null;
    }
    
	public WriteResult(DB db, CommandResult o, WriteConcern concern){
		_db = db;
		_lastConcern = concern;
	}
	
	public String getError() {
        Object foo = getField( "err" );
        if ( foo == null )
            return null;
        return foo.toString();
	}
	public Object getField(String name){
		return getLastError().get( name );
	}
	public WriteResult getLastConcern(){
		return null;
	}
	public int getN(){
		return 0;
	}
	public boolean isLazy(){
		return false;
	}
	
    /**
     * calls {@link WriteResult#getLastError(WriteConcern)} with concern=null
     * @return
     * @throws MongoException
     */
    public synchronized CommandResult getLastError(){
    	return getLastError(null);
    }
    
	@Override
	public String toString() {
		return "";
	}
	
	/**
	 * Returns the existing CommandResult if concern is null or less strict than the concern it was obtained with
	 * 
	 * @param concern
	 * @return
	 */
    public synchronized CommandResult getLastError(WriteConcern concern){
        if ( _lastErrorResult != null ) {
            // do we have a satisfying concern?
            if ( concern == null || ( _lastConcern != null && _lastConcern.getW() >= concern.getW() ) )
                return _lastErrorResult;
        }
        return _lastErrorResult;
    }	
}
