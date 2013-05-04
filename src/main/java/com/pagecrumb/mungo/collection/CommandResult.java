package com.pagecrumb.mungo.collection;

import com.pagecrumb.mungo.common.MungoException;
import com.pagecrumb.mungo.entity.BasicDBObject;

public class CommandResult extends BasicDBObject {
    private final DBObject _cmd;
    private static final long serialVersionUID = 1L;

    static class CommandFailure extends MungoException {
        private static final long serialVersionUID = 1L;

        /**
         * 
         * @param res the result
         * @param msg the message
         */
        public CommandFailure( CommandResult res , String msg ){
            super( DatastoreError.getCode( res ) , msg );
        }
    }
    
    public CommandResult() {
    	this(null);
	}
    
    public CommandResult(DBObject cmd) {
        _cmd = cmd;
    }
    
    /**
     * gets the "ok" field which is the result of the command
     * @return True if ok
     */
    public boolean ok(){
        Object o = get( "ok" );
        if ( o == null )
            throw new IllegalArgumentException( "'ok' should never be null..." );

        if ( o instanceof Boolean )
            return (Boolean) o;

        if ( o instanceof Number )
            return ((Number)o).intValue() == 1;

        throw new IllegalArgumentException( "can't figure out what to do with: " + o.getClass().getName() );
    }
    /**
     * gets the "errmsg" field which holds the error message
     * @return The error message or null
     */
    public String getErrorMessage(){
        Object foo = get( "errmsg" );
        if ( foo == null )
            return null;
        return foo.toString();
    }    
}
