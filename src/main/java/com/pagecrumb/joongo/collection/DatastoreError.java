package com.pagecrumb.joongo.collection;

import org.json.simple.JSONObject;

public class DatastoreError {
	DatastoreError( DBObject o ){
        _err = getMsg( o , null );
        if ( _err == null )
            throw new IllegalArgumentException( "need to have $err" );
        _code = getCode( o );
    }
    
    static String getMsg( JSONObject o , String def ){
        Object e = o.get( "$err" );
        if ( e == null )
            e = o.get( "err" );
        if ( e == null )
            e = o.get( "errmsg" );
        if ( e == null )
            return def;
        return e.toString();
    }

    static int getCode( JSONObject o ){
        Object c = o.get( "code" );
        if ( c == null )
            c = o.get( "$code" );
        if ( c == null )
            c = o.get( "assertionCode" );
        
        if ( c == null )
            return -5;
        
        return ((Number)c).intValue();
    }
    
    /**
     * Gets the error String
     * @return
     */
    public String getError(){
        return _err;
    }

    /**
     * Gets the error code
     * @return
     */
    public int getCode(){
        return _code;
    }

    @Override
    public String toString(){
        if ( _code > 0 )
            return _code + " " + _err;
        return _err;
    }    

    final String _err;
    final int _code;

}
