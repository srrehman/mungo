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

public class DatastoreError {
	DatastoreError( DBObject o ){
        _err = getMsg( o , null );
        if ( _err == null )
            throw new IllegalArgumentException( "need to have $err" );
        _code = getCode( o );
    }
    
    static String getMsg( DBObject o , String def ){
        Object e = o.get( "$err" );
        if ( e == null )
            e = o.get( "err" );
        if ( e == null )
            e = o.get( "errmsg" );
        if ( e == null )
            return def;
        return e.toString();
    }

    static int getCode( DBObject o ){
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
