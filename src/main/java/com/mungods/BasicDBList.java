/**
 * 	
 * Copyright (c) 2013 Pagecrumb
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

import org.bson.types.*;

import com.google.gson.Gson;
import com.mungods.util.*;

/**
 * A basic implementation of bson list that is mungo specific 
 * 
 * @author Kerby Martino <kerbymart@gmail.com>
 */
public class BasicDBList extends BasicBSONList implements DBObject {

    private static final long serialVersionUID = -4415279469780082174L;
    private boolean _isPartialObject;

    /**
     * Returns a JSON serialization of this object
     * @return JSON serialization
     */    
    @Override
    public String toString(){
    	return new Gson().toJson(this); 
    }

    public boolean isPartialObject(){
        return _isPartialObject;
    }

    public void markAsPartialObject(){
        _isPartialObject = true;
    }

	@Override
	public <T> T as(Class<T> clazz) {
		// TODO Auto-generated method stub
		return null;
	}
    
}