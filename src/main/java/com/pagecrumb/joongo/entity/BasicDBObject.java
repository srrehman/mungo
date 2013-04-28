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
package com.pagecrumb.joongo.entity;

import java.util.Map;

import org.bson.types.ObjectId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import com.pagecrumb.joongo.ParameterNames;
import com.pagecrumb.joongo.collection.DBObject;
/**
 * 
 * Basic implementation of DBObject
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 * @since 0.0.1
 * @version 0.0.1
 */
public class BasicDBObject extends DBObject implements ParameterNames {
	private static final long serialVersionUID = 1L;
	public BasicDBObject() {
		super();
		put(OBJECT_ID, new ObjectId());
	}
	public BasicDBObject(String key, Object value){
		this();
		put(key, value);
	}
	public BasicDBObject(String json){
		Object obj = JSONValue.parse(json);
		if (obj instanceof JSONObject){
			putAll((Map) obj);
		} else if(obj instanceof JSONArray){
			for (Object o : (JSONArray) obj){
				
			}
		} else if(obj instanceof JSONValue){

		}
	}
	public Object getId(){
		return get(OBJECT_ID); 
	}
}
