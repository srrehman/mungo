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
package com.pagecrumb.mungo.entity;

import java.util.Map;

import org.bson.types.ObjectId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import com.pagecrumb.mungo.ParameterNames;
import com.pagecrumb.mungo.collection.DBObject;
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
		put(ID, new ObjectId()); // FIXME This is faulty, whenever used it doesn't seem to fit the ID that is stored in the datastore
	}
	@SuppressWarnings("unchecked")
	public BasicDBObject(Map<String,Object> json){
		this();
		putAll(json);
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
	public BasicDBObject append(String key, Object value){
		put(key, value);
		return this;
	}
	public Object getId(){
		return get(ID); 
	}
}
