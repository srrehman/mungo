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
import java.util.Set;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import com.pagecrumb.mungo.collection.DBObject;
/**
 * 
 * JSON implementation of DBObject
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 * @since 0.0.1
 * @version 0.0.1
 */
public class JsonDBObject implements DBObject {
	private static final long serialVersionUID = 1L;
	/**
	 * Create this class from a JSON string
	 * @param json
	 */
	public JsonDBObject(String json){
		Object obj = JSONValue.parse(json); 
		if (obj instanceof JSONObject){
			putAll((JSONObject) obj);
		}
	}
	public boolean containsField(String s) {
		// TODO Auto-generated method stub
		return false;
	}
	public boolean containsKey(String s) {
		// TODO Auto-generated method stub
		return false;
	}
	public Object get(String key) {
		// TODO Auto-generated method stub
		return null;
	}
	public boolean isPartialObject() {
		// TODO Auto-generated method stub
		return false;
	}
	public Set<String> keySet() {
		// TODO Auto-generated method stub
		return null;
	}
	public void markAsPartialObject() {
		// TODO Auto-generated method stub
		
	}
	public Object put(String key, Object v) {
		// TODO Auto-generated method stub
		return null;
	}
	public void putAll(DBObject o) {
		// TODO Auto-generated method stub
		
	}
	public void putAll(Map m) {
		// TODO Auto-generated method stub
		
	}
	public Object removeField(String key) {
		// TODO Auto-generated method stub
		return null;
	}
	public Map toMap() {
		// TODO Auto-generated method stub
		return null;
	}
	public <T> T as(Class<T> clazz) {
		// TODO Auto-generated method stub
		return null;
	}
}
