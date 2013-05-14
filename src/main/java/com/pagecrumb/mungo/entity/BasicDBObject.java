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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.types.ObjectId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.pagecrumb.mungo.ParameterNames;
import com.pagecrumb.mungo.collection.DBObject;
/**
 * 
 * Basic implementation of DBObject
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 * 
 * @since 0.0.1
 * @version 0.0.1
 */
public class BasicDBObject extends JSONObject implements DBObject, ParameterNames {
	
	private static final long serialVersionUID = 1L;
	private boolean isPartial = false;
	private static final Logger LOG 
		= Logger.getLogger(BasicDBObject.class.getName());
	
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
	public boolean containsField(String s) {
		return super.containsKey(s);
	}
	
	public boolean containsKey(String s) {
		return false;
	}
	public Object get(String key) {
		return super.get(key);
	}
	public boolean isPartialObject() {
		return isPartial;
	}
	public void markAsPartialObject() {
		this.isPartial = true;
	}
	public Object put(String key, Object v) {
		return super.put(key, v);
	}
	public void putAll(DBObject o) {
		super.putAll(o.toMap());
	}
	public Object removeField(String key) {
		return super.remove(key);
	}
	public Map toMap() {
		Map m = new HashMap<String,Object>();
		Iterator<String> it = keySet().iterator();
		while (it.hasNext()){
			String key = it.next();
			m.put(key, get(key));
		}
		return m;
	}


	/**
	 * Fetch this DBObject as a type <code>T</code> specified.
	 * Class types should have a no-args constructor
	 * 
	 * @param clazz
	 * @return
	 */
	public <T> T as(Class<T> clazz){
		// For use as in:
		// Iterable<Friend> all = friends.find("{name: 'Joe'}").as(Friend.class);
		// Friend one = friends.findOne("{name: 'Joe'}").as(Friend.class);
		try {
			return createTObject(clazz);
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Exception in getting DBObject as type: " + clazz.getName());
		}
		return null;
	}
	
	private <T> T createTObject(Class<T> clazz){
		T obj = null;
		Gson gson = new Gson();
		try {
			obj = gson.fromJson(this.toJSONString(), clazz);
		} catch (JsonSyntaxException e) {
			LOG.log(Level.SEVERE, "Cannot create object because JSON string is malformed");
		}
		return obj;		
	}		
}
