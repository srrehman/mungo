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
package com.mungoae;

import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.mungoae.object.Mapper;
import com.mungoae.serializer.XStreamGae;
import com.mungoae.util.JSON;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;

import org.bson.*;

/**
 * 
 * Basic implementation of DBObject
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 * 
 * @since 0.0.1
 * @version 0.0.1
 */
public class BasicDBObject extends BasicBSONObject implements DBObject, ParameterNames {
	
	private static final long serialVersionUID = 1L;
    private boolean _isPartialObject;
	
	private static Logger LOG = LogManager.getLogger(BasicDBObject.class.getName());
	
	public BasicDBObject() {
		super();
	}
	
	/**
	 * Construct by <code>Map</code>
	 * 
	 * @param map
	 */
	@SuppressWarnings("unchecked")
	public BasicDBObject(Map<String,Object> map){
		this();
		putAll(map);
	}
	/**
	 * Construct by key value pair
	 * 
	 * @param key
	 * @param value
	 */
	public BasicDBObject(String key, Object value){
		this();
		put(key, value);
	}
	
	/**
	 * Construct by JSON <code>String</code>
	 * 
	 * @param doc
	 */
	@SuppressWarnings("unchecked")
	public BasicDBObject(String doc){
		doc = doc.replaceAll("'", "\"");
		try {
//			Object obj = JSON.parse(doc);
			Object obj = JSON.parse(doc);
			if (obj != null){
				if (obj instanceof DBObject){
					putAll(((BasicDBObject)obj).toMap());
				}
			}
//			if (obj != null){
//				if (obj instanceof JSONObject){ 
//					// TODO - JSONObject is being stored
//					putAll((Map<String,Object>) obj);
//				} else if(obj instanceof JSONArray){
//					throw new RuntimeException("Contructing from JSON array not yet supported");
//				} else if(obj instanceof JSONValue){
//					throw new RuntimeException("Contructing from JSON value not yet supported");
//				}
//			} else {
//				throw new RuntimeException("Cannot parse document: " + doc);
//			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Construct by <code>Object</code>. 
	 * TODO - Add check for obj if its a primitive type
	 * TODO - Improve code depth
	 * @param obj
	 */
	public BasicDBObject (Object obj){
		Gson gson = new Gson();
		String json = gson.toJson(obj);
		Object jsonObject = JSONValue.parse(json);
		if (jsonObject instanceof JSONObject){
			putAll((Map) jsonObject);
		} else if(jsonObject instanceof JSONArray){
			throw new RuntimeException("Contructing from JSON array not yet supported");
//			for (Object o : (JSONArray) obj){
//				
//			}
		} else if(jsonObject instanceof JSONValue){
			throw new RuntimeException("Contructing from JSON value not yet supported");
		}
	}
	
	public BasicDBObject append(String key, Object value){
		put(key, value);
		return this;
	}
	
	public Object getId(){
		return get(DBCollection.MUNGO_DOCUMENT_ID_NAME); 
	}
		
	public Object copy() {
        // copy field values into new object
        BasicDBObject newobj = new BasicDBObject(this.toMap());
        // need to clone the sub obj
        for (String field : keySet()) {
            Object val = get(field);
            if (val instanceof BasicDBObject) {
                newobj.put(field, ((BasicDBObject)val).copy());
            } else if (val instanceof BasicDBList) {
                //newobj.put(field, ((BasicDBList)val).copy());
            }
        }
        return newobj;
    }	
	
	
//	public Map toMap() {
//		Map m = new HashMap<String,Object>();
//		Iterator<String> it = keySet().iterator();
//		while (it.hasNext()){
//			String key = it.next();
//			m.put(key, get(key));
//		}
//		return m;
//	}


	/**
	 * Fetch this DBObject as a type <code>T</code> specified.
	 * Class types should have a no-args constructor
	 * 
	 * @param clazz
	 * @return
	 */
	// For use as in:
	// Iterable<Friend> all = friends.find("{name: 'Joe'}").as(Friend.class);
	// Friend one = friends.findOne("{name: 'Joe'}").as(Friend.class);
	@Override
	public <T> T as(Class<T> clazz){
		try {
			T obj = (T) Mapper.createTObject(clazz, this.toMap());
			return obj;
		} catch (Exception e) {
			LOG.error("Exception in transform of DBObject as type=" + clazz.getName() + " : " + e.getMessage());
			throw new RuntimeException(e);
		}
	}
	
	private <T> T createTObject(Class<T> clazz){
		T obj = null;
		Gson gson = new Gson();
		try {
			obj = gson.fromJson(gson.toJson(this), clazz);
		} catch (JsonSyntaxException e) {
			LOG.error("Cannot create object because JSON string is malformed");
		} catch(Exception e) {
			LOG.error("Some other error occurred when trying to deserialize JSON string");
		}
		return obj;		
	}		
	
	private <T> T createTObjectWithXStream(Class<T> clazz){
		XStream xstream = new XStreamGae(new JettisonMappedXmlDriver());
		T obj = null;
//		try {
//			String json = this.toJSONString();
//			obj = (T) xstream.fromXML(json);
//		} catch (Exception e) {
//			LOG.log(Level.SEVERE, "XStraem cannot deserialize this object: " + e.getMessage());
//		}
		return obj;
	}
	
	private boolean isValidType(Object val){
		if (val instanceof ObjectId){
			return true;
		}
		if (val instanceof String
				|| val instanceof Number
				|| val instanceof Boolean
				|| val instanceof List
				|| val instanceof Map) {
			return true;
		}
		return false;
	}

	@Override
	public boolean isPartialObject() {
		return _isPartialObject;
	}

	@Override
	public void markAsPartialObject() {
		_isPartialObject = true;		
	}

}
