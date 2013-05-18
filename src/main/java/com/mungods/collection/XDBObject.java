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
package com.mungods.collection;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.types.ObjectId;
import org.json.simple.JSONObject;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.mungods.DBObject;
/**
 * Datastore object, a Map-based JSON document.
 * For type safety, you can only put <code>String</code>
 * keys and a set of value types:
 * <code>ObjectId</code>,
 * <code>String</code>,
 * <code>Number</code>,
 * <code>Boolean</code>,
 * <code>List</code>,
 * <code>Map</code> and its subclasses.
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 *
 */
@Deprecated
public abstract class XDBObject extends JSONObject {
	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG 
		= Logger.getLogger(DBObject.class.getName());	
	
	public XDBObject(){}
	
	@Override
	public Object put(Object key, Object value) {	
		if (key instanceof String) {
			LOG.info("Put entry with key="+key+" value=" + value);
			if (value == null){
				return super.put(key, null);
			}
			if (value instanceof ObjectId
					|| value instanceof String
					|| value instanceof Number
					|| value instanceof Boolean
					|| value instanceof List
					|| value instanceof Map) {
				return super.put(key, value);
			} else {
				throw new RuntimeException("Unsupported JSON value type: " + value.getClass().getName());
			}
		}  else {
			throw new RuntimeException("Unsupported JSON key type: " + value.getClass().getName());
		}
	}
	

}
