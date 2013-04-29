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
package com.pagecrumb.joongo.collection;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
/**
 * Datastore object, a Map-based JSON document
 * TODO: Not yet implemented
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 *
 */
public abstract class DBObject extends JSONObject {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG 
		= Logger.getLogger(DBObject.class.getName());	
	public <T> T as(Class<T> clazz){
		// For use as in:
		// Iterable<Friend> all = friends.find("{name: 'Joe'}").as(Friend.class);
		// Friend one = friends.findOne("{name: 'Joe'}").as(Friend.class);
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
