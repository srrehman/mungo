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
package com.mungoae.object;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.users.User;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.mungoae.BasicDBList;
import com.mungoae.BasicDBObject;
import com.mungoae.DBCollection;
import com.mungoae.DBObject;
import com.mungoae.common.SerializationException;
import com.mungoae.operators.OpDecode;
import com.mungoae.operators.Operator;
import com.mungoae.query.Logical;
import com.mungoae.query.Update;
import com.mungoae.query.Update.UpdateOperator;
import com.mungoae.serializer.ObjectSerializer;
import com.mungoae.serializer.XStreamSerializer;
import com.mungoae.util.JSON;
import com.mungoae.util.Tuple;
/**
 * Mapper class to construct DBObject from GAE Entities and vice versa
 * 
 * @author kerby
 *
 */
public class Mapper {
	
	private static Logger LOG = LogManager.getLogger(Mapper.class.getName());
	
	public static final String GAE_ENTITY_ID_NAME = "id";
	/**
	 * Process <code>EmbeddedEntity</code> and inner <code>EmbeddedEntity</code>
	 * of this entity. 
	 * 
	 * @param ee
	 * @return
	 */
	public static Map<String,Object> getMapFromEmbeddedEntity(final EmbeddedEntity ee){
		Map<String,Object> map = null;
		try {
			map = new HashMap<String, Object>();
			map.putAll(ee.getProperties());
			
			Map<String,Object> newMap = new HashMap<String, Object>(); 
			Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<String, Object> entry = it.next();
				if (entry.getValue() instanceof EmbeddedEntity){
					LOG.debug( "Inner embedded entity found with key=" + entry.getKey());
//					newMap.put(entry.getKey(), getMapFromEmbeddedEntity( (EmbeddedEntity) entry.getValue()));
					newMap.put(entry.getKey(), getMapOrList( (EmbeddedEntity) entry.getValue()));
					it.remove();
				}
			}
			map.putAll(newMap);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Error when processing EmbeddedEntity to Map");
		}
		return map;
	}
	
	/**
	 * Get the <code>List</code> out of the Embedded entity. 
	 * The <code>List</code> is expected to be stored following a dot (.) notation.
	 * E.g. A JSON array with a key of "numbers" will be stored as a <code>EmbeddedEntity</code>
	 * with property names:
	 * 
	 * <code>
	 * numbers.0
	 * numbers.1
	 * numbers.2
	 * </code>
	 * 
	 * And so on. And since it is stored a a  <code>EmbeddedEntity</code> then it is ambiguous to a
	 * <code>Map</code> that is also stored in the same Datastore type. 
	 * 
	 * @param ee
	 * @return
	 */
	private static List<Object> getListFromEmbeddedEntity(final EmbeddedEntity ee){
		List<Object> list = null;
		Iterator<Map.Entry<String, Object>> it = ee.getProperties().entrySet().iterator();
		Object[] arr = new Object[1024];
		List<Integer> indexToRemove = new ArrayList<Integer>();
		for (int i=0;i<arr.length;i++){
			indexToRemove.add(i);
		}
		while (it.hasNext()){
			Map.Entry<String, Object> entry = it.next();
			try {
				if (list == null){
					list = new LinkedList<Object>(); 
				}
				Object value = entry.getValue();
				Integer i = Integer.valueOf(entry.getKey());
				LOG.debug("Value="+entry.getValue());
				if (value instanceof String
						|| value instanceof Boolean
						|| value instanceof Number
						|| value instanceof Date
						|| value instanceof User) // GAE supported type
				{
					arr[i] = value;
					indexToRemove.remove(i);
				} else if (value instanceof EmbeddedEntity){
					arr[i] = getMapOrList((EmbeddedEntity)value);
					indexToRemove.remove(i);
				} else {
					throw new RuntimeException("Invalid JSON field type in embedded list entity");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		int[] intArray = ArrayUtils.toPrimitive(indexToRemove.toArray(new Integer[indexToRemove.size()]));
		arr = copyArrayRemove(arr, intArray);
		return Arrays.asList(arr);  
	}
	
	private static Object[] copyArrayRemove(Object[] objects, int[] elemToRemove){
		LOG.debug("Removing elements from array="+elemToRemove);
		Object[] nobjs = Arrays.copyOf(objects, objects.length - elemToRemove.length);
		for (int i = 0, j = 0, k = 0; i < objects.length; i ++) {
		    if (j < elemToRemove.length && i == elemToRemove[j]) {
		        j ++;
		    } else {
		        nobjs[k ++] = objects[i];
		    }
		}	
		return nobjs;
	}
	
	/**
	 * Create <code>EmbeddedEntity</code> from List
	 * 
	 * TODO: This method is quite the most problematic part, since
	 * there is no list implementation in the datastore, unlike with 
	 * a <code>Map</code>.
	 * 
	 * @param parent
	 * @param jsonKey
	 * @param entity
	 * @return
	 */
	public static EmbeddedEntity createEmbeddedEntityFromList(Key parent, List entity){
		EmbeddedEntity ee = null;
		try {
			Preconditions.checkNotNull(entity, "List entity cannot be null");
			int index = 0;
			ee = new EmbeddedEntity();
			if (parent != null)
				ee.setKey(parent);
			for (Object o : entity){
				if (o instanceof String
						|| o instanceof Boolean
						|| o instanceof Number
						|| o instanceof Date
						|| o instanceof User){
					ee.setProperty(String.valueOf(index), o); 
				} else if (o instanceof List){
					ee.setProperty(String.valueOf(index), 
							createEmbeddedEntityFromList(null, (List)o));
				} else if (o instanceof Map){
					ee.setProperty(String.valueOf(index), 
							createEmbeddedEntityFromMap(null, (Map)o));					
				}
				if (o == null){
					ee.setProperty(String.valueOf(index), null);
				}
				index++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ee;
	}
	
	/**
	 * Creates a <code>EmbeddedEntity</code> from a <code>Map</code>
	 * Which may include inner <code>EmbeddedEntity</code>.
	 * 
	 * @param parent
	 * @param jsonKey
	 * @param entity
	 * @return
	 */
	public static EmbeddedEntity createEmbeddedEntityFromMap(Key parent, Map<String,Object> entity){		
		
		Preconditions.checkNotNull(entity, "Map entity cannot be null");
		
		// Deal with empty map
		if (entity.size() == 0){
			EmbeddedEntity ee = new EmbeddedEntity();
			if (parent != null)
				ee.setKey(parent);
			return ee;
		}

		EmbeddedEntity ee = null;
		
		Iterator<Map.Entry<String, Object>> it 
			= entity.entrySet().iterator();
		while (it.hasNext()){
			if (ee == null) {
				ee = new EmbeddedEntity();
				if (parent != null)
					ee.setKey(parent);
			}
			Map.Entry<String, Object> entry = it.next();
			String key = entry.getKey();
			Object value = entry.getValue();
			if (value == null){
				ee.setProperty(key, null);
			} else if (value instanceof String) {
				ee.setProperty(key, value);
			} else if(value instanceof Number) {
				ee.setProperty(key, value);
			} else if(value instanceof Boolean) {
				ee.setProperty(key, value);
			} else if(value instanceof Date) {
				ee.setProperty(key, value);
			} else if(value instanceof User) {
				ee.setProperty(key, value);
			} else if(value instanceof List) {
				ee.setProperty(key, createEmbeddedEntityFromList(ee.getKey(), (List)value));
			} else if(value instanceof Map){
				Map<String, Object> map = (Map<String, Object>) value;
				ee.setProperty(key, createEmbeddedEntityFromMap(ee.getKey(), map));
			}			
		}
		LOG.debug("Warning method is returning null value");
		return ee;
	}
	
	public static Entity createEntityFromDBObject(DBObject object, String kind){
		Preconditions.checkNotNull(object, "DBObject cannot be null");
		Preconditions.checkNotNull(kind, "Entity kind cannot be null");
		Entity e = null;
		// Pre-process object id
		if (object.get(DBCollection.MUNGO_DOCUMENT_ID_NAME) != null){
			LOG.debug("Constructing Entity from DBObject with id="+object.get(DBCollection.MUNGO_DOCUMENT_ID_NAME));
			Object oid = object.get(DBCollection.MUNGO_DOCUMENT_ID_NAME);
			if (oid instanceof ObjectId){
				e = new Entity(KeyStructure.createKey(((ObjectId) oid).toStringMongod(), kind));
			} else if (oid instanceof String){
				e = new Entity(KeyStructure.createKey((String)oid, kind));
			} else {
				// FIXME - This could be really unsafe
				e = new Entity(KeyStructure.createKey(oid.toString(), kind));
			}
			LOG.debug("Datastore Key constructed: " + e.getKey());
		}
		Map<String,Object> map = convertToMap(object);
		Iterator<Entry<String, Object>> it = map.entrySet().iterator();
		if (!it.hasNext()){
			LOG.debug("Iterator is empty");
		}
		while (it.hasNext()){
			if (e == null) {
				e = new Entity(KeyStructure.createKey(new ObjectId().toStringMongod(), kind));
			}
			Object entry = it.next();
			try {
				Map.Entry<Object, Object> mapEntry
					= (Entry<Object, Object>) entry;
				// Key at this point is still raw
				Object key = mapEntry.getKey();
				Object value = mapEntry.getValue();
				if (key instanceof String
						&& !((String) key).equals(DBCollection.MUNGO_DOCUMENT_ID_NAME)){ // skip the object id
					if (value instanceof Map){
						e.setProperty((String)key, createEmbeddedEntityFromMap(null, (Map)value));
					} else if (value instanceof List){
						throw new RuntimeException("List values are not yet supported");
					} else if (value instanceof String 
							|| value instanceof Number
							|| value instanceof Boolean
							|| value instanceof Date
							|| value instanceof User) {
						e.setProperty((String)key, value);
					} else {
						throw new RuntimeException("Unsupported DBObject property type");
					}
				}
				Preconditions.checkNotNull(e, "Entity is null");
			} catch (ClassCastException ex) {
				// Something is wrong here
				ex.printStackTrace();
			} catch (Exception ex){
				ex.printStackTrace();
			}
		}	
		return e;
	}
	
	public static Map<String,Object> createMapFromEntity(Entity e){
		Map<String,Object> map = null;
		if (e == null)
			return null;		
		try {
			map = new LinkedHashMap<String, Object>();
			Map<String,Object> props = e.getProperties();
			Iterator<Map.Entry<String, Object>> it = props.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<String, Object> entry = it.next();
				String key = entry.getKey();
				Object val = entry.getValue();
				if (val == null){
					map.put(key, val);
				} else if (val instanceof String
						|| val instanceof Number
						|| val instanceof Boolean
						|| val instanceof Date
						|| val instanceof User) {
					map.put(key, val);
				} else if (val instanceof Text) {
					map.put(key, ((Text) val).getValue());
				} else if (val instanceof EmbeddedEntity) { // List and Map are stored as EmbeddedEntity internally
					// TODO Must identify if the EmbeddedEntity is a List or Map
					LOG.debug( "Embedded entity found.");
					//Map<String,Object> ee = getMapFromEmbeddedEntity((EmbeddedEntity) val);
					// Fix for Github Issue #17
					Object mapOrList = Mapper.getMapOrList((EmbeddedEntity) val);
					map.put(key, mapOrList);
				} 
			}
			map.put(DBCollection.MUNGO_DOCUMENT_ID_NAME, e.getKey().getName());
		} catch (Exception ex) {
			// Just return null
		} finally {
			
		}
		return map;
	}	
	
	/**
	 * 
	 * Evaluates whether to return a <code>List</code> or a <code>Map</code> from the 
	 * values in the given <code>EmbeddedEntity</code>
	 * <br>
	 * <br>
	 * Since a JSON List or Map is stored in the same type as a <code>EmbeddedEntity</code> 
	 * it is needed to analyze the property names of the specified embedded entity to decide whether its 
	 * a <code>List</code> or a <code>Map</code> instance. 
	 * 
	 * An <code>EmbeddedEntity</code> was chosen approach than directly mapping the list into the 
	 * parent Entity because JSON array can contain arbitrary values and even objects too. 
	 * 
	 * This method will read all the property names of the entity and if all of its properties have 
	 * a dot-number prefix then it will be transformed into a List, otherwise a Map
	 * 
	 * @param ee
	 * @return
	 */
	public static Object getMapOrList(final EmbeddedEntity ee){
		boolean isList = true;
		Iterator<String> it = ee.getProperties().keySet().iterator();
		while (it.hasNext()){
			String propName = it.next();
			if (!propName.matches("[0-9]{1,9}")){
				isList = false;
			}
		}
		if (isList){
			return getListFromEmbeddedEntity(ee);
		} else {
			return getMapFromEmbeddedEntity(ee);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static DBObject createDBObjectFromEntity(Entity e){
		// FIXME - Would instantiated this here 
		// will make it very slow?
		ObjectSerializer serializer = new XStreamSerializer();
		if (e == null)
			return null;
		Map<String,Object> map = createMapFromEntity(e);
		Map<String,Object> newMap = new HashMap<String, Object>();
		BasicDBObject obj = new BasicDBObject();
		// Check if the String values or deserializable
		Iterator<String> it = map.keySet().iterator();
		while (it.hasNext()){
			String key = it.next();
			Object value = map.get(key);
			if (value instanceof String){
				try {
					value = serializer.deserialize((String)value);
				} catch (SerializationException e2) {
					// do nothing
				} catch (Exception e2) {
					// do nothing
				}
			}
			newMap.put(key, value);
		}
		obj.putAll(newMap);
		_checkId(obj);
		return obj;
	}
	
	private static void _checkId(DBObject obj) {
		if (obj.get(DBCollection.MUNGO_DOCUMENT_ID_NAME) != null 
				&& obj.get(DBCollection.MUNGO_DOCUMENT_ID_NAME) instanceof ObjectId){
			((ObjectId)obj.get(DBCollection.MUNGO_DOCUMENT_ID_NAME)).notNew();
		}
	}
	
	public static Map<String,Object> convertToMap(DBObject o){
		return o.toMap();
	}	
	
	/**
	 * Translates a DBObject query into a Map of Datastore Query filters
	 * e.g. { "field1" : { "$gte" : 1} }
	 * 
	 * @param query
	 * @return
	 */
	public static Map<String, Tuple<FilterOperator, Object>> createFilterOperatorObjectFrom(DBObject query){ 
		if (query == null){
			return null;
		}
		Map<String, Tuple<FilterOperator, Object>> _ops = new HashMap<String, Tuple<FilterOperator, Object>>();
		// Iterate over all the fields
		for (String field : query.keySet()){
			// Skip fields starting with '$' as its a query field
			if (field.startsWith("$")){
				continue;
			}
			try {
				Object operatorOrValue = (Object) query.get(field);
				LOG.debug("Operator="+operatorOrValue);
				if (operatorOrValue instanceof String 
						|| operatorOrValue instanceof Boolean
						|| operatorOrValue instanceof Number
						|| operatorOrValue instanceof Date
						|| operatorOrValue instanceof User){
					// then just do, equality operator
					_ops.put(field, new Tuple<Query.FilterOperator, Object>(FilterOperator.EQUAL, operatorOrValue));   
				} else if (operatorOrValue instanceof ObjectId) {
					String objectString = ((ObjectId)operatorOrValue).toStringMongod();
					if (field == DBCollection.MUNGO_DOCUMENT_ID_NAME){ // make the ID field compatible with GAE datastore entity
						field = GAE_ENTITY_ID_NAME;
					}
					_ops.put(GAE_ENTITY_ID_NAME, new Tuple<Query.FilterOperator, Object>(FilterOperator.EQUAL, objectString));   
				} else if (operatorOrValue instanceof DBObject) {
					for (String op : ((DBObject)operatorOrValue).keySet()){
						// e.g { "$gte" : 10 } 
						Object compareValue = ((DBObject)operatorOrValue).get(op);
						_ops.put(field, new Tuple<Query.FilterOperator, Object>(OpDecode.parseFilterFrom(op), compareValue));   
					}	
				} 
			} catch (Exception e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Invalid query: " + query.get(field));
			}
		}
		LOG.debug("Created filter object=" + _ops);
		LOG.debug("Created from DBObject=" + JSON.serialize(query)); 
		return _ops;
	}
	
	/**
	 * Transform a update query like:
	 *  
	 * {name: 'Joe'}").with("{$inc: {age: 1}}
	 * 
	 * into Map update query
	 * 
	 * @param query
	 * @return
	 */
	/*
	@SuppressWarnings("unused")
	public static Map<String, Tuple<UpdateOperator, Object>> createUpdateOperatorFrom(DBObject query) {
		LOG.debug("Creating update operator from query: " + query);
		Map<String, Tuple<UpdateOperator, Object>> result = null;
		Iterator<String> it = query.keySet().iterator();
		while(it.hasNext()){
			if (result == null){
				result = new HashMap<String, Tuple<UpdateOperator, Object>>();
			}
			String operatorField = it.next();
			Object fieldValue = query.get(operatorField);
			
			// Skip fields that are not query fields, i.e
			// starts with '$'
			if (!operatorField.startsWith("$")){
				continue;
			}
			
			//System.out.println(fieldValue.getClass().getName());
			if (fieldValue != null && fieldValue instanceof DBObject){
				Map<String, Tuple<FilterOperator, Object>> filters = new HashMap<String, Tuple<FilterOperator, Object>>();
				BasicDBList dbo = (BasicDBList) fieldValue;
				Iterator<String> _it = dbo.keySet().iterator();
				while (_it.hasNext()){ // Get the field and value for this current operation
					//DBObject item = _it.next();
					//String field = _it.next();
					//Object value = dbo.get(field);
					//result.put(field, new Tuple<Update.UpdateOperator, Object>(OpDecode.parseUpdateFilterFrom(operatorField), value));
					String key = _it.next();
					Object value = dbo.get(key);
					if (value instanceof DBObject){
						Mapper.createFilterOperatorObjectFrom((DBObject)value);
						Iterator<String> __it = ((DBObject)value).keySet().iterator();
						if (__it.hasNext()){
							String _key = __it.next();
							Object innerValue = ((DBObject)value).get(_key);
							if (innerValue instanceof DBObject){
								// get keys $lt, $lte, etc...
								String opKey = "";
								Iterator<String> ____it = ((DBObject)innerValue).keySet().iterator();
								while(____it.hasNext()){
									Object __value = ((DBObject)innerValue).get(opKey);  // e.g. 20
								}
							} else {
								// perhaps a String or Long type
//								result.put(_key, new Tuple<Update.UpdateOperator, Object>(
//										OpDecode.parseUpdateFilterFrom(opKey), innerValue));
							}
//							result.put(_key, new Tuple<Update.UpdateOperator, Object>(
//									OpDecode.parseUpdateFilterFrom(opKey), innerValue));
						}
					}
					value = 1;
				}
			}
		}
		return result;
	}
	*/
	/**
	* Transform a update query like:
	*
	* {name: 'Joe'}").with("{$inc: {age: 1}}
	*
	* into Map update query
	*
	* @param query
	* @return
	*/
	@SuppressWarnings("unused")
	public static Map<String, Tuple<UpdateOperator, Object>> createUpdateOperatorFrom(DBObject query) {
		LOG.debug("Creating update operator from query: " + query);
		Map<String, Tuple<UpdateOperator, Object>> result = null;
		Iterator<String> it = query.keySet().iterator();
		while(it.hasNext()){
			if (result == null){
				result = new HashMap<String, Tuple<UpdateOperator, Object>>();
			}
			String operatorField = it.next();
			Object fieldValue = query.get(operatorField);
		
			// Skip fields that are not query fields, i.e
			// starts with '$'
			if (!operatorField.startsWith("$")){
				continue;
			}
		
			//System.out.println(fieldValue.getClass().getName());
			if (fieldValue != null && fieldValue instanceof DBObject){
//				if (fieldValue instanceof BasicDBList){
//					BasicDBList dbo = (BasicDBList) fieldValue;
//					Iterator<String> _it = dbo.keySet().iterator();
//					while (_it.hasNext()){
//						String key = _it.next();
//						Object value = dbo.get(key);
//						if (value instanceof DBObject){
//							Map<String, Tuple<FilterOperator, Object>>  fitler = Mapper.createFilterOperatorObjectFrom((DBObject)value);
//							Iterator<String> __it = fitler.keySet().iterator();
//						}
//					}
//				} else {
//					BasicDBObject dbo = (BasicDBObject) fieldValue;
//					Iterator<String> _it = dbo.keySet().iterator();
//					while (_it.hasNext()){ // Get the field and value for this current operation
//						String field = _it.next();
//						Object value = dbo.get(field);
//						result.put(field, new Tuple<Update.UpdateOperator, Object>(OpDecode.parseUpdateFilterFrom(operatorField), value));
//					}	
//				}
				BasicDBObject dbo = (BasicDBObject) fieldValue;
				Iterator<String> _it = dbo.keySet().iterator();
				while (_it.hasNext()){ // Get the field and value for this current operation
					String field = _it.next();
					Object value = dbo.get(field);
					result.put(field, new Tuple<Update.UpdateOperator, Object>(OpDecode.parseUpdateFilterFrom(operatorField), value));
				}
			}
		}
		return result;
	}	
	
	/*
	 * Build an object of of a Logical query string
	 * 
	 * <code>
	 * { price:1.99, $or: [ { qty: { $lt: 20 } }, { sale: true } ] }
	 * </code>
	 */
	@SuppressWarnings("unused")
	public static List<Tuple<Logical,List<Tuple<String,Tuple<FilterOperator,Object>>>>> createLogicalQueryObjectFrom(DBObject logic){
		List<Tuple<Logical,List<Tuple<String,Tuple<FilterOperator,Object>>>>> logicList
			= new ArrayList<Tuple<Logical,List<Tuple<String,Tuple<FilterOperator,Object>>>>>();
		if (logic == null){
			return logicList; // or null?
		}
		for (String key : logic.keySet()){
			Object fieldValue = logic.get(key); 
			if (key.equals("$or")){	
				if (fieldValue instanceof BasicDBList){
					BasicDBList dbo = (BasicDBList) fieldValue;
					Iterator<String> it = dbo.keySet().iterator();
					while (it.hasNext()){
						String _key = it.next();
						Object value = dbo.get(_key);
						if (value instanceof DBObject){
							Map<String, Tuple<FilterOperator, Object>>  filter = Mapper.createFilterOperatorObjectFrom((DBObject)value);
							Iterator<String> filterIterator = filter.keySet().iterator();
							// for each filter bound to this $or
							// add and entry to the result
							List<Tuple<String,Tuple<FilterOperator,Object>>> list 
								= new ArrayList<Tuple<String,Tuple<FilterOperator,Object>>>();
							while(filterIterator.hasNext()){
								String filterKey = filterIterator.next();
								list.add(new Tuple<String, Tuple<FilterOperator,Object>>(filterKey, filter.get(filterKey)));
								LOG.debug("Filter operator in logical query=" + filterKey);
							}
							logicList.add(new Tuple<Logical, List<Tuple<String,Tuple<FilterOperator,Object>>>>(Logical.OR, list));
						} 
					}
				}
//				List<Tuple<FilterOperator,Object>> conditions = new ArrayList<Tuple<FilterOperator,Object>>();
//				for (Object pair : values) {
//					LOG.debug("Pair object type="+((BasicDBObject)pair).getClass().getName());
//					for (String filterKey : ((BasicDBObject)pair).keySet()){ // $lt, $lte, etc...
//						if (!filterKey.startsWith("$")){
//							// Then this must be value
//							conditions.add(new Tuple<Query.FilterOperator, Object>(FilterOperator.EQUAL, ((BasicDBObject)pair).get(filterKey))); 
//						} else {
//							conditions.add(new Tuple<Query.FilterOperator, Object>(OpDecode.parseFilterFrom(filterKey), 
//									((BasicDBObject)pair).get(filterKey))); 
//						}
//						
//					}
//				}
//				logicList.add(new Tuple<Logical, List<Tuple<FilterOperator,Object>>>(Logical.OR, conditions));
			} else if (key.equals("$and")){
//				List<Tuple<FilterOperator,Object>> conditions = new ArrayList<Tuple<FilterOperator,Object>>();
//				for (Object pair : values) {
//					for (String filterKey : ((BasicDBObject)pair).keySet()){ // $lt, $lte, etc...
//						if (!filterKey.startsWith("$")){
//							// Then this must be value
//							conditions.add(new Tuple<Query.FilterOperator, Object>(FilterOperator.EQUAL, ((BasicDBObject)pair).get(filterKey))); 
//						} else {
//							conditions.add(new Tuple<Query.FilterOperator, Object>(OpDecode.parseFilterFrom(filterKey), 
//									((BasicDBObject)pair).get(filterKey))); 
//						}
//						
//					}
//				}
//				logicList.add(new Tuple<Logical, List<Tuple<FilterOperator,Object>>>(Logical.AND, conditions));				
			} else if (key.equals("$not")){
//				List<Tuple<FilterOperator,Object>> conditions = new ArrayList<Tuple<FilterOperator,Object>>();
//				for (Object pair : values) {
//					for (String filterKey : ((BasicDBObject)pair).keySet()){ // $lt, $lte, etc...
//						if (!filterKey.startsWith("$")){
//							// Then this must be value
//							conditions.add(new Tuple<Query.FilterOperator, Object>(FilterOperator.EQUAL, ((BasicDBObject)pair).get(filterKey))); 
//						} else {
//							conditions.add(new Tuple<Query.FilterOperator, Object>(OpDecode.parseFilterFrom(filterKey), 
//									((BasicDBObject)pair).get(filterKey))); 
//						}
//						
//					}
//				}
//				logicList.add(new Tuple<Logical, List<Tuple<FilterOperator,Object>>>(Logical.NOT, conditions));				
			} else if (key.equals("$nor")){
//				List<Tuple<FilterOperator,Object>> conditions = new ArrayList<Tuple<FilterOperator,Object>>();
//				for (Object pair : values) {
//					for (String filterKey : ((BasicDBObject)pair).keySet()){ // $lt, $lte, etc...
//						if (!filterKey.startsWith("$")){
//							// Then this must be value
//							conditions.add(new Tuple<Query.FilterOperator, Object>(FilterOperator.EQUAL, ((BasicDBObject)pair).get(filterKey))); 
//						} else {
//							conditions.add(new Tuple<Query.FilterOperator, Object>(OpDecode.parseFilterFrom(filterKey), 
//									((BasicDBObject)pair).get(filterKey))); 
//						}
//						
//					}
//				}
//				logicList.add(new Tuple<Logical, List<Tuple<FilterOperator,Object>>>(Logical.NOR, conditions));				
			}
		}
		return logicList;
	}
	
	/**
	 * Transform a order by object like:
	 * 
	 * into a field-SortDirection map 
	 * 
	 * @param orderby
	 * @return
	 */
	public static Map<String, Query.SortDirection> createSortObjectFrom(DBObject orderby){
		if (orderby == null){
			return null;
		}
		Map<String, Query.SortDirection> sorts = new LinkedHashMap<String,Query.SortDirection>();
		Iterator<String> kit = orderby.keySet().iterator();
		while (kit.hasNext()){
			String key = kit.next();
			int dir = (Integer) orderby.get(key);
			Query.SortDirection direction =  dir == 1 ? 
					Query.SortDirection.ASCENDING : null;
			direction = dir == -1 ? 
					Query.SortDirection.DESCENDING : direction;
			LOG.debug("Adding sort key="+key + " direction=" + direction);
			sorts.put(key, direction);
		}
		return sorts;
	}
	
	/**
	 * Creates a new DBObject from a update object
	 * 
	 * @param updates
	 * @return
	 */
	public static DBObject createDBObjectFromUpdateFilter(Map<String, Tuple<UpdateOperator, Object>> updates){
		BasicDBObject dbo = new BasicDBObject();
		Iterator<String> it = updates.keySet().iterator();
		while(it.hasNext()){
			String field = it.next();
			// '$set'
			Tuple<UpdateOperator, Object> filterOp = updates.get(field);
			if (filterOp.getFirst() == UpdateOperator.SET){
				dbo.put(field, filterOp.getSecond());
			} else if (filterOp.getFirst() == UpdateOperator.UNSET){
				
			} else if (filterOp.getFirst() == UpdateOperator.INCREMENT){
				
			} else if (filterOp.getFirst() == UpdateOperator.DECREMENT){
				
			}
		}
		return dbo;
	}
	
	/**
	 * Construct a new object of type T from a given <code>Map</code> 
	 * 
	 * @param clazz
	 * @param toConvert
	 * @return
	 */
	public static <T> T createTObject(Class<T> clazz, Map<String,Object> toConvert){
		T obj = null;
		Gson gson = new Gson();
		try {
			obj = gson.fromJson(gson.toJson(toConvert), clazz);
		} catch (JsonSyntaxException e) {
			LOG.error("Cannot create object because JSON string is malformed");
		} catch(Exception e) {
			LOG.error("Some other error occurred when trying to deserialize JSON string");
		}
		return obj;		
	}
	
	// TODO - Check if obj has "_id" as it is required
	public DBObject copyFieldsOnly(DBObject obj, DBObject fields){
		DBObject copy = null;
		Iterator<String> it = fields.keySet().iterator();
		while (it.hasNext()){
			if (copy == null){
				copy = new BasicDBObject();
			}
			String key = it.next();
			copy.put(key, obj.get(key));
		}
		return copy;
	}
	
	/**
	 * Helper method to convert a list of <code>Entity</code> to
	 * list of <code>Map</code>.
	 * 
	 * @param entities
	 * @return
	 */
	public static List<Map<String,Object>> entitiesToMap(List<Entity> entities){
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		for (Entity e : entities){
			Map<String,Object> m = e.getProperties();
			list.add(m);
		}
		return list;
	}
	
	public static DBObject createDBObjectQueryFrom(Map<String, Tuple<FilterOperator, Object>> filter){
		BasicDBObject query = new BasicDBObject();
		for (String key : filter.keySet()){
			Tuple<FilterOperator, Object> f = filter.get(key);
			FilterOperator operator = f.getFirst();
			Object value = f.getSecond();
			if (operator == FilterOperator.EQUAL){
				query.append(key, new BasicDBObject("$e", value));
			} else if (operator == FilterOperator.NOT_EQUAL){
				query.append(key, new BasicDBObject("$ne", value));
			} else if (operator == FilterOperator.GREATER_THAN) {
				query.append(key, new BasicDBObject("$gt", value));
			} else if (operator == FilterOperator.GREATER_THAN_OR_EQUAL) {
				query.append(key, new BasicDBObject("$gte", value));
			} else if (operator == FilterOperator.LESS_THAN) {
				query.append(key, new BasicDBObject("$le", value)); 
			} else if (operator == FilterOperator.LESS_THAN_OR_EQUAL) {
				query.append(key, new BasicDBObject("$lte", value));
			}else if (operator == FilterOperator.IN) {
				query.append(key, new BasicDBObject("$in", value));
			}
		}
		return query;
	}
	
	public static DBObject createDBObjectOrderByFrom(Map<String, com.google.appengine.api.datastore.Query.SortDirection> sort){
		if (sort == null){
			return new BasicDBObject();
		}
		BasicDBObject dbQuery = new BasicDBObject();
		for (String key : sort.keySet()){
			com.google.appengine.api.datastore.Query.SortDirection direction = sort.get(key);
			if (direction == com.google.appengine.api.datastore.Query.SortDirection.ASCENDING){
				dbQuery.append(key, 1);
			} else if (direction == com.google.appengine.api.datastore.Query.SortDirection.DESCENDING){
				dbQuery.append(key, -1);
			} 
		}
		return dbQuery;
	}
	
	/**
	 * { $inc: { field1: amount } } 
	 * 
	 * @param update
	 * @return
	 */
	public static DBObject createDBObjectUpdateFrom(Map<String, Tuple<UpdateOperator, Object>> update){
		BasicDBObject dbUpdate = new BasicDBObject();
		for (String key : update.keySet()){
			Tuple<UpdateOperator, Object> value = update.get(key);
			if (value.getFirst() == UpdateOperator.INCREMENT){
				dbUpdate.append("$inc", new BasicDBObject(key, value.getSecond()));
			} else if (value.getFirst() == UpdateOperator.DECREMENT){
				dbUpdate.append("$dec", new BasicDBObject(key, value.getSecond()));
			} else if (value.getFirst() == UpdateOperator.RENAME){
				dbUpdate.append("$rename", new BasicDBObject(key, value.getSecond()));
			} else if (value.getFirst() == UpdateOperator.SET){
				dbUpdate.append("$set", new BasicDBObject(key, value.getSecond()));
			} else if (value.getFirst() == UpdateOperator.SET_ON_INSERT){
				dbUpdate.append("$setOnInsert", new BasicDBObject(key, value.getSecond()));
			} else if (value.getFirst() == UpdateOperator.UNSET){
				dbUpdate.append("$uset", new BasicDBObject(key, value.getSecond()));
			}
		}
		return dbUpdate;
	}
	
}
