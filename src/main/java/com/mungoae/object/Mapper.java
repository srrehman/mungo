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
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.mungoae.BasicDBObject;
import com.mungoae.DBCollection;
import com.mungoae.DBObject;
import com.mungoae.common.SerializationException;
import com.mungoae.operators.OpDecode;
import com.mungoae.operators.Operator;
import com.mungoae.query.UpdateQuery;
import com.mungoae.query.UpdateQuery.UpdateOperator;
import com.mungoae.serializer.ObjectSerializer;
import com.mungoae.serializer.XStreamSerializer;
import com.mungoae.util.JSON;
import com.mungoae.util.Tuple;

public class Mapper {
	
	private static Logger LOG = LogManager.getLogger(Mapper.class.getName());

	private static final String ID = "_id";
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
						|| value instanceof Date){
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
						|| o instanceof Date){
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
		if (object.get(ID) != null){
			LOG.debug("Constructing Entity from DBObject with id="+object.get(ID));
			Object oid = object.get(ID);
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
						&& !((String) key).equals(ID)){ // skip the object id
					if (value instanceof Map){
						e.setProperty((String)key, createEmbeddedEntityFromMap(null, (Map)value));
					} else if (value instanceof List){
						throw new RuntimeException("List values are not yet supported");
					} else if (value instanceof String 
							|| value instanceof Number
							|| value instanceof Boolean
							|| value instanceof Date) {
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
						|| val instanceof Date) {
					map.put(key, val);
				} else if (val instanceof Text) {
					map.put(key, ((Text) val).getValue());
				} else if (val instanceof EmbeddedEntity) { // List and Map are stored as EmbeddedEntity internally
					// TODO Must identify if the EmbeddedEntity is a List or Map
					LOG.debug( "Embedded entity found.");
					Map<String,Object> ee = getMapFromEmbeddedEntity((EmbeddedEntity) val);
					map.put(key, ee);
				} 
			}
			map.put(ID, e.getKey().getName());
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
		if (obj.get("_id") != null && obj.get("_id") instanceof ObjectId){
			((ObjectId)obj.get("_id")).notNew();
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
		Preconditions.checkNotNull(query, "Cannot convert null query object to filter object");
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
						|| operatorOrValue instanceof Date){
					// then just do, equality operator
					_ops.put(field, new Tuple<Query.FilterOperator, Object>(FilterOperator.EQUAL, operatorOrValue));   
				} else if (operatorOrValue instanceof ObjectId) {
					String objectString = ((ObjectId)operatorOrValue).toStringMongod();
					_ops.put(field, new Tuple<Query.FilterOperator, Object>(FilterOperator.EQUAL, objectString));   
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
				BasicDBObject dbo = (BasicDBObject) fieldValue;
				Iterator<String> _it = dbo.keySet().iterator();
				while (_it.hasNext()){ // Get the field and value for this current operation
					String field = _it.next();
					Object value = dbo.get(field);
					result.put(field, new Tuple<UpdateQuery.UpdateOperator, Object>(OpDecode.parseUpdateFilterFrom(operatorField), value));
				}
			}
		}
		return result;
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
		// Removed since pojo should have '_id' field
//		if (toConvert.get("_id")!=null){
//			Object id = toConvert.get("_id");
//			toConvert.remove("_id");
//			toConvert.put("id", id);
//		}
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
}