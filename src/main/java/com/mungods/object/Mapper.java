package com.mungods.object;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.ArrayUtils;
import org.bson.types.ObjectId;

import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Text;
import com.google.common.base.Preconditions;
import com.mungods.BasicDBObject;
import com.mungods.DBObject;

public class Mapper {
	
	private static final Logger logger 
		= Logger.getLogger(Mapper.class.getName());
	
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
					logger.log(Level.INFO, "Inner embedded entity found with key=" + entry.getKey());
//					newMap.put(entry.getKey(), getMapFromEmbeddedEntity( (EmbeddedEntity) entry.getValue()));
					newMap.put(entry.getKey(), getMapOrList( (EmbeddedEntity) entry.getValue()));
					it.remove();
				}
			}
			map.putAll(newMap);
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Error when processing EmbeddedEntity to Map");
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
				logger.info("Value="+entry.getValue());
				if (value instanceof String
						|| value instanceof Boolean
						|| value instanceof Number){
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
		logger.info("Removing elements from array="+elemToRemove);
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
						|| o instanceof Number){
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
			} else if(value instanceof List) {
				ee.setProperty(key, createEmbeddedEntityFromList(ee.getKey(), (List)value));
			} else if(value instanceof Map){
				Map<String, Object> map = (Map<String, Object>) value;
				ee.setProperty(key, createEmbeddedEntityFromMap(ee.getKey(), map));
			}			
		}
		logger.info("Warning method is returning null value");
		return ee;
	}
	
	public static Entity createEntityFromDBObject(DBObject object, String kind){
		Preconditions.checkNotNull(object, "DBObject cannot be null");
		Preconditions.checkNotNull(kind, "Entity kind cannot be null");
		Entity e = null;
		// Pre-process object id
		if (object.get(ID) != null){
			Object oid = object.get(ID);
			if (oid instanceof ObjectId){
				e = new Entity(KeyStructure.createKey(((ObjectId) oid).toStringMongod(), kind));
			} else if (oid instanceof String){
				e = new Entity(KeyStructure.createKey((String)oid, kind));
			} else {
				// FIXME This could be really unsafe
				e = new Entity(KeyStructure.createKey(oid.toString(), kind));
			}
		}
		Map map = convertToMap(object);
		Iterator it = map.entrySet().iterator();
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
							|| value instanceof Boolean) {
						e.setProperty((String)key, value);
					} else {
						throw new RuntimeException("Unsupported DBObject property type");
					}
				}
			} catch (ClassCastException ex) {
				// Something is wrong here
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
						|| val instanceof Boolean) {
					map.put(key, val);
				} else if (val instanceof Text) {
					map.put(key, ((Text) val).getValue());
				} else if (val instanceof EmbeddedEntity) { // List and Map are stored as EmbeddedEntity internally
					// TODO Must identify if the EmbeddedEntity is a List or Map
					logger.log(Level.INFO, "Embedded entity found.");
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
		if (e == null)
			return null;
		Map<String,Object> map = createMapFromEntity(e);
		BasicDBObject obj = new BasicDBObject();
		obj.putAll(map);
		return obj;
	}
	
	public static Map convertToMap(DBObject o){
		return o.toMap();
	}	
}
