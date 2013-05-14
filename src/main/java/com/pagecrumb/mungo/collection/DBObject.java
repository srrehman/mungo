package com.pagecrumb.mungo.collection;

import java.util.Map;
import java.util.Set;

public interface DBObject {
	/**
	 * Checks if this object contains a field with the given name.
	 * 
	 * @param s
	 * @return
	 */
	public boolean containsField(String s);
	@Deprecated
	public boolean containsKey(String s);
	/**
	 * Gets a field from this object by a given name.
	 * 
	 * @param key
	 * @return
	 */
	public Object get(String key);
	/**
	 * whether markAsPartialObject was ever called only matters 
	 * if you are going to upsert and dont' want to risk losing fields
	 * @return
	 */
	public boolean isPartialObject();
	/**
	 * Returns this object's fields' names
	 * 
	 * @return
	 */
	public Set<String> keySet();
	/**
	 * if this object was loaded with only some fields (using a field filter) 
	 * this method will be called to notify
	 */
	public void markAsPartialObject();
	/**
	 * Sets a name/value pair in this object.
	 * 
	 * @param key
	 * @param v
	 * @return
	 */
	public Object put(String key, Object v);
	public void putAll(DBObject o);
	public void putAll(Map m);
	/**
	 * Remove a field with a given name from this object. 
	 * @param key
	 * @return
	 */
	public Object removeField(String key);
	/**
	 * Returns a map representing this DBObject.
	 * 
	 * @return
	 */
	public Map toMap();
	
	public <T> T as(Class<T> clazz);
}
