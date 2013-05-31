package com.mungods;

import org.bson.BSONObject;

public interface DBObject extends BSONObject {

	/**
	 * whether markAsPartialObject was ever called only matters 
	 * if you are going to upsert and dont' want to risk losing fields
	 * @return
	 */
	public boolean isPartialObject();

	/**
	 * if this object was loaded with only some fields (using a field filter) 
	 * this method will be called to notify
	 */
	public void markAsPartialObject();
	
	// TODO - Move this to Q class?
	public <T> T as(Class<T> clazz);
}
