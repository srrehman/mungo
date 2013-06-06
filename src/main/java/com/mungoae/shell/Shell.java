package com.mungoae.shell;

import com.mungoae.DBCursor;
import com.mungoae.DBObject;
/**
 * Shell interface to the GAE Datastore. It represents the "core" functions
 * of the Mungo Datastore API.
 * 
 * @author Kerby Martino <kerbymart@gmail.com>
 *
 */
public interface Shell {
	/**
	 * Primary method to insert a document or documents.
	 * Automatically generate object id if not present.
	 * 
	 * Bulk insert is processed when the document passed in is a array. 
	 * Otherwise, it is a single document. 
	 * 
	 * @see <a href="http://docs.mongodb.org/manual/core/create/"></a>
	 * @param document
	 */
	public void insert(DBObject document);
	public void save(DBObject document);
	public void update(DBObject query, DBObject update, boolean upsert); 
	/*
	 * 
	 * @param query
	 * @param projection defines which fields to return
	 */
	public DBCursor find(DBObject query, DBObject DBObject);
	public DBObject findOne(DBObject query, DBObject DBObject);
	
	public void remove(DBObject query, boolean justOne);
	public void remove();
	
	/**
	 * All write operation will issue this command to
	 * confirm the result of the write operation
	 * 
	 * @return
	 */
	public DBObject getLastError();
}
