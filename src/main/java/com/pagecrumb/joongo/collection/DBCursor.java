package com.pagecrumb.joongo.collection;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.TransactionOptions;

public class DBCursor implements Iterator<DBObject>,
	Iterable<DBObject>, Closeable {
	
	private static final Logger LOG 
		= Logger.getLogger(DBCursor.class.getName());
	
	protected static DatastoreService _ds;
	protected static TransactionOptions options;
	
	private Iterator<Entity> it; 
	private PreparedQuery pq;
	private final String dbName;
	private final String collName; // kind
	
	public DBCursor(DBCollection collection) {
		if (_ds == null) {
			_ds = DatastoreServiceFactory.getDatastoreService();
			options = TransactionOptions.Builder.withXG(true);
			LOG.log(Level.INFO, "Create a new DatastoreService instance");
		}		
		dbName = collection.getDB().getName();
		collName = collection.getName();
		Query q = new Query(collName); 
		this.pq = _ds.prepare(q);
	}
	
	public DBCursor(DBCollection collection, int numToSkip, int batchSize){
		this(collection);
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	public Iterator<DBObject> iterator() {
		throw new IllegalArgumentException("Not yet implemented");
	}

	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	public DBObject next() {
		Entity e = it.next();
		DBObject next = null;
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(dbName);
		try {
			next = createFrom(e);
		} catch (Exception ex) {

		} finally {
			NamespaceManager.set(oldNamespace);
		}		
		return null;
	}

	public void remove() {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * Constructs a <code>DBObject</code> from <code>Entity</code>. 
	 * Might call subsequent Datastore queries if needed.
	 * 
	 * @param e
	 * @return
	 */
	private static DBObject createFrom(Entity e){
		return null;
	}

}
