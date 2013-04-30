package com.pagecrumb.joongo.collection;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;

public class DBCursor implements Iterator<DBObject>,
	Iterable<DBObject>, Closeable {
	
	private static final Logger LOG 
		= Logger.getLogger(DBCursor.class.getName());
	
	private final Iterator<DBObject> it;
	
	public DBCursor(Iterator<DBObject> it){
		Preconditions.checkNotNull(it, "DBObject iterator cannot be null");
		this.it = it;
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	public Iterator<DBObject> iterator() {
		return it;
	}

	public boolean hasNext() {
		return it.hasNext();
	}

	public DBObject next() {
		return it.next();
	}

	public void remove() {
		it.remove();
	}
	
}
