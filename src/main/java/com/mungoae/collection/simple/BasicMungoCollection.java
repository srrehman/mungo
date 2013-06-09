package com.mungoae.collection.simple;

import java.util.List;

import com.mungoae.BasicDBObject;
import com.mungoae.DB;
import com.mungoae.DBObject;
import com.mungoae.MungoCollection;
import com.mungoae.object.ObjectStore;
import com.mungoae.query.BasicDBQuery;
import com.mungoae.query.Result;
import com.mungoae.query.DBQuery;
import com.mungoae.query.UpdateQuery;

public class BasicMungoCollection extends MungoCollection {

	private ObjectStore _store = null;
	
	public BasicMungoCollection(String database, String collection) {
		super(database, collection);
		_store = ObjectStore.get(database, collection);
	}

	@Override
	public DBQuery find() {
		return new BasicDBQuery(this); 
	}

	@Override
	public DBQuery find(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBObject findOne(Object id) {
		return _store.getObjectById(id);
	}

	@Override
	public void insert(String doc) {
		BasicDBObject obj = new BasicDBObject(doc);
		insert(obj);
	}

	@Override
	public <T> void insert(T doc) {
		if (doc instanceof DBObject){
			_store.persistObject((DBObject)doc);
		} else {
			
		}		
	}

	@Override
	public <T> void insert(T... docs) {
		if (docs instanceof DBObject[]){
			
		} else {
			
		}		
	}

	@Override
	public <T> void insert(List<T> docs) {
		if (docs instanceof List){
			
		} else {
			
		}
	}
	
	@Override
	public UpdateQuery update(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T save(T doc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean remove(Object id) {
		return _store.deleteObject(id);
	}

	@Override
	public boolean remove(String query) {
		// TODO Auto-generated method stub
		return false;
	}


}
