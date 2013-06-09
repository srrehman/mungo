package com.mungoae;

import java.util.List;

import com.mungoae.query.Result;
import com.mungoae.query.DBQuery;
import com.mungoae.query.UpdateQuery;

public abstract class MungoCollection {
	
	private DB _db;
	protected final String _databaseName;
	protected final String _collectionName;
	
	public MungoCollection(String database, String collection) {
		_databaseName = database;
		_collectionName = collection;
	}

	public abstract DBQuery find();
	public abstract DBQuery find(String query);
	public abstract DBObject findOne(Object id);
	public abstract void insert(String doc);
	public abstract <T> void insert(T doc);
	public abstract <T> void insert(T... docs); 
	public abstract <T> void insert(List<T> docs); 
	public abstract UpdateQuery update(String query);
	public abstract <T> T save(T doc);
	public abstract boolean remove(Object id);
	public abstract boolean remove(String query);
	
	//public abstract DBQuery __find();
	
	public String getName(){
		return _collectionName;
	}
	
	public String getDatabaseName() {
		return _databaseName;
	}
	
}
