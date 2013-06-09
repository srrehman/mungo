package com.mungoae.query;

import java.util.Iterator;

import com.mungoae.DBObject;

public interface Query {
	public enum SortDirection { 
		ASCENDING(1), DESCENDING(-1);
		private final int dir;
		SortDirection(int dir){
			this.dir = dir;
		}
		public int getValue() {
			return dir;
		}
	}
	public QueryImpl or(Object value);
	public Iterator<DBObject> now();
	public QueryFilter filter(String field);
	public QueryImpl sort(QueryImpl.SortDirection direction);
	public QueryImpl sort(String field, QueryImpl.SortDirection direction);
	public QueryImpl limit(int max);
	public QueryImpl skip(int numToSkip);
}
