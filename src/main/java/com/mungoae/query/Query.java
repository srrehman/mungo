package com.mungoae.query;

import com.mungoae.Mungo;

public class Query {
	
	private Mungo _mungo;
	
	public Query(Mungo mungo){
		_mungo = mungo;
	}
	
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
}
