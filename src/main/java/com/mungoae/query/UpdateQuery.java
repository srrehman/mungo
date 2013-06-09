package com.mungoae.query;

import java.util.HashMap;
import java.util.Map;

import com.mungoae.util.Tuple;

public class UpdateQuery {

	public enum UpdateOperator {
		INCREMENT, DECREMENT
	}
	
	private boolean _multi = false;
	private boolean _upsert = false;
	
	private Integer _by = 0;
	private String _field = null;
	
	private Map<String, Tuple<UpdateOperator, Object>> updates = new HashMap<String,Tuple<UpdateOperator, Object>>();
	
	public UpdateQuery increment(String field, Object byValue) {
		_field = field;
		updates.put(_field, new Tuple<UpdateQuery.UpdateOperator, Object>(UpdateOperator.INCREMENT, byValue));
		return this;
	}
	
	public UpdateQuery decrement(String field, Object byValue) {
		_field = field;
		updates.put(_field, new Tuple<UpdateQuery.UpdateOperator, Object>(UpdateOperator.DECREMENT, byValue));
		return this;
	}
	
	public void by(Integer by){
		_by = by;

	}

	
	public UpdateQuery upsert() {
		_upsert = true;
		return this;
	}
	public UpdateQuery multi() {
		_multi = true;
		return this;
	}
	
	public boolean isMulti() {
		return _multi;
	}
	
	public boolean isUpsert() {
		return _upsert;
	}
}
