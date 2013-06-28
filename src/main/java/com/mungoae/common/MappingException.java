package com.mungoae.common;

public class MappingException extends MungoException {
	public MappingException() {
		super();
	}
	public MappingException(Exception e){
		super("", e.getCause());
	}
	public MappingException(Throwable t){
		super("", t);
	}
}
