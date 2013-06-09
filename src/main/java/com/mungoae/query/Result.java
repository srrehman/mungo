package com.mungoae.query;

public abstract class Result {
	public abstract <T> T as (Class<T> clazz);
}
