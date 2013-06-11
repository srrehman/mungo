package com.mungoae.query;

import org.bson.types.ObjectId;

public class Oid {
	public static ObjectId withOid(String id){
		return new ObjectId(id);
	}
}
