package com.mungoae.object;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.inject.Singleton;
import com.mungoae.DB;

@Singleton
public class GAEObjectFactory {
	
	public GAEObjectFactory(){

	}
	
	public static GAEObject get(String namespace, String kind){
		return new GAEObject(namespace, kind);
	}
}
