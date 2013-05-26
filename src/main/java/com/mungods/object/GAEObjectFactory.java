package com.mungods.object;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.inject.Singleton;
import com.mungods.DB;

@Singleton
public class GAEObjectFactory {
	
	public GAEObjectFactory(){

	}
	
	public static GAEObject get(String namespace, String kind){
		return new GAEObject(namespace, kind);
	}
}
