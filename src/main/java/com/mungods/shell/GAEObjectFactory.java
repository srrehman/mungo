package com.mungods.shell;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.inject.Singleton;
import com.mungods.DB;
import com.mungods.object.ObjectStore;

@Singleton
public class GAEObjectFactory {
	
	public GAEObjectFactory(){

	}
	
	public static GAEObject get(String namespace, String kind){
		return new GAEObject(namespace, kind);
	}
}
