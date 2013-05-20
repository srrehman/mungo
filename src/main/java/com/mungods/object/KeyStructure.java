package com.mungods.object;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

public class KeyStructure {
	public static Key createKey(String kind, String key) {
		return KeyFactory.createKey(kind, key);
	}

	public static Key createKey(Key parent, String kind, String key) {
		return KeyFactory.createKey(parent, kind, key);
	}	
}
