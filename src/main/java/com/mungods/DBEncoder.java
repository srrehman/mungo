package com.mungods;

import org.bson.BSONObject;

import com.google.appengine.api.datastore.Entity;

public interface DBEncoder {
    public int writeObject( Entity e, BSONObject o );
}
