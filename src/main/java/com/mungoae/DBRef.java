package com.mungoae;

import org.bson.BSONObject;

/**
 * overrides DBRefBase to understand a BSONObject representation of a reference.
 * @dochub dbrefs
 */
public class DBRef extends DBRefBase {
    
    static final boolean D = Boolean.getBoolean( "DEBUG.DBREF" );

    /**
     * Creates a DBRef
     * @param db the database
     * @param o a BSON object representing the reference
     */
    public DBRef(DB db , BSONObject o ){
        super( db , o.get( "$ref" ).toString() , o.get( "$id" ) );
    }

    /**
     * Creates a DBRef
     * @param db the database
     * @param ns the namespace where the object is stored
     * @param id the object id
     */
    public DBRef(DB db , String ns , Object id) {
        super(db, ns, id);
    }

    /**
     * fetches a referenced object from the database
     * @param db the database
     * @param ref the reference
     * @return
     * @throws MongoException
     */
    public static DBObject fetch(DB db, DBObject ref) {
        String ns;
        Object id;

        if ((ns = (String)ref.get("$ref")) != null && (id = ref.get("$id")) != null) {
            return db.getCollection(ns).findOne(new BasicDBObject(DBCollection.MUNGO_DOCUMENT_ID_NAME, id));
        }
        return null;
    }
}
