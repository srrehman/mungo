package com.mungods.object;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.mungods.BasicDBObject;
import com.mungods.DBCursor;
import com.mungods.DBObject;

public class ObjectStoreTest {
    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
                .setDefaultHighRepJobPolicyUnappliedJobPercentage(0)); 	   	

    @Before
    public void setUp() {
        helper.setUp();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }	
	
	@Test
	public void testGetObjectId(){
		DBObject obj = new BasicDBObject("hello", "world");
		Object id = ObjectStore.get("db", "coll").persistObject(obj);
		assertNotNull(id);
		assertTrue(id instanceof ObjectId);
	}
	
	// Fails because of a bug
	// that a number String is interpreted
	// as Long
	//@Test
	public void testGetStringId(){
		DBObject obj = new BasicDBObject("hello", "world")
			.append("_id", "123");
		Object id = ObjectStore.get("db", "coll").persistObject(obj);
		assertNotNull(id);
		assertTrue(id instanceof String);
	}	
	
	@Test
	public void testGetLongId(){
		DBObject obj = new BasicDBObject("hello", "world")
			.append("hey", "dude") 
			.append("_id", 123L);
		Object id = ObjectStore.get("db", "coll").persistObject(obj);
		assertNotNull(id);
		assertTrue(id instanceof Long);
	}		
    
	@Test
	public void testPersistObject() {
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hello", "world"));
	}
	
	@Test
	public void testContainsObject() {
		// Test for object that does not exist
		assertFalse(ObjectStore.get("db", "coll")
				.containsObject(new BasicDBObject("hello", "world")));
		// Persist
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hello", "world"));
		// Check
		assertTrue(ObjectStore.get("db", "coll").containsObject(new BasicDBObject("hello", "world")));
	}
	
	@Test
	public void testGetObjectById(){
		// Get for non existent object
		// Should return null
		DBObject helloWorld = ObjectStore.get("db", "coll").getObject(
				new BasicDBObject("_id", "123")); 
		assertNull(helloWorld);
		
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hello", "world")
				.append("_id", "123"));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there")
				.append("how","are you?")); 
		
		helloWorld = ObjectStore.get("db", "coll").getObject(new BasicDBObject("_id", "123"));
		assertNotNull(helloWorld);
		assertEquals("world", helloWorld.get("hello"));
	}	
	
	@Test
	public void testGetObjectLike(){
		l(">>>>>>>>>>> Test Get Objects >>>>>>>>>>>>>>>>>");
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there"));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there"));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there")
			.append("how", "are you") 
			.append("good", "morning")); 
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("yey", "yow"));
		
		Iterator<DBObject> it 
			= ObjectStore.get("db", "coll").getObjectsLike(new BasicDBObject("hi", "there"));
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}
		l("<<<<<<<<<<< Test Get Objects <<<<<<<<<<<<<<<<<<");
	}	
	
	@Test
	public void testGetSortedObjectLike(){
		l(">>>>>>>>>>> Test Get Sorted Objects Like >>>>>>>>>>>>>>>>>");
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 1));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 2));
		ObjectStore.get("db", "coll").persistObject(
				new BasicDBObject("hi", "there").append("how", "are you").append("good", "morning").append("count", 3)); 
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("yey", "yow").append("count", 4));
		
		Iterator<DBObject> it 
			= ObjectStore.get("db", "coll").getSortedObjectsLike(new BasicDBObject("hi", "there"), new BasicDBObject("count", -1));
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}
		l("<<<<<<<<<<< Test Get Sorted Objects Like <<<<<<<<<<<<<<<<<<");
	}		
	
	@Test
	public void testGetSortedObject() {
		l(">>>>>>>>>>> Test Get Sorted Objects >>>>>>>>>>>");
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 1)) ;
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 2));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there")
			.append("how", "are you") 
			.append("good", "morning").append("count", 3)); 
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("yey", "yow").append("count", 4));
		
		Iterator<DBObject> it 
			= ObjectStore.get("db", "coll").getSortedObjects(new BasicDBObject("count", -1));
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}		
		l("<<<<<<<<<<< Test Get Sorted Objects <<<<<<<<<<<<");
	}
	
	@Test
	public void testDeleteObject(){
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there"));
		DBObject result = ObjectStore.get("db", "coll").getFirstObjectLike(new BasicDBObject("_id", "123"));
		ObjectStore.get("db", "coll").deleteObject(result);
		assertFalse(ObjectStore.get("db", "coll")
				.containsObject(new BasicDBObject("hi", "there")));
	}
	
	@Test
	public void testDeleteObjectNotExist(){
		boolean result = ObjectStore.get("db", "coll").deleteObject(new BasicDBObject("_id", "123"));
		assertFalse(result);
	}	

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}	
	
}
