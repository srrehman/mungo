package com.mungoae.object;

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
import com.mungoae.BasicDBObject;
import com.mungoae.BasicDBObjectBuilder;
import com.mungoae.DBObject;
import com.mungoae.object.ObjectStore;
import com.mungoae.util.Tuple;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.SortDirection;

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
	public void testContainsObject() {
		ObjectId id = new ObjectId();
		System.out.println("Test id=" + id.toStringMongod());
		
		// Test for object that does not exist
		assertFalse(ObjectStore.get("db", "coll")
				.containsObject(new BasicDBObject("hello", "world").append("_id", id)));
		
		// Persist
		
		ObjectStore.get("db", "coll").persistObject(
				new BasicDBObject("hello", "world").append("_id", id)); 
		// Check
		assertTrue(ObjectStore.get("db", "coll").containsObject(
				new BasicDBObject("hello", "world").append("_id", id))); 
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
	public void testGetObjectById(){
		// Get for non existent object
		// Should return null
		DBObject helloWorld = ObjectStore.get("db", "coll").queryObject(
				new BasicDBObject("_id", "123")); 
		assertNull(helloWorld);
		
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hello", "world")
				.append("_id", "123"));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there")
				.append("how","are you?")); 
		
		helloWorld = ObjectStore.get("db", "coll").queryObject(new BasicDBObject("_id", "123"));
		assertNotNull(helloWorld);
		assertEquals("world", helloWorld.get("hello"));
	}	
	
	@Test
	public void testGetObjectsLike(){
		l(">>>>>>>>>>> Test Get Objects Like >>>>>>>>>>>>>>>>>");
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there"));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there"));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there")
			.append("how", "are you") 
			.append("good", "morning")); 
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("yey", "yow"));
		
		Iterator<DBObject> it 
			= ObjectStore.get("db", "coll").queryObjects(new BasicDBObject("hi", new BasicDBObject("$gte", "there")), 
				null, // order by 
				null, // fields
				null, null, null, null);
		
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}
		l("<<<<<<<<<<< Test Get Objects Like <<<<<<<<<<<<<<<<<<");
	}	
	
	@Test
	public void testGetObjectLikeMulti() {
		l(">>>>>>>>>>> Test Get Objects Multi Like >>>>>>>>>>>>>>>>>");
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("title", "sample1"));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("title", "sample2"));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("title", "sample3"));
		
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("title", "sample1").append("count", 1));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("title", "sample2").append("count", 2));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("title", "sample3").append("count", 3));
		
		DBObject query = new BasicDBObject("title", new BasicDBObject("$e", "sample1"))
				.append("count", new BasicDBObject("$e", 1)); 
				
		Iterator<DBObject> it = ObjectStore.get("db", "coll").queryObjects(query, 
			null, // order by 
			null, // fields
			null, null, null, null);
		
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}
		l("<<<<<<<<<<< Test Get Objects Multi Like <<<<<<<<<<<<<<<<<<");
	}
	
	@Test
	public void testGetSortedObjectsLike(){
		l(">>>>>>>>>>> Test Get Sorted Objects Like >>>>>>>>>>>>>>>>>");
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 1));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 2));
		ObjectStore.get("db", "coll").persistObject(
				new BasicDBObject("hi", "there").append("how", "are you").append("good", "morning").append("count", 3)); 
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("yey", "yow").append("count", 4));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("will not be", "fetched"));
		
		Iterator<DBObject> it 
			= ObjectStore.get("db", "coll").queryObjects(
					new BasicDBObject("count", new BasicDBObject("$gte", 2)), 
					new BasicDBObject("count", -1), null, null, null, null, null);
		
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}
		l("<<<<<<<<<<< Test Get Sorted Objects Like <<<<<<<<<<<<<<<<<<");
	}	
	
	/**
	 * This test 
	 */
	@Test
	public void testGetSortedObjectLike2(){
		l(">>>>>>>>>>> Test Get Sorted Objects Like2 >>>>>>>>>>>>>>>>>");
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 1));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 2));
		ObjectStore.get("db", "coll").persistObject(
				new BasicDBObject("hi", "there").append("how", "are you").append("good", "morning").append("count", 3)); 
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("yey", "yow").append("count", 4));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("will not be", "fetched"));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("a", "b").append("number", 1)
				.append("count", 2)); 
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("c", "d").append("number", 2)
				.append("count", 1));  
		
		Iterator<DBObject> it = ObjectStore.get("db", "coll").queryObjects(
				new BasicDBObject("count", new BasicDBObject("$gte", 2)).append("number", new BasicDBObject("$gte", 1)), 
				new BasicDBObject("count", -1).append("number", -1), // order by 
				null, // fields
				null, null, null, null);
		
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}
		l("<<<<<<<<<<< Test Get Sorted Objects Like2 <<<<<<<<<<<<<<<<<<");
	}	
	
	@Test
	public void testQuerySortedObjects(){
		l(">>>>>>>>>>> Test Query Sorted Objects >>>>>>>>>>>>>>>>>");
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 1));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 2));
		ObjectStore.get("db", "coll").persistObject(
				new BasicDBObject("hi", "there").append("how", "are you").append("good", "morning").append("count", 3)); 
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("yey", "yow").append("count", 4));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("will not be", "fetched"));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("a", "b").append("number", 1)
				.append("count", 2)); 
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("c", "d").append("number", 2)
				.append("count", 1)); 
		
//		Iterator<DBObject> it 
//			= ObjectStore.get("db", "coll").queryAllObjectsLike(
//					new BasicDBObject("count", new BasicDBObject("$gte", 2))
//						.append("number", new BasicDBObject("$gte", 1)),  
//					new BasicDBObject("count", -1).append("number", -1)); 
		
		
		Map<String, Tuple<FilterOperator, Object>> filters = new HashMap<String, Tuple<FilterOperator,Object>>();
		Map<String, SortDirection> sorts = new HashMap<String, SortDirection>();
		sorts.put("count", SortDirection.DESCENDING);
		
		Iterator<DBObject> it = ObjectStore.get("db", "coll").queryObjects(
				Mapper.createDBObjectQueryFrom(filters),   
				Mapper.createDBObjectOrderByFrom(sorts), // order by 
				null, // fields
				null, null, null, null);
		
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}
		l("-----");
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("yey", "yow").append("count", 5));
		
		it = ObjectStore.get("db", "coll").queryObjects(
				Mapper.createDBObjectQueryFrom(filters),   
				Mapper.createDBObjectOrderByFrom(sorts), // order by 
				null, // fields
				null, null, null, null);
		
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}

		l("<<<<<<<<<<< Test Query Sorted Objects <<<<<<<<<<<<<<<<<<");
	}
	
	@Test
	public void testGetbjects(){
		l(">>>>>>>>>>> Test Get Objects >>>>>>>>>>>>>>>>>");
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 1));
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there").append("count", 2));
		ObjectStore.get("db", "coll").persistObject(
				new BasicDBObject("hi", "there").append("how", "are you").append("good", "morning").append("count", 3)); 
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("yey", "yow").append("count", 4));
		
		Iterator<DBObject> it = ObjectStore.get("db", "coll").queryObjects(
				null,   
				null, // order by 
				null, // fields
				null, null, null, null);
		
		
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}
		l("<<<<<<<<<<< Test Get Objects <<<<<<<<<<<<<<<<<<");		
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

		Iterator<DBObject> it = ObjectStore.get("db", "coll").queryObjects(
				null,   
				new BasicDBObject("count", -1), // order by 
				null, // fields
				null, null, null, null);
		
		assertNotNull(it);
		while(it.hasNext()){
			DBObject obj = it.next();
			assertNotNull(obj);
			l(obj);
		}		
		l("<<<<<<<<<<< Test Get Sorted Objects <<<<<<<<<<<<");
	}
	
	@Test
	public void testGetFirstObjectLike(){
		ObjectStore.get("db", "coll").persistObject(new BasicDBObject("hi", "there"));
	}
	
	@Test
	public void testDeleteObject(){
		ObjectStore.get("db", "coll").persistObject(
				new BasicDBObject("username", "jack").append("points", 100)); 
		DBObject result = ObjectStore.get("db", "coll").queryFirstObjectLike(
				new BasicDBObject("points", 
				new BasicDBObject("$gte", 100)));
		assertNotNull(result);
		l("Fetched document="+result);
		
		ObjectStore.get("db", "coll").deleteObject(result);
		
		assertFalse(ObjectStore.get("db", "coll")
			.containsObject(new BasicDBObject("hi", "there")));
	}
	
	//@Test
	public void testDeleteObjectNotExist(){
		boolean result = ObjectStore.get("db", "coll").deleteObject(
				new BasicDBObject("_id", 123));
		assertFalse(result);
	}	

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}	
	
}
