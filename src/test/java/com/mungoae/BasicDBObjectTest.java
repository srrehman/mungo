package com.mungoae;

import static org.junit.Assert.*;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.mungoae.util.JSON;

public class BasicDBObjectTest {
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
	public void testCreateFromString() {
		BasicDBObject obj = new BasicDBObject("{'username' : 'kirbymart'}");
		assertNotNull(obj.get("username")); 
	}
	
	@Test
	public void testCreateQueryFromString() {
		BasicDBObject query = new BasicDBObject("{'$inc': { 'number': 1}}");
		assertNotNull(query.get("$inc")); 
	}
	
	@Test
	public void testCreateObjectWithID(){
		BasicDBObject obj = new BasicDBObject("_id", new ObjectId("51b6125923185d39fe416f94"));
		l(JSON.serialize(obj));
		
		BasicDBObject newObj = new BasicDBObject("{ '_id' : { '$oid' : '51b6125923185d39fe416f94'}}");
		assertNotNull(newObj);
		assertTrue(newObj.get("_id") instanceof ObjectId);
		l(JSON.serialize(newObj));
		
		newObj = new BasicDBObject("{'name': 'Kirby', age: 25, 'address' : { 'address': 'somewhere over the rainbow' } }")
			.append("_id", new ObjectId("51b6125923185d39fe416f94")); 
		
		assertNotNull(newObj);
		l(JSON.serialize(newObj));
		
		l(JSON.serialize(BasicDBObjectBuilder.start("_id", new ObjectId("51b6125923185d39fe416f94")).add("name", "Kirtby")
				.add("age", 26).add("address", new BasicDBObject("adress", "there")).get()));
	}

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}	
	
}
