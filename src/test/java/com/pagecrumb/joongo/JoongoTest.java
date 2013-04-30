package com.pagecrumb.joongo;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.pagecrumb.joongo.Joongo;
import com.pagecrumb.joongo.collection.DB;
import com.pagecrumb.joongo.collection.DBCollection;
import com.pagecrumb.joongo.collection.DBCursor;
import com.pagecrumb.joongo.collection.DBObject;
import com.pagecrumb.joongo.collection.WriteResult;
import com.pagecrumb.joongo.entity.BasicDBObject;

public class JoongoTest {
	
    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
                .setDefaultHighRepJobPolicyUnappliedJobPercentage(0)); 	   

    private Joongo joongo;
    
    @Before
    public void setUp() {
        helper.setUp(); 
        joongo = new Joongo();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }	
    
    @Test
    public void test() {
    	doTest();
    }
    
    public void doTest() {
    	DB db1 = joongo.getDB("db1");
    	DB db2 = joongo.getDB("db2");
    	
    	assertNotNull(db1);
    	assertNotNull(db2);
    	
    	DBCollection users = db1.createCollection("users");
    	DBCollection pages = db1.createCollection("pages");
    	DBCollection messages = db1.createCollection("messages");
    	
    	assertEquals(3, db1.countCollections());
    	assertEquals(0, db2.countCollections());
    	
    	assertNotNull(users);
    	
    	assertEquals("users",users.getName());
    	assertEquals("pages",pages.getName());
    	assertEquals("messages",messages.getName());
    	
    	BasicDBObject obj = new BasicDBObject("hi", "there");
    	BasicDBObject level1 = new BasicDBObject("yes", "i'm here");
    	obj.put("level1", level1);
    	BasicDBObject level2 = new BasicDBObject("where", "am i");
    	level1.put("level2", level2);
    	
    	
    	assertTrue(obj.get("hi") instanceof String);
    	assertTrue(obj.get("level1") instanceof DBObject);
    	
    	ObjectId id = new ObjectId();
    	obj.put("_id", id);
    	
    	BasicDBObject obj2 = new BasicDBObject();
    	ObjectId id2 = new ObjectId();
    	obj2.put("_id", id2);
    	obj2.put("hi", "there");
    	obj2.put("say", "cheeze");
    	
    	messages.insert(obj);
    	messages.insert(obj2);
    	
    	DBObject result = messages.findOne(id);
    	DBObject result2 = messages.findOne(id2);
    	
    	assertNotNull(result);
    	assertNotNull(result2);
    	
    	l(result.toJSONString());
    	l(result2.toJSONString());
    
    }
    
    @Test
    public void testCreateFromJSONString(){
    	BasicDBObject fromString = new BasicDBObject("{\"hello\" : \"world\"}");
    	assertNotNull(fromString);
    	assertEquals("world", (String) fromString.get("hello"));
    	l("Test create from JSON String=" + fromString);
    }
    
    public class Greeting {
    	private String greeting;

		public String getGreeting() {
			return greeting;
		}

		public void setGreeting(String greeting) {
			this.greeting = greeting;
		}
    }
    
    public class GreetingWithId {
    	private Long id;
    	private String greeting;

		public String getGreeting() {
			return greeting;
		}

		public void setGreeting(String greeting) {
			this.greeting = greeting;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}
    }
    
    @Test
    public void testAsObject() {
    	DB db = joongo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection greetings = db.createCollection("Greetings");
    	BasicDBObject fromString = new BasicDBObject("{\"greeting\" : \"good morning\"}")
    		.append("_id", id); 
    	greetings.insert(fromString);
    	Greeting greeting = greetings.findOne(id).as(Greeting.class);
    	assertNotNull(greeting);
    	l("Greeting greeting=" + greeting.getGreeting());
    }
    
    @Test
    public void testAsObjectWithObjectId() {
    	DB db = joongo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection greetings = db.createCollection("Greetings");
    	BasicDBObject fromString = new BasicDBObject("{\"greeting\" : \"good morning\"}")
    		.append("_id", id); 
    	greetings.insert(fromString);
    	GreetingWithId greeting = greetings.findOne(id).as(GreetingWithId.class);
    	assertNotNull(greeting);
    	l("Greeting id=" + greeting.getId() + " greeting=" + greeting.getGreeting());
    }    
    
    @Test
    public void testDBCursor() {
    	
    	DB db = joongo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection greetings = db.createCollection("Greetings");
    	
    	BasicDBObject fromString = new BasicDBObject("{\"greeting\" : \"good morning\"}")
    		.append("_id", id);  
    	BasicDBObject fromString2 = new BasicDBObject("{\"greeting\" : \"good morning\"}")
			.append("_id", new ObjectId());  
    	BasicDBObject fromString3 = new BasicDBObject("{\"moshimoshi\" : \"hello\"}")
			.append("_id", new ObjectId());      	
    	
    	greetings.insert(fromString);
    	greetings.insert(fromString2);
    	greetings.insert(fromString3);
    	
    	BasicDBObject ref = new BasicDBObject("greeting", "good morning");
    	DBCursor objects = greetings.find(ref);

    	for (DBObject obj : objects){
    		l("First Fetched DBOBject=" + obj.toJSONString());
    	}
    	
    	BasicDBObject fromString4 = new BasicDBObject("{\"greeting\" : \"good morning\"}")
    		.append("hello", "world");  
    	greetings.insert(fromString4);
    	
    	BasicDBObject ref2 = new BasicDBObject("greeting", "good morning")
    		.append("hello", "world");
    	
    	DBCursor objects2 = greetings.find(ref2);
    	
    	for (DBObject obj : objects2){
    		l("Second Fetched DBOBject=" + obj.toJSONString());
    	}
    	

    	BasicDBObject ref3 = new BasicDBObject("non", "existent");
    	objects2 = greetings.find(ref3);
    	assertNull(objects2);
    	
    	BasicDBObject ref4 = new BasicDBObject("non", "existent")
    		.append("hello", "world"); 
    	//assertNull(greetings.find(ref4)); 	
    	
    	DBCollection mixedCollection = db.createCollection("Mixed");
    	BasicDBObject numberValue1 = new BasicDBObject("count", 123);
    	BasicDBObject numberValue2 = new BasicDBObject("count", 123);
    	BasicDBObject numberValue3 = new BasicDBObject("count", 456);
    	BasicDBObject booleanValue = new BasicDBObject("active", false);
    	
    	mixedCollection.insert(numberValue1);
    	mixedCollection.insert(numberValue2);
    	mixedCollection.insert(numberValue3);
    	mixedCollection.insert(booleanValue);
    	
    	BasicDBObject ref5 = new BasicDBObject("count", 123);

    	assertTrue(mixedCollection.find(ref5).hasNext());
    	
    	for (DBObject o : mixedCollection.find(ref5)){
    		l("From mixedCollection query for 'count'=123 fetch=" + o.toJSONString());
    	}
    }
    
    @Test
    public void testParseBasicDBObjectType(){
    	DB db = joongo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection coll = db.createCollection("Collection");    	
    	Person person = new Person("Some", "One");
    	coll.insert(person);
    	BasicDBObject foundPerson = (BasicDBObject) coll.findOne(person); 
    	assertNotNull(foundPerson);
    }

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}
	
}

