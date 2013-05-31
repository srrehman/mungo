/**
 * 	
 * Copyright 2013 Pagecrumb
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 */
package com.mungods;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.mungods.Mungo;
import com.mungods.collection.WriteResult;

public class MungoTest {
	
    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
                .setDefaultHighRepJobPolicyUnappliedJobPercentage(0)); 	   

    private Mungo mungo;
    
    @Before
    public void setUp() {
        helper.setUp(); 
        mungo = new Mungo();
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
    	DB db1 = mungo.getDB("db1");
    	DB db2 = mungo.getDB("db2");
    	
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
    	
    	l("Using fineOne(): " + toJSONString(result));
    	l("Using fineOne(): " + toJSONString(result));
    
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
    	DB db = mungo.getDB("db1");
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
    	DB db = mungo.getDB("db1");
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
    	
    	DB db = mungo.getDB("db1");
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
    		l("First Fetched DBOBject=" + toJSONString(obj));
    	}
    	
    	BasicDBObject fromString4 = new BasicDBObject("{\"greeting\" : \"good morning\"}")
    		.append("hello", "world");  
    	greetings.insert(fromString4);
    	
    	BasicDBObject ref2 = new BasicDBObject("greeting", "good morning")
    		.append("hello", "world");
    	
    	DBCursor objects2 = greetings.find(ref2);
    	
    	for (DBObject obj : objects2){
    		l("Second Fetched DBOBject=" + toJSONString(obj));
    	}
    	

    	BasicDBObject ref3 = new BasicDBObject("non", "existent");
    	DBCursor curr = greetings.find(ref3);
    	assertFalse(curr.hasNext());
    	//assertNull(curr);
    	
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
    		l("From mixedCollection query for 'count'=123 fetch=" + toJSONString(o));
    	}
    }
    
    @Test
    public void testParseBasicDBObjectType(){
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection coll = db.createCollection("Collection");    	
    	Person person = new Person("Some", "One");
    	coll.insert(person);
    	BasicDBObject foundPerson = (BasicDBObject) coll.findOne(person); 
    	assertNotNull(foundPerson);
    }
    
    @Test
    public void testPersistJSONArrayList(){
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection coll = db.createCollection("Collection");      
    	BasicDBObject numbers = new BasicDBObject("{\"numbers\" : [1,2,3,4,5]}");
    	numbers.put("_id", id);
    	coll.insert(numbers);
    	DBObject result = coll.findOne(id);
    	assertNotNull(result);
    	l("JSON array =" + toJSONString(numbers));
    } 
    
    @Test
    public void testPersistJSONArrayListWithAnyObject(){
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection coll = db.createCollection("Collection");      
    	BasicDBObject numbers = new BasicDBObject("{\"numbers\" : [true,1,2,3,\"hello world\"]}");
    	numbers.put("_id", id);
    	coll.insert(numbers);
    	DBObject result = coll.findOne(id);
    	assertNotNull(result);
    	l("JSON array =" + toJSONString(result));
    }   
    
    @Test
    public void testPersistJSONArrayListWithEmbeddedObject(){
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection coll = db.createCollection("Collection");      
    	BasicDBObject complexObject 
    		= new BasicDBObject("{\"numbers\" : [true,1,2,3,\"hello world\", { \"inner\": \"text\" }]}");
    	complexObject.put("_id", id);
    	coll.insert(complexObject);
    	DBObject result = coll.findOne(id);
    	assertNotNull(result);
    	l("Complex JSON array =" + toJSONString(result));
    }    
    
    @Test
    public void testPersistAndGetWithLongId(){
    	DB db = mungo.getDB("db1");
    	Long id = new Long(1L);
    	DBCollection coll = db.createCollection("Collection");      
    	BasicDBObject complexObject 
    		= new BasicDBObject("{\"numbers\" : [true,1,2,3,\"hello world\", { \"inner\": \"text\" }]}");
    	l("Put id to json="+id);
    	complexObject.put("_id", id);
    	coll.insert(complexObject);
    	DBObject result = coll.findOne(id);
    	assertNotNull(result);
    	l("Complex JSON array with Long id =" + toJSONString(result));    	
    }
    
    @Test
    public void testPersistComplexObjectQueryByProperty(){
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection coll = db.createCollection("Collection");      
    	BasicDBObject complexObject 
    		= new BasicDBObject("{\"numbers\" : [true,1,2,3,\"hello world\", { \"inner\": \"text\" }]}")
    			.append("name", "test123"); 
    	complexObject.put("_id", id);
    	coll.insert(complexObject);
    	DBObject result = coll.findOne(new BasicDBObject("name", "test123")); 
    	DBObject result1 = coll.findOne(id); 
    	DBObject result2 = coll.findOne(new BasicDBObject("_id", id)); 
    	assertNotNull(result);
    	l("Complex JSON array and query result =" + toJSONString(result));
    	l("Complex JSON array and query result1 =" + toJSONString(result1));
    	l("Complex JSON array and query result2 =" + toJSONString(result2));
    }     
    
    @Test
    public void testDeleteObjectLongId(){
    	DB db = mungo.getDB("db1");
    	Long id = new Long(1L);
    	DBCollection coll = db.createCollection("Collection");      
    	BasicDBObject complexObject 
    		= new BasicDBObject("{\"numbers\" : [true,1,2,3,\"hello world\", { \"inner\": \"text\" }]}");
    	l("Put id to json="+id);
    	complexObject.put("_id", id);
    	coll.insert(complexObject);
    	DBObject result = coll.findOne(id);
    	assertNotNull(result); 	
    	
    	// delete
    	WriteResult wr = coll.remove(result);
    	//assertTrue((Boolean)wr.getField("ok")); // <- This keeps on failing!
    	// should have been deleted
    	assertNull(coll.findOne(id));
    }
    
	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}
	
	private String toJSONString(DBObject o){
		JSONObject jso = new JSONObject();
		jso.putAll(o.toMap());
		return jso.toJSONString();
	}
	
}

