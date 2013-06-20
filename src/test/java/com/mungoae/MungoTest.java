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
package com.mungoae;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.labs.repackaged.com.google.common.collect.Lists;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.mungoae.BasicDBObject;
import com.mungoae.DB;
import com.mungoae.DBCollection;
import com.mungoae.DBCursor;
import com.mungoae.DBObject;
import com.mungoae.Mungo;
import com.mungoae.models.Person;
import com.mungoae.util.JSON;

public class MungoTest {
	
    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
                .setDefaultHighRepJobPolicyUnappliedJobPercentage(0)); 	   
    
    private Mungo mungo;
    
    public class Greeting {
    	
    	private String _id;
    	private String greeting;

		public String getGreeting() {
			return greeting;
		}

		public void setGreeting(String greeting) {
			this.greeting = greeting;
		}

		public String getId() {
			return _id;
		}

		public void setId(String id) {
			this._id = id;
		}
    }
    
    
    @Before
    public void setUp() {
        helper.setUp(); 
        mungo = new Mungo();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }	
    
    
    // TODO - Query by ObjectId in a DBObject query or shell query does not work!
    @SuppressWarnings("unused")
	@Test
    public void testPersistComplexObjectQueryByProperty(){
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId("51b6cb8623185501bd24c036"); // for test reference
    	DBCollection coll = db.createCollection("Collection");       
    	BasicDBObject doc 
    		= new BasicDBObject("{'numbers' : [true,1,2,3,'hello world', { 'inner': 'text' }]}")
    			.append("name", "kiji8889"); 
    	doc.put("_id", id);
    	coll.insert(doc);
    	
    	DBObject oid = coll.findOne(id);
    	assertNotNull(oid);
    	assertEquals("kiji8889", oid.get("name"));
    	// TODO - ID must return the same object type when it is tored.
    	// E.g. ObjectId
//    	assertEquals("51b6cb8623185501bd24c036", ((ObjectId)oid.get("_id")).toStringMongod());
    	assertEquals("51b6cb8623185501bd24c036", ((String)oid.get("_id")));
    	
//    	DBObject oid1 = coll.findOne("{ '_id' : { '$oid' : '51b6cb8623185501bd24c036' } }");
//    	assertNotNull(oid1);
//    	assertEquals("kiji8889", oid1.get("name"));

    	
    	//Iterable<DBObject> oids = coll.find("{ '_id' : { '$oid' : '51b6cb8623185501bd24c036' } }");
    	//assertNotNull(oids);
    	//List<DBObject> oidList = Lists.newArrayList(oids);
    	//assertEquals(1, oidList.size());
    	
    	DBObject name = coll.findOne("{ 'name' : { '$e' : 'kiji8889' }}"); 
    	assertNotNull(name);
    	
    	//assertNotNull(oid1);
    	
    	assertNotNull(coll.findOne(new BasicDBObject("name", new BasicDBObject("$e", "kiji8889"))));
    	//assertNotNull(coll.findOne(new BasicDBObject("_id", new BasicDBObject("$e", id))));
    }      
    
    @Test
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
    	
    	l("Using findOne(): " + toJSONString(result));
    	l("Using findOne(): " + toJSONString(result));
    
    }
    
    @Test
    public void testCreateFromJSONString(){
    	BasicDBObject fromString = new BasicDBObject("{'hello' : 'world'}");
    	assertNotNull(fromString);
    	assertEquals("world", (String) fromString.get("hello"));
    	l("Test create from JSON String=" + fromString);
    }
 
    @Test
    public void testAsObject() {
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference, not necessary for actuall app
    	DBCollection greetings = db.createCollection("Greetings");
    	BasicDBObject fromString = new BasicDBObject("{'greeting' : 'good morning'}")
    		.append("_id", id); 
    	greetings.insert(fromString);
    	Greeting greeting = greetings.findOne(id).as(Greeting.class);
    	assertNotNull(greeting);
    	assertEquals("good morning", greeting.greeting);
    	assertEquals(id.toStringMongod(), greeting.getId());
    	l("Greeting greeting=" + greeting.getGreeting());
    }   
    
    @Test
    public void testDBCursor() {
    	
    	DB db = mungo.getDB("db1");
    	DBCollection greetings = db.createCollection("Greetings");
    	
    	ObjectId id = new ObjectId(); // for test reference
    	
    	BasicDBObject greet1 = new BasicDBObject("{'greeting' : 'kamusta ka'}")
    		.append("_id", id);  
    	BasicDBObject greet2 = new BasicDBObject("{'greeting' : 'good morning'}")
			.append("_id", new ObjectId());  
    	BasicDBObject greet3 = new BasicDBObject("{'greeting' : 'moshi moshi'}")
			.append("_id", new ObjectId());      	
    	
    	greetings.insert(greet1);
    	greetings.insert(greet2);
    	greetings.insert(greet3);
    	
    	// Query all
    	Iterable<DBObject> all = greetings.find();
    	List<DBObject> allList = Lists.newArrayList(all);
    	assertEquals("kamusta ka", allList.get(0).get("greeting"));    
    	assertEquals("good morning", allList.get(1).get("greeting"));
    	assertEquals("moshi moshi", allList.get(2).get("greeting"));
    	
    	// Query single
    	BasicDBObject query = new BasicDBObject("greeting", "good morning");
    	Iterable<DBObject> result = greetings.find(query);
    	List<DBObject> list = Lists.newArrayList(result);
    	assertEquals("good morning", list.get(0).get("greeting"));
    	
    	BasicDBObject greet4 = new BasicDBObject("{'greeting' : 'good morning'}")
    		.append("created", new Date(8889));  	
    	greetings.insert(greet4);
    	
    	Iterable<DBObject> created = greetings.find(new BasicDBObject("created", new Date(8889)));  
    	List<DBObject> createdList = Lists.newArrayList(created);
    	assertEquals("good morning", createdList.get(0).get("greeting"));

    	DBCollection mixedCollection = db.createCollection("Mixed");
    	BasicDBObject numberValue1 = new BasicDBObject("count", 123);
    	BasicDBObject numberValue2 = new BasicDBObject("count", 123);
    	BasicDBObject numberValue3 = new BasicDBObject("count", 456);
    	BasicDBObject booleanValue = new BasicDBObject("active", false);
    	
    	mixedCollection.insert(numberValue1);
    	mixedCollection.insert(numberValue2);
    	mixedCollection.insert(numberValue3);
    	mixedCollection.insert(booleanValue);
    	
    	BasicDBObject count = new BasicDBObject("count", 123);
    	assertTrue(mixedCollection.find(count).hasNext());
    	List countList = Lists.newArrayList((Iterable<DBObject>)mixedCollection.find(count));
    	assertTrue(countList.size() == 2);
    }
    
    @Test
    public void testPersistAndGetWithLongId(){
    	DB db = mungo.getDB("db1");
    	Long id = new Long(1L);
    	DBCollection coll = db.createCollection("Collection");      
    	BasicDBObject complexObject 
    		= new BasicDBObject("{'numbers' : [true,1,2,3,'hello world', { 'inner': 'text' }]}");
    	complexObject.put("_id", id);
    	coll.insert(new BasicDBObject("_id", id)); 
    	DBObject result = coll.findOne(id);
    	assertNotNull(result);
    }    
    
    @Test
    public void testQueryDocNotExist(){
    	DB db = mungo.getDB("db1");
    	DBCollection greetings = db.createCollection("Greetings");
    	
    	BasicDBObject notExist = new BasicDBObject("greeting", "that does not exist");
    	Iterable<DBObject> curr = greetings.find(notExist);
    	assertTrue(Lists.newArrayList(curr).size() == 0);
    }
    
    @Test
    public void testParseBasicDBObjectType(){
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection coll = db.createCollection("people");    	
    	
    	Person person = new Person("Feng", "Zhu");
    	person.append("_id", id);
    	coll.insert(person);
    	
    	BasicDBObject foundPerson = (BasicDBObject) coll.findOne(id); 
    	assertNotNull(foundPerson);
    }
    
    @Test
    public void testPersistJSONArrayNumberList(){
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection coll = db.createCollection("todo-list");      
    	BasicDBObject numbers = new BasicDBObject("{'numbers' : [1,2,3,4,5]}");
    	numbers.put("_id", id);
    	coll.insert(numbers);
    	DBObject result = coll.findOne(id);
    	assertNotNull(result);
    } 
    
    @Test
    public void testPersistJSONArrayListWithAnyObject(){
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection coll = db.createCollection("todo-list");      
    	BasicDBObject any = new BasicDBObject("{'any' : [true,1,2,3,'hello world']}");
    	any.put("_id", id);
    	coll.insert(any);
    	DBObject result = coll.findOne(id);
    	assertNotNull(result);
    }   
    
    @Test
    public void testPersistJSONArrayListWithEmbeddedObject(){
    	DB db = mungo.getDB("db1");
    	ObjectId id = new ObjectId(); // for test reference
    	DBCollection coll = db.createCollection("Collection");      
    	BasicDBObject any 
    		= new BasicDBObject("{'any' : [true,1,2,3,4,'hello world', { 'inner': 'text' }]}");
    	any.put("_id", id);
    	coll.insert(any);
    	DBObject complex = coll.findOne(id);
    	assertNotNull(complex);
    	assertTrue(complex.get("any") instanceof List);
    	List<Object> list = (List<Object>) complex.get("any");
    	assertEquals(7, list.size()); 
    	assertTrue((Boolean)list.get(0)); 
    	assertEquals(1L, list.get(1)); 
    	assertEquals(2L, list.get(2)); 
    	assertEquals(3L, list.get(3)); 
    	assertEquals(4L, list.get(4)); 
    	assertEquals("hello world", list.get(5));
    	assertTrue(list.get(6) instanceof Map);
    	assertEquals("text", ((Map)list.get(6)).get("inner"));
    }    
   
    
    @Test
    public void testDeleteObjectLongId(){
    	DB db = mungo.getDB("db1");
    	Long id = new Long(1L);
    	DBCollection coll = db.createCollection("Collection");      
    	coll.insert(new BasicDBObject("_id", id));
    	DBObject result = coll.findOne(id);
    	assertNotNull(result); 	
    	// delete
    	coll.remove(result);
    	// should have been deleted
    	assertNull(coll.findOne(id));
    }
    
	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}
	
	private String toJSONString(DBObject o){
		return JSON.serialize(o); 
	}
	
}

