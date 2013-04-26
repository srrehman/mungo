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
import com.pagecrumb.joongo.collection.DBObject;
import com.pagecrumb.joongo.collection.WriteResult;
import com.pagecrumb.joongo.entity.BasicDBObject;

public class JoongoTest {
	
    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
                .setDefaultHighRepJobPolicyUnappliedJobPercentage(0)); 	   

    private Joongo go;
    
    @Before
    public void setUp() {
        helper.setUp(); 
        go = new Joongo();
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
    	DB db1 = go.getDB("db1");
    	DB db2 = go.getDB("db2");
    	
    	assertNotNull(db1);
    	assertNotNull(db2);
    	
    	DBCollection users = db1.createCollection("users");
    	DBCollection pages = db1.createCollection("pages");
    	DBCollection messages = db1.createCollection("messages");
    	
    	assertEquals(3, db1.countCollections());
    	assertEquals(0, db2.countCollections());
    	
    	assertNotNull(users);
    	assertEquals("users",users.getName());
    
    	BasicDBObject obj = new BasicDBObject();
    	BasicDBObject embedded = new BasicDBObject();
    	
    	obj.put("hi", "there");
    	embedded.put("hello", embedded);
    	obj.put("embedded", embedded);
    	
    	assertTrue(obj.get("hi") instanceof String);
    	assertTrue(obj.get("embedded") instanceof DBObject);
    	
    	ObjectId id = new ObjectId();
    	obj.put("_id", id);
    	
//    	WriteResult result = messages.insert(obj);
//    	DBObject msg = messages.findOne(id);
    	
//    	assertNotNull(msg);
    	
//    	l(msg.toJSONString());
    	
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
    	//assertNotNull(result2);
    	
    	l(result.toJSONString());
    	l(result2.toJSONString());
    	
    }

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}
	
}

