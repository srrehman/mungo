package com.pagecrumb.joongo;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
    	obj.put("hi", "there");
    	BasicDBObject embedded = new BasicDBObject();
    	embedded.put("hello", embedded);
    	obj.put("embedded", embedded);
    	
    	assertTrue(obj.get("hi") instanceof String);
    	assertTrue(obj.get("embedded") instanceof DBObject);
    	
    	String id = messages.createObject(obj);   	
    	l(id);
    }

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}
	
}

