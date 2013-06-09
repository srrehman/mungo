package com.mungoae;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

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
		BasicDBObject obj = new BasicDBObject("{'username' : 'xirby'}");
		assertNotNull(obj.get("username")); 
	}
	
	@Test
	public void testCreateQueryFromString() {
		BasicDBObject query = new BasicDBObject("{'$inc': { 'number': 1}}");
		assertNotNull(query.get("$inc")); 
	}

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}	
	
}
