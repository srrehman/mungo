package com.mungoae;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.labs.repackaged.com.google.common.collect.Lists;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.mungoae.BasicDBObject;
import com.mungoae.DBCollection;
import com.mungoae.DBCursor;
import com.mungoae.DBObject;
import com.mungoae.Mungo;
/**
 * A very crud test
 * 
 * @author kerby
 *
 */
public class DBCursorTest {
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
	public void test() {
		
		Mungo mungo = new Mungo();
		DBCollection coll = mungo.getDB("TestDB").getCollection("DBCursorTest");
		
		persistTestData();
		
		assertNotNull(coll);

		Iterable<DBObject> cursor = new DBCursor(coll, 
				null, // Reference object
				null);// Keys to include 
		 
		List asList = Lists.newArrayList(cursor);
		printList(asList);
		assertEquals(11, asList.size());
		
		
		BasicDBObject query = new BasicDBObject("number", new BasicDBObject("$gte", 1));
		cursor = new DBCursor(coll, 
				query, null).sort(new BasicDBObject("number", 1));  
		asList = Lists.newArrayList(cursor);
		printList(asList);
		assertEquals(11, asList.size());
		
		query = new BasicDBObject("number", new BasicDBObject("$gte", 5));
		cursor = new DBCursor(coll, 
				query, null).sort(new BasicDBObject("number", -1));  
		asList = Lists.newArrayList(cursor);
		printList(asList);
		assertEquals(7, asList.size());

		query = new BasicDBObject("hi", new BasicDBObject("$e", "dont exist"));
		cursor = new DBCursor(coll, 
				query, null).sort(new BasicDBObject("hi", -1));  
		asList = Lists.newArrayList(cursor);
		printList(asList);
		assertEquals(0, asList.size());
		
		
		/**
		 * Limit and skip tests
		 */
		
		cursor = new DBCursor(coll, 
				new BasicDBObject("number", new BasicDBObject("$gte", 1)), 
				null).sort(new BasicDBObject("number", -1)).limit(5).skip(1);  
		
		asList = Lists.newArrayList(cursor);
		printList(asList);
		//assertEquals(11, list.size());
		
		cursor = new DBCursor(coll, null, null).sort(new BasicDBObject("number", 1)).limit(5).skip(1);
		asList = Lists.newArrayList(cursor);
		printList(asList);
		
		cursor = new DBCursor(coll, null, null).sort(new BasicDBObject("number", -1)).limit(3); 
		asList = Lists.newArrayList(cursor);
		printList(asList);
		
		coll.insert(new BasicDBObject("hi", "there").append("number", 12)); 
		
		cursor = new DBCursor(coll, null, null).sort(new BasicDBObject("number", -1)).limit(3); 
		asList = Lists.newArrayList(cursor);
		printList(asList);
		
		// Query by string
		
		cursor = new DBCursor(coll, null, null).sort(new BasicDBObject("number", -1)).limit(3); 
		asList = Lists.newArrayList(cursor);
		printList(asList);
	}
	
	public void persistTestData(){
		
		Mungo mungo = new Mungo();
		DBCollection coll = mungo.getDB("TestDB").getCollection("DBCursorTest");
		
		coll.insert(new BasicDBObject("username", "abby").append("number", 1)); 
		coll.insert(new BasicDBObject("username", "bobby").append("number", 2));
		coll.insert(new BasicDBObject("username", "cubby").append("number", 3));
		coll.insert(new BasicDBObject("username", "dubby").append("number", 4));
		coll.insert(new BasicDBObject("username", "fubby").append("number", 5));
		coll.insert(new BasicDBObject("username", "gubby").append("number", 6));
		coll.insert(new BasicDBObject("username", "hubby").append("number", 7));
		coll.insert(new BasicDBObject("username", "ibby").append("number", 8));
		coll.insert(new BasicDBObject("username", "jibby").append("number", 9));
		coll.insert(new BasicDBObject("username", "kibby").append("number", 10));
		coll.insert(new BasicDBObject("username", "libby").append("number", 11));
	}
	
	private void printList(List<DBObject> list){
		l("======================================================================");
		for (DBObject o : list){
			l(o);
		}
		l("======================================================================");
	}
 
	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}	
	
}
