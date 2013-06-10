package com.mungoae;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

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

		DBCursor cursor = new DBCursor(coll, 
				null, // Reference object
				null);// Keys to include 
		
		List<DBObject> list = copyIterator(cursor);
		printList(list);
		assertEquals(11, list.size());
		
		
		BasicDBObject query = new BasicDBObject("number", new BasicDBObject("$gte", 1));
		cursor = new DBCursor(coll, 
				query, null).sort(new BasicDBObject("number", -1));  
		list = copyIterator(cursor);
		printList(list);
		assertEquals(11, list.size());
		
		query = new BasicDBObject("number", new BasicDBObject("$gte", 5));
		cursor = new DBCursor(coll, 
				query, null).sort(new BasicDBObject("number", -1));  
		list = copyIterator(cursor);
		printList(list);
		assertEquals(7, list.size());

		query = new BasicDBObject("hi", new BasicDBObject("$e", "dont exist"));
		cursor = new DBCursor(coll, 
				query, null).sort(new BasicDBObject("hi", -1));  
		list = copyIterator(cursor);
		printList(list);
		assertEquals(0, list.size());
		
		
		/**
		 * Limit and skip tests
		 */
		
		cursor = new DBCursor(coll, 
				new BasicDBObject("number", new BasicDBObject("$gte", 1)), 
				null).sort(new BasicDBObject("number", -1)).limit(5).skip(1);  
		
		list = copyIterator(cursor);
		printList(list);
		//assertEquals(11, list.size());
		
		cursor = new DBCursor(coll, null, null).sort(new BasicDBObject("number", 1)).limit(5).skip(1);
		list = copyIterator(cursor);
		printList(list);
		
		cursor = new DBCursor(coll, null, null).sort(new BasicDBObject("number", -1)).limit(3); 
		list = copyIterator(cursor);
		printList(list);
		
		coll.insert(new BasicDBObject("hi", "there").append("number", 12)); 
		
		cursor = new DBCursor(coll, null, null).sort(new BasicDBObject("number", -1)).limit(3); 
		list = copyIterator(cursor);
		printList(list);
		
		// Query by string
		
		cursor = new DBCursor(coll, null, null).sort(new BasicDBObject("number", -1)).limit(3); 
		list = copyIterator(cursor);
		printList(list);
	}
	
	public void persistTestData(){
		
		Mungo mungo = new Mungo();
		DBCollection coll = mungo.getDB("TestDB").getCollection("DBCursorTest");
		
		coll.insert(new BasicDBObject("hi", "there").append("number", 1)); 
		coll.insert(new BasicDBObject("hi", "there").append("number", 2));
		coll.insert(new BasicDBObject("hi", "there").append("number", 3));
		coll.insert(new BasicDBObject("hi", "there").append("number", 4));
		coll.insert(new BasicDBObject("hi", "there").append("number", 5));
		coll.insert(new BasicDBObject("hi", "there").append("number", 6));
		coll.insert(new BasicDBObject("hi", "there").append("number", 7));
		coll.insert(new BasicDBObject("hi", "there").append("number", 8));
		coll.insert(new BasicDBObject("hi", "there").append("number", 9));
		coll.insert(new BasicDBObject("hi", "there").append("number", 10));
		coll.insert(new BasicDBObject("whats", "up").append("number", 11));
	}
	
	List<DBObject> copyIterator(Iterator<DBObject> it){
		List<DBObject> copy = new ArrayList<DBObject>();
		while (it.hasNext()){
		    copy.add(it.next());
		}
		return copy;
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
