package com.mungods;

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
import com.mungods.object.GAEObject;
import com.mungods.object.ObjectStore;

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

		DBCursor cursor = new DBCursor(coll, 
				null, // Reference object
				null);// Keys to include 
		
		List<DBObject> list = copyIterator(cursor);
		printList(list);
		assertEquals(11, list.size());
		
		
		cursor = new DBCursor(coll, 
				BasicDBObjectBuilder.start("hi", "there").get(), 
				null).sort(BasicDBObjectBuilder.start("number", -1).get());  

		list = copyIterator(cursor);
		printList(list);
		assertEquals(10, list.size());
		
		/*
		cursor = new DBCursor(coll, null, null).skip(5); 
		list = copyIterator(cursor); 
		printList(list);
		assertEquals(5, list.size());
	
		cursor = new DBCursor(coll, null, null).limit(5); 
		list = copyIterator(cursor); 
		printList(list);
		assertEquals(5, list.size());
	
		cursor = new DBCursor(coll, null, null).limit(1); 
		list = copyIterator(cursor); 
		printList(list);
		assertEquals(1, list.size());
		
		// FIXME - Fails because the iterator is limited to 1 element
		// while trying to skip 5 elements

		cursor = new DBCursor(coll, null, null).skip(5).limit(1); 
		list = copyIterator(cursor); 
		printList(list);
		//assertEquals(1, list.size());

		cursor = new DBCursor(coll, null, null).limit(5).skip(1); 
		list = copyIterator(cursor); 
		printList(list);
		//assertEquals(1, list.size());
		*/
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
