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

public class DBCollectionTest {
    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
                .setDefaultHighRepJobPolicyUnappliedJobPercentage(0)); 	   	

    Mungo mungo;
    DBCollection coll;
    
    @Before
    public void setUp() {
        helper.setUp();
        mungo = new Mungo();
        coll = mungo.getDB("TestDB").getCollection("TestCollection");
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }	
	
	@Test
	public void testFind() {
		persistTestData();
		
        QueryOpBuilder builder = new QueryOpBuilder()
    		.addQuery(BasicDBObjectBuilder.start("hi", "there").get())
    		.addOrderBy(BasicDBObjectBuilder.start("number", -1).get());
		
		Iterator<DBObject> it = coll.__find(builder.get(), null, 0, 0, 0, 0);
		assertNotNull(it);
		
		List<DBObject> list = copyIterator(it);
		assertFalse(list.isEmpty());
		printList(list);
		
	}
	
	public void persistTestData(){		
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
