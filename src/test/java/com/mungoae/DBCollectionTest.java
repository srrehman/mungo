package com.mungoae;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.mungoae.BasicDBObject;
import com.mungoae.BasicDBObjectBuilder;
import com.mungoae.DBCollection;
import com.mungoae.DBObject;
import com.mungoae.Mungo;
import com.mungoae.QueryOpBuilder;
import com.mungoae.util.Tuple;

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
        persistTestData();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }	
    
    @Test
    public void testFindOne(){
    	DBObject findOne = coll.findOne();
    	assertNotNull(findOne);
    }
	
	@Test
	public void testFindSortDescending() {
        QueryOpBuilder builder = new QueryOpBuilder()
    		.addQuery(BasicDBObjectBuilder.start("number", new BasicDBObject("$gte", 1)).get())
    		.addOrderBy(BasicDBObjectBuilder.start("number", -1).get()); 
		
		Iterator<DBObject> it = coll.__find(builder.get(), null, 0, 0, 0, 0);
		assertNotNull(it);
		
		List<DBObject> list = copyIterator(it);
		assertFalse(list.isEmpty());
		assertEquals(11L, list.get(0).get("number"));
		printList(list);
	
	}
	
	@Test
	public void testFindSortDescending2() { 
		
		Map<String, Tuple<FilterOperator, Object>> filters = new HashMap<String, Tuple<FilterOperator,Object>>();
		Map<String, SortDirection> sorts = new HashMap<String, SortDirection>();
		sorts.put("number", SortDirection.DESCENDING);
		
		Iterator<DBObject> it = coll.__find(filters, sorts, null, 5, null, null);
		assertNotNull(it);
		
		List<DBObject> list = copyIterator(it);
		assertFalse(list.isEmpty());
		assertEquals(11L, list.get(0).get("number"));
		printList(list);
		
		coll.insert(new BasicDBObject("hi", "there").append("number", 11));
		it = coll.__find(filters, sorts, null, 5, null, null);
		assertNotNull(it);
		list = copyIterator(it);
		assertFalse(list.isEmpty());
		assertEquals(11L, list.get(0).get("number"));
		printList(list);
		
		DBCursor curr1 = coll.find().sort("number", DBCursor.SortDirection.DESCENDING).limit(3).now(); 
		assertNotNull(curr1);
		list = copyIterator(curr1);
		assertFalse(list.isEmpty());
		assertEquals(11L, list.get(0).get("number"));
		printList(list);		
	}
	
	@Test
	public void testFindSortAscending() {
        QueryOpBuilder builder = new QueryOpBuilder()
    		.addQuery(BasicDBObjectBuilder.start("number", new BasicDBObject("$gte", 1)).get())
    		.addOrderBy(BasicDBObjectBuilder.start("number", 1).get()); 
		
		Iterator<DBObject> it = coll.__find(builder.get(), null, 0, 0, 0, 0);
		assertNotNull(it);
		
		List<DBObject> list = copyIterator(it);
		assertFalse(list.isEmpty());
		assertEquals(1L, list.get(0).get("number"));
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
		coll.insert(new BasicDBObject("hi", "there").append("number", 11));
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
