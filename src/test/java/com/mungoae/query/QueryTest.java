package com.mungoae.query;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.Iterator;

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
import com.mungoae.DBCollection;
import com.mungoae.DBCursor.SortDirection;
import com.mungoae.collection.simple.BasicMungoCollection;

public class QueryTest {
    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
                .setDefaultHighRepJobPolicyUnappliedJobPercentage(0)); 	   	
    Mungo mungo;
    DBCollection coll;
    
    @Before
    public void setUp() {
        helper.setUp();
        mungo = new Mungo();
        coll = new BasicMungoCollection(mungo.getDB("db"), "TestCollection");
        persistTestData();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }	
	
	@Test
	public void testQueryNumbers() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Query Number >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		DBCursor result = coll.find().filter("number").lessThanOrEqualTo(1).now();
		while (result.hasNext()){
			DBObject obj = result.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Query Number <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}
	
	@Test
	public void testQueryStrings() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Query Strings >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		DBCursor result = coll.find()
				.filter("username").greaterThanOrEqualTo("a").sort(SortDirection.DESCENDING).limit(5).skip(1).now();  
		while (result.hasNext()){
			DBObject obj = result.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Query Strings <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}	
	
	@Test
	public void testQueryDate() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Query Date >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		DBCursor result = coll.find().sort("created", SortDirection.DESCENDING).now();  
		while (result.hasNext()){
			DBObject obj = result.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Query Date <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}	
	
	@Test
	public void testQueryNumber() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Query Number >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		DBCollection coll = mungo.getDB("db").getCollection("DateQueryCollection");
		DBCursor result = coll.find().filter("numeber").greaterThanOrEqualTo(1).sort("number", SortDirection.DESCENDING).now();  
		while (result.hasNext()){ 
			DBObject obj = result.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Query Number <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}

	@Test
	public void testQueryMuti() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Query Multi >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		DBCursor result = coll.find().filter("number").equalTo(11).now();
		while (result.hasNext()){
			DBObject obj = result.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Query Multi <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}	
	
	@Test
	public void testQueryByIntegerID() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Query By ID >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		DBCursor result = coll.find().filter("_id").equalTo(123).now();
		while (result.hasNext()){
			DBObject obj = result.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Query By ID <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}
	
	@Test
	public void testQueryByStringID() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Query By String ID >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		DBCursor result = coll.find().filter("_id").equalTo(456).now();
		while (result.hasNext()){
			DBObject obj = result.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Query By String ID <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}
	
	@Test
	public void testUpdateQuery() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Update query >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		coll.update("{'username': 'kirby'}").with("{ '$set' : {'number': 123}}").now();
		coll.update("{'username': 'kirby'}").with("{ '$rename' : {'number': 'count'}}").now();
		DBCursor result = coll.find().now();
		while (result.hasNext()){
			DBObject obj = result.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Update query <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");		
	}
	
	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}

	public void persistTestData(){		
		Date baseDate = new Date();
		coll.insert(new BasicDBObject("username", "amanda").append("number", 1).append("created", new Date(baseDate.getTime() + 1*60*60*1000))); 
		coll.insert(new BasicDBObject("username", "bill").append("number", 5).append("created", new Date(baseDate.getTime() + 2*60*60*1000)));
		coll.insert(new BasicDBObject("username", "cathy").append("number", 6).append("created", new Date(baseDate.getTime() + 3*60*60*1000)));
		coll.insert(new BasicDBObject("username", "dennis").append("number", 2).append("created", new Date(baseDate.getTime() + 4*60*60*1000)));
		coll.insert(new BasicDBObject("username", "edward").append("number", 3).append("created", new Date(baseDate.getTime() + 5*60*60*1000)));
		coll.insert(new BasicDBObject("username", "faye").append("number", 4).append("created", new Date(baseDate.getTime() + 6*60*60*1000)));
		coll.insert(new BasicDBObject("username", "georgy").append("number", 7).append("created", new Date(baseDate.getTime() + 7*60*60*1000)));
		coll.insert(new BasicDBObject("username", "hanah").append("number", 8).append("created", new Date(baseDate.getTime() + 8*60*60*1000)));
		coll.insert(new BasicDBObject("username", "irwin").append("number", 9).append("created", new Date(baseDate.getTime() + 9*60*60*1000)));
		coll.insert(new BasicDBObject("username", "jack").append("number", 10).append("created", new Date(baseDate.getTime() + 10*60*60*1000)));
		coll.insert(new BasicDBObject("username", "kirby").append("number", 11).append("created", new Date(baseDate.getTime() + 11*60*60*1000)));
		coll.insert(new BasicDBObject("username", "lizi").append("number", 11).append("created", new Date(baseDate.getTime() + 12*60*60*1000)));
		//coll.insert("{'username':'mary', 'number' : 12, 'created':'Jun 9, 2013 6:13:05 PM'}");
		coll.insert("{'username':'mary', 'number' : 12, 'created':'Jun 9, 2013 6:13:05 PM'}");
		coll.insert(new BasicDBObject("username", "nancy").append("number", 13)
				.append("_id", 123)
				.append("created", new Date(baseDate.getTime() + 14*60*60*1000)));
		coll.insert(new BasicDBObject("username", "oliver").append("number", 14)
				.append("_id", "456")
				.append("created", new Date(baseDate.getTime() + 15*60*60*1000)));

	}
	
}
