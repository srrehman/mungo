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
import com.mungoae.DBObject;
import com.mungoae.Mungo;
import com.mungoae.query.Query;
import com.mungoae.query.Query.SortDirection;

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
        coll = mungo.getDB("TestDB").getCollection("TestCollection");
        persistTestData();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }	
	
	@Test
	public void testQueryNumbers() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Query Number >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		Query q = mungo.query("TestDB", "TestCollection");
		Iterator<DBObject> it = q.filter("number").lessThanOrEqualTo(1).now();
		while (it.hasNext()){
			DBObject obj = it.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Query Number <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}
	
	@Test
	public void testQueryStrings() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Query Strings >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		Query q = mungo.query("TestDB", "TestCollection");
		Iterator<DBObject> it = q.filter("username").greaterThanOrEqualTo("a").sort(SortDirection.DESCENDING).limit(5).skip(1).now();  
		while (it.hasNext()){
			DBObject obj = it.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Query Strings <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}	
	
	@Test
	public void testQueryDate() {
		l(">>>>>>>>>>>>>>>>>>>>>>>>> Test Query Date >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		Query q = mungo.query("TestDB", "TestCollection");
		Iterator<DBObject> it = q.sort("created", SortDirection.DESCENDING).now();  
		while (it.hasNext()){
			DBObject obj = it.next();
			l(obj);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<< Test Query Date <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
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
	}
	
}
