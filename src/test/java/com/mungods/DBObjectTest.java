package com.mungods;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class DBObjectTest {
    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
                .setDefaultHighRepJobPolicyUnappliedJobPercentage(0)); 	   	

    class Tweet extends BasicDBObject {
    	private static final long serialVersionUID = 1L;
    	public Tweet() {
    	    super();
    	  }
    	  public Tweet(DBObject base) {
    	    super();
    	    this.putAll(base);
    	  }
    }
    
    Mungo mungo;
    DBCollection collection;
    
    @Before
    public void setUp() {
        helper.setUp();
        mungo = new Mungo();
        collection = mungo.getDB("TestDB").getCollection("TestCollection"); 
        
        Tweet myTweet = new Tweet();
		myTweet.put("user", 1);
		myTweet.put("message", "Hello world!");
		myTweet.put("date", new Date());	
		collection.insert(myTweet);
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }	
	
	@Test
	public void test() {
			
	}
	
	@Test
	public void testFind(){
		collection.setObjectClass(Tweet.class);
		//Tweet myTweet = (Tweet)collection.findOne();
		DBObject myTweet = collection.findOne();
		assertNotNull(myTweet);
		l(myTweet);
	}
	
	@Test
	public void testSave(){
//		collection.setObjectClass(Tweet.class);
//		Tweet myTweet = (Tweet)collection.findOne();
//		myTweet.put("message", "Hi there!");
//		collection.save(myTweet);
		
		collection.setObjectClass(Tweet.class);
		BasicDBObject myTweet = (BasicDBObject)collection.findOne();
		myTweet.put("message", "Hi there!");
		collection.save(myTweet);
	}

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}	
	
}
