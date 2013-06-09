package com.mungoae;

import static org.junit.Assert.*;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.mungoae.collection.simple.BasicMungoCollection;

public class MungoCollectionTest {
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
		MungoCollection friends = new BasicMungoCollection("myapp", "friends");
		//friends.find().filter("username").equalTo("kite").now().as(String.class); 
		
		// Setup
		Iterable<Friend> all = friends.find("{name: 'Joe'}").as(Friend.class);
		Friend one = friends.findOne("{name: 'Joe'}").as(Friend.class);		
		
		all = friends.find().filter("name").equalTo("Joe").as(Friend.class);
		
		// Save
		Friend joe = new Friend("Joe", 27);
		friends.save(joe);
		joe.age = 28;
		friends.save(joe);
		
		// Update
		friends.update("{name: 'Joe'}").increment("age", 1);
		friends.update("{name: 'Joe'}").upsert().multi().increment("age", 1);
	
		
		//friends.update("{name: 'Joe'}").with(new Friend("Joe" , 27));
		//friends.update("{name: 'Joe'}").with("{$set: {address: #}}", new Friend.Address("Blk 1, Lot 2, Greenland Subd., Pines City"));
		
		// Insert
		friends.insert("{name: 'Joe', age: 18}");

		friends.insert(new Friend("Joe", 27));
		friends.insert(new Friend("Joe", 27), new Friend("Jack", 26));
		
		// Remove
		friends.remove("{name: 'Joe'}");
		friends.remove(new ObjectId());
		
	}

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}	
	
}
