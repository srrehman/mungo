package com.mungoae;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.mungoae.collection.simple.BasicMungoCollection;
import com.mungoae.query.DBQuery;

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
	
	//@Test
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
	
	@Test
	public void testMungoCollection() {
		
		persistTestData();
		
		MungoCollection friends = new BasicMungoCollection("myapp", "friends");
		// Setup
		Iterable<DBObject> _all = friends.find();
		assertTrue(copyIterable(_all).isEmpty() == false);
		
		Iterable<Friend> all = friends.find("{name: 'Kirby'}").now().as(Friend.class); 
		assertTrue(copyIterable(all).isEmpty() == false);
		
		DBObject one = friends.findOne("888");
		l("Find one result="+one);
	}
	
	private void persistTestData(){
		Mungo mungo = new Mungo();
		MungoCollection friends = new BasicMungoCollection("myapp", "friends");
		friends.insert("{name: 'Joe'}");
		friends.insert("{name: 'Kirby'}");
		friends.insert("{name: 'Kirby', age: 27 }");
		friends.insert("{name: 'Kirby', age: 27, address : { address: 'somewhere over the rainbow' } }");
		friends.insert("{_id: '888', name: 'Kirby', age: 27, address : { address: 'somewhere over the rainbow' } }");
	}
	
	List<DBObject> copyIterator(Iterator<DBObject> it){
		List<DBObject> copy = new ArrayList<DBObject>();
		while (it.hasNext()){
		    copy.add(it.next());
		}
		return copy;
	}
	
	<T> List<T> copyIterable(Iterable<T> it){
		l(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		List<T> copy = new ArrayList<T>();
		for (T o : it){
			copy.add(o);
			l(o);
		}
		l("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		return copy;
	}

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}	
	
}
