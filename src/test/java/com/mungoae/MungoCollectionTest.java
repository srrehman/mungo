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

import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.labs.repackaged.com.google.common.collect.Lists;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.mungoae.collection.simple.BasicMungoCollection;
import com.mungoae.models.Friend;
import com.mungoae.util.JSON;

public class MungoCollectionTest {
    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
                .setDefaultHighRepJobPolicyUnappliedJobPercentage(0)); 	   	

    Mungo mungo;
	DB db;
	DBCollection friends;
    @Before
    public void setUp() {
        helper.setUp();
        mungo = new Mungo();
        db = mungo.getDB("db");
        friends = new BasicMungoCollection(db, "friends");
        persistTestData();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }	
	
	@Test
	public void testSetup() {		
		// Setup
		Iterable<DBObject> allObject = friends.find("{name: 'Joe'}").sort("name", com.mungoae.DBCursor.SortDirection.ASCENDING); 
		assertNotNull(allObject); 
		List<DBObject> oneList = Lists.newArrayList(allObject);
		l(oneList);
		assertEquals(1, oneList.size());
		
		
		Iterable<Friend> all = friends.find("{name: 'Joe'}").as(Friend.class);
		assertNotNull(all);
		List<Friend> list = Lists.newArrayList(all);
		l(list);
		assertEquals(1, list.size());
		
		
		Iterable<DBObject> _all = friends.find();
		List<DBObject> _list = Lists.newArrayList(_all);
		assertFalse(_list.isEmpty());
		l(_list);
		
		all = friends.find("{name: 'Kirby'}").now().as(Friend.class); 
		assertNotNull(all);
		list = Lists.newArrayList(all);
		l(list);
		assertFalse(list.isEmpty());
		assertEquals(4, list.size());
	}
	
	@Test
	public void testSave() {
		// Save
//		Friend joe = new Friend("Joe", 27);
//		friends.save(joe);
//		joe.age = 28;
//		friends.save(joe);
	}
	
	@Test
	public void testUpdate() {
		// Update
//		friends.update("{name: 'Joe'}").increment("age", 1);
//		friends.update("{name: 'Joe'}").upsert().multi().increment("age", 1);
	
		
		//friends.update("{name: 'Joe'}").with(new Friend("Joe" , 27));
		//friends.update("{name: 'Joe'}").with("{$set: {address: #}}", new Friend.Address("Blk 1, Lot 2, Greenland Subd., Pines City"));
	
	}
	
	@Test
	public void testInsert() {
		// Insert
//		friends.insert("{name: 'Joe', age: 18}");
//		friends.insert(new Friend("Joe", 27));
//		friends.insert(new Friend("Joe", 27), new Friend("Jack", 26));
	}
	
	@Test
	public void testQuery() {
		
		DBObject dbo = friends.findOne("{name: 'Joe'}");
		assertNotNull(dbo);
		assertTrue(friends.findOne("{name: 'Joe'}") instanceof DBObject);
		assertEquals("Joe", dbo.get("name")); 

		Friend one = friends.findOne("{name: 'Joe'}").as(Friend.class);		
		assertNotNull(one);
		assertTrue(friends.findOne("{name: 'Joe'}").as(Friend.class) instanceof Friend);
		assertEquals("Joe", one.getName());

		Iterable<Friend> all = friends.find().filter("name").equalTo("Kirby").as(Friend.class);
		assertNotNull(all);
		List<Friend> list = Lists.newArrayList(all);
		assertTrue(list.size() == 4);
		assertTrue(list.get(0).getName().equals("Kirby"));
		l(list);
		
		//DBObject _one = friends.findOne("{ '_id' : '8888' }");
		DBObject _one = friends.findOne("{ age : 28 }");
		l(_one);
		assertNotNull(_one);
		assertEquals("Kirby", _one.get("name"));
		assertEquals(28L, _one.get("age"));
		assertNotNull(_one.get("address"));
		
	}
	
	@Test
	public void testNotExistAndRemove() {
		DBObject one = friends.findOne("{ name : 'Pete' }");
		assertNull(one);
		
		// Remove
//		friends.remove("{name: 'Joe'}");
//		friends.remove(new ObjectId());
	}
	

	
	private void persistTestData(){
		DBCollection friends = new BasicMungoCollection(db, "friends");
		friends.insert("{'name': 'Joe'}");
		friends.insert("{'name': 'Kirby'}");
		friends.insert("{'name': 'Kirby', 'age': 27 }");
		friends.insert("{name: 'Kirby', age: 27, address : { address: 'somewhere over the rainbow' } }");
		friends.insert("{_id: '888', name: 'Kirby', age: 28, address : { address: 'somewhere over the rainbow' } }");
//		friends.insert(new BasicDBObject("{'name': 'Kirby', 'age': 28, 'address' : { 'address': 'somewhere over the rainbow' } }")
//				.append("_id", new BasicDBObject("51b6125923185d39fe416f94")));  
		// oid = "51b6125923185d39fe416f94"
//		friends.insert(BasicDBObjectBuilder.start("_id", new ObjectId()).add("name", "Kirtby")
//				.add("age", 28).add("address", new BasicDBObject("adress", "there")).get()); 
		
		
	}

	private void l(Object log){
		System.out.print("=================================================\n");
		System.out.print(String.valueOf(log) + "\n"); 
		System.out.print("=================================================\n");
	}	
	
}
