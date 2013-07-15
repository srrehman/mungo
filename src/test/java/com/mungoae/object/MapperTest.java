package com.mungoae.object;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bson.types.BasicBSONList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import sun.rmi.runtime.Log;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.mungoae.BasicDBObject;
import com.mungoae.DBObject;
import com.mungoae.models.Greeting;
import com.mungoae.models.User;
import com.mungoae.query.Logical;
import com.mungoae.query.Update;
import com.mungoae.query.Update.UpdateOperator;
import com.mungoae.util.Tuple;

public class MapperTest {
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
    public void testCreateFromObject() {
    	User user = new User();
    	user.setId("123");
    	user.setUsername("demo");
    	DBObject obj = Mapper.createFromObject(user);
    	assertNotNull(obj);
    	assertEquals("123", obj.get("_id"));
    	assertEquals("demo", obj.get("username"));
    }
	
	@Test
	public void testCreateDBObjectFromEntity() {
		Entity e = new Entity(KeyStructure.createKey("Greetings", "1"));  
		e.setProperty("message", "Hello world");
		DBObject obj = Mapper.createDBObjectFromEntity(e);
		assertEquals("Hello world", obj.get("message"));
	}
	
	@Test
	public void test() {
		BasicDBObject logic = new BasicDBObject("{ '$or' : [{ 'qty': { '$lt': 20 } }, { 'sale': true }] }");;

		assertNotNull(logic.get("$or"));
		assertTrue(logic.get("$or") instanceof List);
		DBObject or = (DBObject) logic.get("$or");
		assertTrue(or instanceof BasicBSONList);
		Iterator<Object> it = ((BasicBSONList)or).iterator();
		while(it.hasNext()){
			l(it.next());
		}
		assertTrue(or.get("0") instanceof DBObject);
		// [[first=OR, second=[[first=qty, second=[first=<, second=20]]]], 
		// [first=OR, second=[[first=sale, second=[first==, second=true]]]]]
		List<Tuple<Logical, List<Tuple<String, Tuple<FilterOperator, Object>>>>>  map 
			= Mapper.createLogicalQueryObjectFrom(logic);
		l(map);
	}
	
	//{ $inc: { field1: amount } } 
	@Test
	public void testCreateDBObjectUpdateFrom() {
		Map<String, Tuple<UpdateOperator, Object>> update = new HashMap<String, Tuple<UpdateOperator, Object>>();
		update.put("field1", new Tuple<Update.UpdateOperator, Object>(UpdateOperator.INCREMENT, 1));
		assertEquals("{\"$inc\":{\"field1\":1}}", Mapper.createDBObjectUpdateFrom(update).toString()); 
	}
	
	@Test
	public void testMapDBObjectToPOJO(){
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("greeting", "Hello world!");
		map.put("_id", "51cc93ad23187afa8e0a4433");
		Greeting greeting = Mapper.createTObject(Greeting.class, map);
		assertNotNull(greeting);
		assertEquals("Hello world!", greeting.getGreeting());
	}

	private void l(Object log){
		System.out.print(String.valueOf(log) + "\n"); 
	}	
	
}
