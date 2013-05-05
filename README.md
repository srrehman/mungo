Mungo
=========

Mungo is a abstraction layer to the App Engine Datastore which introduces the concept of DB, DBCollection, and DBObject.
It is designed to allow the storing of JSON-documents in the most native way possible.

  - It's a work in progress where changes are rapid
  - It makes storing JSON objects easy
  - Easy to use
Joong
Mungo, does not try to create yet another "object mapping" framework or some sort. So it should not be compared to Objectify or Twig or anything like that. 
It just provides the Datastore a wire-up to allow the easy storing of JSON objects or Maps in a way that it is not serialized in some form.  

The goal is to be able to store JSON documents and its sub-documents as flat as possible. Allowing smooth queries to 
deep stored embedded documents (Work in progress).

>    	Mungo mungo = new Mungo(); 
>    	DB testDB = mungo.getDB("testDB");
>		DBCollection messages = testDB.createCollection("Message");
>    	BasicDBObject obj = new BasicDBObject("{\"hello\" : \"world\"}");
>    	obj.put("hi", "there");
> 		WriteResult wr = messages.insert(obj); // Done!
> 		DBObject result = messages.findOne(obj.getId()); // Get it

It's that easy!
Version
-

0.0.1

Tech
-----------

Mungo uses a number of open source projects to work properly:

* [GAE SDK] - SDK for the AppEngine platform (GAE or JBoss CapeDwarf)
* [JSON.Simple] - a simple Java toolkit for JSON. You can use JSON.simple to encode or decode JSON text.
* [Guice] - is a lightweight dependency injection framework for Java 5 and above 

Installation
--------------

```
mvn clean install
```

or add this to your POM:

    <repositories>
	    <repository>
	        <id>mungo-mvn-repo</id>
	        <url>https://raw.github.com/pagecrumb/mungo/mvn-repo/</url>
	        <snapshots>
	            <enabled>true</enabled>
	            <updatePolicy>always</updatePolicy>
	        </snapshots>
	    </repository>
    </repositories>
  

Dependency
--------------

        <dependency>
		  <groupId>com.pagecrumb</groupId>
		  <artifactId>mungo</artifactId>
		  <version>0.0.1-SNAPSHOT</version>		
		</dependency>

Contribution
--------------

* Anyone is welcome to contribute,  implement feature or fix bugs. 

License
-

Apache License, Version 2.0 
