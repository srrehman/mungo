Mungo
=========

Mungo is a "document" interface to the App Engine Datastore which introduces the concept of DB, DBCollection, and DBObject.
It is designed to store JSON-documents into the Datastore in the most native way possible.

  - It's a work in progress where changes are rapid
  - It makes storing JSON objects easy
  - Easy to use

Mungo, does not try to create a "yet another object mapping" framework. What it provides is just a wire-up to allow the easy storing of JSON objects or Maps 
by mapping to native Datastore entities and not serializing the document.  

The goal is to be able to store JSON documents and its sub-documents as flat as possible. Allowing smooth queries to 
deep stored embedded documents (Work in progress).

>    	Mungo mungo = new Mungo(); 
>    	DB testDB = mungo.getDB("testDB");
>		DBCollection greetings = testDB.createCollection("Message");
>    	BasicDBObject obj = new BasicDBObject("{'username' : 'jack'}")
>			obj.put("greeting", "Hello world")
>    		obj.put("created", new Date());
> 		WriteResult wr = greetings.insert(obj); // Done!
> 		DBObject greeting = greetings.findOne("{'username' : 'jack'}"); // Get it

Or

>		Greeting greeting = greetings.findOne("{'username' : 'jack'}").as(Greeting.class);

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
