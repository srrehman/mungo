package com.mungoae.models;

public class Greeting {
	
	private String _id;
	private String greeting;
	
	public Greeting() {
		
	}

	public String getGreeting() {
		return greeting;
	}

	public void setGreeting(String greeting) {
		this.greeting = greeting;
	}

	public String getId() {
		return _id;
	}

	public void setId(String id) {
		this._id = id;
	}
}