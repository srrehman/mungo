package com.mungoae.models;

import com.mungoae.annotations.Entity;
import com.mungoae.annotations.Id;

@Entity
public class User {
	@Id
	private String id;
	private String username;
	public User(){}
	public User(String username){
		this.username = username;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

}