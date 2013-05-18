package com.mungods;

/**
 * Test entity
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 *
 */
public class Person extends BasicDBObject {
	private static final long serialVersionUID = 3749939344713359304L;
	private String firstname;
	private String lastname;
	
	public Person() {}
	
	public Person(String firstName, String lastName){
		put("firstname", firstName);
		put("lastname", lastName);
//		this.firstname = firstName;
//		this.lastname = lastName;
	}
	public String getFirstname() {
		return firstname;
	}
	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}
	public String getLastname() {
		return lastname;
	}
	public void setLastname(String lastname) {
		this.lastname = lastname;
	}
}
