package com.mungoae.models;

public class Friend {

	public class Address {
		private String address;

		public Address() {}
		
		public Address(String address){
			setAddress(address);
		}
		
		public String getAddress() {
			return address;
		}

		public void setAddress(String address) {
			this.address = address;
		}
		
		@Override
		public String toString() {
			return "[" + address + "]";
		}
	}
	
	public String _id;
	public String name;
	public Integer age;
	public Address address;

	public Friend() {}
	
	public Friend(String name, Integer age){
		setName(name);
		setAge(age);
	}
	
	public Address getAddress() {
		return address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}
	
	@Override
	public String toString() {
		return "Friend id=" + _id 
				+" name="+name
				+" age=" + age
				+" address=" + address;
	}
}
