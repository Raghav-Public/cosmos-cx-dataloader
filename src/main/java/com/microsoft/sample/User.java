package com.microsoft.sample;

public class User {
	
	private int userid;
	private String name;
	private String email;
	
	public int getUserId() {
		return userid;
	}
	
	public String getName() {
		return name;
	}
	
	public String getEmail() {
		return email;
	}
	
	public User(int uid, String name, String email) {
		this.email = email;
		this.name = name;
		this.userid = uid;
	}
	
	public String toString() {
		return this.userid + " | " + this.name + " | " + this.email;
	}

}
