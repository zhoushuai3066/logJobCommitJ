package com.busap.logJobCommitJ.model;

import java.io.Serializable;

public class NewUser  implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String userId;
    private String date;
    
    
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
    
    

}
