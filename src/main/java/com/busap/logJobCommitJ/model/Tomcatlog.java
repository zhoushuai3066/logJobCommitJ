package com.busap.logJobCommitJ.model;

import java.io.Serializable;

import com.busap.logJobCommitJ.table.Table;

public class Tomcatlog  implements Serializable {
	

	private static final long serialVersionUID = 1L;
	
	private String date;
	private int userId;
	private String province;
	private String city;
	private String platform;
	private String version;
	private String channel;
	private String operationMain;
	private String operationSlave;
	private String extend;
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	public String getProvince() {
		return province;
	}
	public void setProvince(String province) {
		this.province = province;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getChannel() {
		return channel;
	}
	public void setChannel(String channel) {
		this.channel = channel;
	}
	public String getOperationMain() {
		return operationMain;
	}
	public void setOperationMain(String operationMain) {
		this.operationMain = operationMain;
	}
	public String getOperationSlave() {
		return operationSlave;
	}
	public void setOperationSlave(String operationSlave) {
		this.operationSlave = operationSlave;
	}
	public String getExtend() {
		return extend;
	}
	public void setExtend(String extend) {
		this.extend = extend;
	}

}
