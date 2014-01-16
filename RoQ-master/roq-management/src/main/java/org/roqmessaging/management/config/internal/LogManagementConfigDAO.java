package org.roqmessaging.management.config.internal;


public class LogManagementConfigDAO {

	private String hostAddress = "localhost";
	private String hostPort = "7000";
	private String directory = "/";
	private long maxFileSize = 1000L;
	private int duration = 50; // hours
	
	public String getHostAddress() {
		return hostAddress;
	}

	public void setHostAddress(String hostAddress) {
		this.hostAddress = hostAddress;
	}
	
	public String getHostPort() {
		return hostPort;
	}
	
	public void setHostPort(String hostPort) {
		this.hostPort = hostPort;
	}

	public String getDirectory() {
		return this.directory;
	}
	
	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public long getMaxFileSize() {
		return this.maxFileSize;
	}
	
	public void setMaxFileSize(long maxFileSize) {
		this.maxFileSize = maxFileSize;
	}

	public int getDuration() {
		return this.duration ;
	}
	public void setDuration(int duration) {
		this.duration = duration;
	}

}
