package org.roqmessaging.log.reliability;

import java.util.Random;

public class PluginInfo {
	
	private String name = null;
	private String address = null;
	private int configPort = 0;
	private int dataPort = 0;
	private int counter = new Random().nextInt(10);//0;
	private long heartbeat = 0;
	
	public PluginInfo(String name, String address, int configPort, int dataPort) {
		this.name = name;
		this.address = address;
		this.configPort = configPort;
		this.dataPort = dataPort;
		this.heartbeat = System.currentTimeMillis();
	}
	
	public String getName() {
		return this.name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getAddress() {
		return this.address;
	}
	
	public void setAddress(String address) {
		this.address = address;
	}
	
	public int getConfigPort() {
		return this.configPort;
	}
	
	public void setConfigPort(int port) {
		this.configPort = port;
	}

	public int getDataPort() {
		return this.dataPort;
	}
	
	public void setDataPort(int port) {
		this.dataPort = port;
	}
	
	public int getCounter() {
		return this.counter;
	}
	
	public void setCounter(int counter) {
		this.counter = counter;
	}
	
	public void increaseCounter() {
		try {
			this.counter = this.counter + 1;
		}
		catch(Exception e) {
			System.err.println(e + "The maximum size of counter (Integer) is reached.");
		}
	}
	
	public void decreaseCounter() {
		if(counter > 0) {
			counter = counter - 1;
		}
		else {
			System.err.println("The minimum size (size=0) of counter is reached and canot be decreased anymore.");
		}
	}
}
