package org.roqmessaging.log.reliability;

import java.util.HashMap;

import org.zeromq.ZMQ.Socket;

public class ConnectedPlugin {

	private String name = null;
	private Socket config = null;
	private Socket data = null;
	private long heartbeat = 0;
	private HashMap<String, Msg> msgs = null;
	
	public ConnectedPlugin(String name, Socket config, Socket data) {
		this.name = name;
		this.config = config;
		this.data = data;
		this.heartbeat = System.currentTimeMillis();
		this.msgs = new HashMap<String, Msg>();
	}
	
	public HashMap<String, Msg> getMsgs() {
		return this.msgs;
	}
	
	public Msg getMsg(String topic) {
		return this.msgs.get(topic);
	}
	
	public void addMsg(String topic, Msg msg) {
		this.msgs.put(topic, msg);
	}
	
	public long getOffset(String topic) {
		return msgs.get(topic).getOffset();
	}
	
	public void setOffset(String topic, long sn) {
		this.msgs.get(topic).setOffset(sn);
	}
	
	public String getName() {
		return this.name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public Socket getConfig() {
		return this.config;
	}
	
	public void setConfig(Socket s) {
		this.config = s;
	}
	
	public Socket getData() {
		return this.data;
	}
	
	public void setData(Socket s) {
		this.data = s;
	}
	
	public long getHeartbeat() {
		return heartbeat;
	}
	
	public void setHeartbeat(long hb) {
		this.heartbeat = hb;
	}
}
