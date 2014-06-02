package org.roqmessaging.log.reliability;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class Msg {

	private String topic = null;
	private ByteBuffer bb = null;
	private long offset = 0;
	private int msgSize = 0;
	private boolean ack = false;
	private int count = 0;
	private long time = 0;
	private HashMap<String, Boolean> replicas = new HashMap<String, Boolean>();
	
	public Msg(String topic, ByteBuffer bb, long offset, int msgSize) {
		this.topic = topic;
		this.bb = bb;
		this.offset = offset;
		this.msgSize = msgSize;
		this.time = System.currentTimeMillis();
	}
	
	public HashMap<String, Boolean> getReplicas() {
		return this.replicas;
	}
	
	public void setReplicas(HashMap<String, Boolean> rep) {
		this.replicas = rep;
	}
	
	public long getTime() {
		return this.time;
	}
	
	public void setTime() {
		this.time = System.currentTimeMillis();
	}
	
	public int getMsgSize() {
		return this.msgSize;
	}
	
	public void setMsgSize(int size) {
		this.msgSize = size;
	}
	
	public int getCount() {
		return this.count;
	}

	public void setCount(int c) {
		this.count = c;
	}
	
	public void upCount() {
		this.count++;
	}
	
	public String getTopic() {
		return this.topic;
	}
	
	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public ByteBuffer getBB() {
		return this.bb;
	}
	
	public void setBB(ByteBuffer bb) {
		this.bb = bb;
	}
	
	public long getOffset() {
		return this.offset;
	}
	
	public void setOffset(long sn) {
		this.offset = sn;
	}
	
	public boolean getAck() {
		return this.ack;
	}
	
	public void setAck(boolean ack) {
		this.ack = ack;
	}
	
}
