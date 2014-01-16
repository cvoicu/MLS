package org.roqmessaging.log.reliability;

import java.nio.ByteBuffer;

public class Package {

	private long offset = 0;
	private ByteBuffer bb = null;
	private boolean ack = false;
	private boolean isSent = false;
	
	public Package(long offset, ByteBuffer bb) {
		this.offset = offset;
		this.bb = bb;
	}
	
	public long getOffset() {
		return this.offset;
	}
	
	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	public ByteBuffer getByteBuffer() {
		return this.bb;
	}
	
	public void setByteBuffer(ByteBuffer bb) {
		this.bb = bb;
	}
	
	public boolean getAck() {
		return this.ack;
	}
	
	public void setAck(boolean ack) {
		this.ack = ack;
	}
	
	public boolean getIsSent() {
		return this.isSent;
	}
	public void setIsSent(boolean value) {
		this.isSent = value;
	}
}
