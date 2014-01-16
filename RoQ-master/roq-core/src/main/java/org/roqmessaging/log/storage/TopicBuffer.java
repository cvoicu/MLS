package org.roqmessaging.log.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.roqmessaging.log.FileConfigurationReader;
import org.roqmessaging.log.LogConfigDAO;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
/**
* @author Cristian Voicu
*/
public class TopicBuffer {

	private String name = null;
	private HashMap<Long, ByteBuffer> buffer = null;
	private long startOffset, endOffset;
	
	private MappedByteBuffer bufferToSend = null; 
	
	private LogManagement lm = null;
	
	public TopicBuffer(String name, LogConfigDAO properties) {
		buffer = new HashMap<Long, ByteBuffer>();
		//bufferToSend = ByteBuffer.allocateDirect(65000);
		this.name = name;

		try {
			lm = new LogManagement(properties);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public LogManagement getLogManagement() {
		return lm;
	}
	
	public ByteBuffer getBufferToSend() {
		return bufferToSend;
	}

	public String getName() {
		return this.name;
	}

	public HashMap<Long, ByteBuffer> getBuffer() {
		return this.buffer;
	}
	
	public void setBuffer(HashMap<Long, ByteBuffer> buffer) {
		this.buffer = buffer;
	}
	
	public long getStartOffset() {
		return this.startOffset;
	}
	public void setStartOffset(long startOffset) {
		this.startOffset = startOffset;
	}
	public long getEndOffset() {
		return this.endOffset;
	}
	public void setEndOffset(long endOffset) {
		this.endOffset = endOffset;
	}
	public void putMessage(ByteBuffer message, Socket inproc, String nodeName) {
		lm.putMessage(name, message, inproc, nodeName);
	}
	/*
	public void putMessage(ByteBuffer message, Socket inproc, String nodeName) {
		//getBuffer().put(getEndOffset(), message);
		//setEndOffset(getEndOffset()+message.capacity());
		//bufferToSend = lm.getLogFile(name).getMappedBB();
		message.position(0);
		if (bufferToSend.remaining() >= message.capacity()+4) {
			bufferToSend.putInt(message.capacity());
			bufferToSend.put(message);
		}
		else {
			//logIn.send(name.getBytes(), 0);
			//logIn.send(""+bufferToSend.position());
			//logIn.sendZeroCopy(bufferToSend, bufferToSend.position(), 0);
			try {
				bufferToSend.position(0);
				lm.store(nodeName, nodeName, name, bufferToSend);
				inproc.send(name, ZMQ.SNDMORE);
				inproc.send(""+bufferToSend.capacity(), ZMQ.SNDMORE);
				inproc.sendByteBuffer(bufferToSend, 0);
			} catch (IOException e) {
				e.printStackTrace();
			}
			bufferToSend.clear();
			if (bufferToSend.remaining() < message.capacity()+4) {
				ByteBuffer newBuffer = ByteBuffer.allocateDirect(message.capacity() + 4);
				newBuffer.putInt(message.capacity());
				newBuffer.put(message);
				//logIn.send(name.getBytes(), 0);
				//logIn.send(""+newBuffer.capacity());
				//logIn.sendZeroCopy(newBuffer, newBuffer.capacity(), 0);
				try {
					newBuffer.position(0);
					lm.store(nodeName, nodeName, name, newBuffer);
					inproc.send(name, ZMQ.SNDMORE);
					inproc.send(""+newBuffer.capacity(), ZMQ.SNDMORE);
					inproc.sendByteBuffer(newBuffer, 0);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
				bufferToSend.putInt(message.capacity());
				bufferToSend.put(message);
			}
		}
	}*/
	public ByteBuffer popMessage() {
		long offset = getStartOffset();
		ByteBuffer message = getBuffer().remove(offset);
		if(offset < getEndOffset()) {
			setStartOffset(offset+message.capacity());
		}
		else { 
			// startOffset == endOffset -> the buffer is empty.
		}
		return message;
	}
	
}
