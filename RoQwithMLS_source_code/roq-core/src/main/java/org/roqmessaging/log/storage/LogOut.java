package org.roqmessaging.log.storage;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class LogOut implements Runnable {

	private Logger logger = Logger.getLogger(LogOut.class);

	
	private Context context = null;
	private String logOutAddress = null;

	private Socket logOutSocket; //ZMQ socket
	
	public LogOut(Context context, String logOutAddress) {
		this.context = context;
		this.logOutAddress = logOutAddress;
	
	}

	public void close() {
		
	}

	@Override
	public void run() {

		this.logOutSocket = context.socket(ZMQ.REQ);
		this.logOutSocket.connect("tcp://"+logOutAddress);

		logger.info("connected to log out management:"+logOutAddress);
		
	}
	
	public ByteBuffer loadFromLog(String topic, long offset) {
		
		//send request
		logOutSocket.send(topic, 0);
		logger.debug("the topic was received by the log Management:" + new String(logOutSocket.recv()));
		byte[] arrayOffset = ByteBuffer.allocate(8).putLong(offset).array();
		logOutSocket.send(arrayOffset, 0);
		
		//reveice message
		int length = Integer.valueOf(new String(logOutSocket.recv()));
		if(length == 0) {
			return null;
		}

		ByteBuffer message = ByteBuffer.allocateDirect(length);
		logOutSocket.send("OKKKK");
		logOutSocket.recvZeroCopy(message, length, 0);
		
		return message;
	}

}
