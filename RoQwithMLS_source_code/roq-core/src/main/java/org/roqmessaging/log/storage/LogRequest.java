package org.roqmessaging.log.storage;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

public class LogRequest implements Runnable {
	
	private Logger logger = Logger.getLogger(LogRequest.class);

	
	private Context context = null;
	private MessageHandler logIn = null;
	private LogOut logOut = null;
	
	private ZMQ.Socket logRequestSocket = null;

	public LogRequest(Context context, MessageHandler logIn, LogOut logOut, String logRequestAddress) {
		this.context = ZMQ.context(1);
		this.logIn = logIn;
		this.logOut = logOut;
	
		this.logRequestSocket = this.context.socket(ZMQ.REP);
		this.logRequestSocket.bind("tcp://"+logRequestAddress);
		
		logger.info("wait for connections from subscribers:"+logRequestAddress);

		
	}

	@Override
	public void run() {
		ZMQ.Poller poller = new ZMQ.Poller(1);
		poller.register(this.logRequestSocket);
		while (true) {
			poller.poll(100);
			if (poller.pollin(0)) {

				logger.debug("$$$$$$ TEST 3" );
				String topic = new String(logRequestSocket.recv());
				logRequestSocket.send("OK");
				long msgNo = Long.valueOf(new String(logRequestSocket.recv()));
				logRequestSocket.send("OK");
				long offset = Long.valueOf(new String(logRequestSocket.recv()));
				ByteBuffer message = logIn.loadFromBuffer(topic, offset);
				if(message==null) {
					message = logOut.loadFromLog(topic, offset+(4*msgNo));
				}
				if(message == null) {
					logRequestSocket.send("NULL"); 
				}
				else {
					logRequestSocket.send("OK");
					logRequestSocket.recv();
					logRequestSocket.send(""+message.capacity());
					logRequestSocket.recv();
					logRequestSocket.sendZeroCopy(message, message.capacity(), 0);
					logger.info("TEST!!!! the message was sent");
				}

			}
		}
	}
	
	public long decodeOffset(byte[] request) {
		
		long offset;
		offset = ByteBuffer.wrap(request, 0, 8).getLong();
		
		return offset;
	}
	
	public String decodeTopic(byte[] request) {
		String topic = null;
		
		topic = new String(ByteBuffer.wrap(request, 8, request.length).array());
		
		return topic;
	}

}
