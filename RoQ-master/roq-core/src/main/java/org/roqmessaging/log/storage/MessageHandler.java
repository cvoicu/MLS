package org.roqmessaging.log.storage;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.roqmessaging.log.LogConfigDAO;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;


/**
* @author Cristian Voicu
*/
public class MessageHandler {
	
	private Logger logger = Logger.getLogger(MessageHandler.class);
	private LogConfigDAO properties = null;
	private HashMap<String, TopicBuffer> exchangeBuffer = null;
	private String nodeName = null;
	private Socket inproc = null;

	public MessageHandler(LogConfigDAO properties, Context context, String nodeName, String logInAddress) {
		this.properties = properties;
		this.nodeName = nodeName;
		this.inproc = context.socket(ZMQ.PUSH);
		this.inproc.bind("inproc://"+this.nodeName);

		exchangeBuffer = new HashMap<String, TopicBuffer>();
	}

	public void manage(String topic, byte[] message) {
		if(!exchangeBuffer.containsKey(topic)) {
			createTopicBuffer(topic);
		}
		TopicBuffer topicBuffer = exchangeBuffer.get(topic);
		//save the message to the buffer
		ByteBuffer bmessage = ByteBuffer.allocateDirect(message.length);
		bmessage.put(message);
		
		topicBuffer.putMessage(bmessage, inproc, nodeName);
	}
	
	public HashMap<String, TopicBuffer> getExchangeBuffer() {
		return exchangeBuffer;
	}

	private void createTopicBuffer(String topic) {
		TopicBuffer topicBuffer = new TopicBuffer(topic, properties);
		exchangeBuffer.put(topic, topicBuffer);
	}

	public ByteBuffer loadFromBuffer(String topic, long offset) {
		
		TopicBuffer topicBuffer = exchangeBuffer.get(topic);
		if(topicBuffer == null) {
			return null;
		}
		topicBuffer.popMessage();
		ByteBuffer message = topicBuffer.getBuffer().get(offset);
		
		return message;
	}

	public void forceSendBuffer() {
		Iterator<String> it = exchangeBuffer.keySet().iterator();
		while(it.hasNext()) {
			//TODO logInSocket.sendZeroCopy(exchangeBuffer.get(it.next()).getBufferToSend(),exchangeBuffer.get(it.next()).getBufferToSend().position(), 0); 
		}
		
	}
	
}
