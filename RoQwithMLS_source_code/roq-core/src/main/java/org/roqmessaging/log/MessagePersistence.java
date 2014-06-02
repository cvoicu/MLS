package org.roqmessaging.log;

import org.apache.commons.configuration.ConfigurationException;
import org.roqmessaging.log.reliability.Plugin;
import org.roqmessaging.log.storage.MessageHandler;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;


public class MessagePersistence {
	
	private MessageHandler storage = null;
	private Plugin node = null;
	
	private LogConfigDAO properties = null;
	
	private String nodeName = "";
	private int replicationFactor = 0;
	
	private String nodeAddress = null;
	private int configPort = 0;
	private int dataPort = 0;
	private String seeds = null;
	private boolean useLog = false;
	
	private Context context = null;

	public MessagePersistence(String logAddress, String seeds, String propertyFile) {
		System.out.println("!!!!!!!!!!!! SEEDS:"+seeds);
		System.out.println("prop file= "+ propertyFile);
		FileConfigurationReader reader = new FileConfigurationReader();
		try {
			this.properties = reader.loadLogConfiguration(propertyFile);
			this.useLog = properties.getUseLog();
			this.replicationFactor = properties.getReplicationFactor();
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		
		this.seeds = seeds;
		String[] connectionInfo = logAddress.split(":");
		if(connectionInfo.length >= 2) {
			nodeAddress = connectionInfo[0];
			configPort = new Integer(connectionInfo[1]);
		}
		dataPort = configPort+1;
		
		this.nodeName = "Node"+logAddress+"x";
		
		context = ZMQ.context(1);

		Socket start = context.socket(ZMQ.PUB);
		start.bind("inproc://"+nodeName+"start"+configPort);
		
		if(useLog) {
			storage = new MessageHandler(properties, context, nodeName, logAddress, configPort);
			node = new Plugin(properties, context, nodeName, replicationFactor, nodeAddress, configPort, dataPort, this.seeds, storage);
			Thread nodeThread = new Thread(node);
			nodeThread.start();
		}
		start.send("start");
	}

	//this method is called every time a message arrives on Exchange.
	public void handle(String topic, String prodID, byte[] message) {
		
		storage.manage(topic, message);
		
	}
	
	public boolean getUseLog() {
		return useLog;
	}
	
	public MessageHandler getMessageHandler() {
		return this.storage;
	}

}
