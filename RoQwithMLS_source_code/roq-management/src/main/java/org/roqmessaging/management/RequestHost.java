package org.roqmessaging.management;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

public class RequestHost implements Runnable{

	private Logger logger = Logger.getLogger(LogicalQFactory.class);
	
	public MyLook look = null;
	public ZMQ.Socket gcm = null;
	
	public RequestHost(MyLook look, ZMQ.Socket gcm) {
		this.look = look;
		this.gcm = gcm;
	}
	
	@Override
	public void run() {
		
		logger.info("cvoicu: create new Host");
		
		gcm.send((Integer.toString(1234503) + "," + "launch Host").getBytes(), 0);
		String result = new String(gcm.recv(0));
		if (Integer.parseInt(result) != 1234504) {
			look.setLook(true);
			logger.error("The host launching failed on the global configuration server (GCM).");
		}
		else {
			
			logger.info("cvoicu: A new host is beeing launched on amazon ec2.");
		}
		
		//wait for launching the host. and ask every 5 seconds to the gcm if it is launched.
		
		while(!look.getLook()){
		
		gcm.send((Integer.toString(1234505) + "," + "host launched?").getBytes(), 0);
		String result2 = new String(gcm.recv(0));
		if (Integer.parseInt(result2) != 1234506) {
			logger.info("The host is not yet ready.");
		}
		else {
			try {
				Thread.sleep(90000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			look.setLook(true);
			logger.info("cvoicu: A new host was launched on amazon ec2.");
		}
		}
	}

}
