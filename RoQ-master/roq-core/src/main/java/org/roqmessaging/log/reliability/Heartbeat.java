package org.roqmessaging.log.reliability;

import java.util.ArrayList;

public class Heartbeat implements Runnable {

	private String name = "";
	private int interval = 0;
	private ArrayList<ConnectedNode> nodes = new ArrayList<ConnectedNode>();
	private ArrayList<ConnectedNode> masters = new ArrayList<ConnectedNode>();

	
	public Heartbeat(String name, int interval, ArrayList<ConnectedNode> nodes, ArrayList<ConnectedNode> masters) {
		this.name = name;
		this.interval = interval;
		this.nodes = nodes;
		this.masters = masters;
	}

	public void run() {
		while(true) {
		
		
		int i = 0;
		while(i < nodes.size()) {
			nodes.get(i).getConfig().sendMore("heartbeat");
			nodes.get(i).getConfig().send(name);
			i++;
		}
		i=0;
		while(i < masters.size()) {
			masters.get(i).getConfig().sendMore("heartbeat");
			masters.get(i).getConfig().send(name);
			i++;
		}
		try {
			Thread.sleep(interval);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		
	}
	
}
