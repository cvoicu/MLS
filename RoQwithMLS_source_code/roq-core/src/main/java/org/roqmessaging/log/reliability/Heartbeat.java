package org.roqmessaging.log.reliability;

import java.util.ArrayList;

public class Heartbeat implements Runnable {

	private String name = "";
	private int interval = 0;
	private ArrayList<ConnectedPlugin> nodes = new ArrayList<ConnectedPlugin>();
	private ArrayList<ConnectedPlugin> masters = new ArrayList<ConnectedPlugin>();

	
	public Heartbeat(String name, int interval, ArrayList<ConnectedPlugin> nodes, ArrayList<ConnectedPlugin> masters) {
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
