package org.roqmessaging.log.reliability;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TimerTask;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class HandleAck extends TimerTask {
	
	private ArrayList<ConnectedPlugin> list = null;
	Plugin node = null;
	HashMap<String, ArrayList<Msg>> pkgs = null;
	
	public HandleAck(Plugin node, ArrayList<ConnectedPlugin> list, HashMap<String, ArrayList<Msg>> pkgs) {
		this.node = node;
		this.list = list;
		this.pkgs = pkgs;
	}

	public void run() {
		
		Collection<ArrayList<Msg>> p = pkgs.values();
		Iterator<ArrayList<Msg>> pit = p.iterator();
		while(pit.hasNext()) {
			
			ArrayList<Msg> mlist = pit.next();
			int i = 0;
			while(i < mlist.size()) {
				
				Set<String> mset = mlist.get(i).getReplicas().keySet();
				Iterator<String> mit = mset.iterator();
				while(mit.hasNext()) {
					String rep = mit.next();
					if(!mlist.get(i).getReplicas().get(rep) && (System.currentTimeMillis() - mlist.get(i).getTime()) > 1500) {
						Msg msg = mlist.get(i);
						if(msg.getCount() >=3) {
							node.handleCrash(rep);
							mlist.remove(i);
							i--;
							break;
						}
						else {
							Socket data = null;
							boolean exists = false;
							int j = 0;
							while(j < list.size()) { 
								if(list.get(j).getName().compareTo(rep) == 0) {
									data = list.get(j).getData();
									exists = true;
									break;
								}
								j++;
							}
							if(exists) {
								data.send(node.getNodeName(), ZMQ.SNDMORE);
								data.send(msg.getTopic(), ZMQ.SNDMORE);
								data.send(""+msg.getOffset(), ZMQ.SNDMORE);
								data.send(""+msg.getMsgSize(), ZMQ.SNDMORE);
								data.sendByteBuffer(msg.getBB(), 0);
								msg.setTime();
								msg.upCount();
							}
						}
					}
				}
				i++;
		}
		
		}
	}
	

}
