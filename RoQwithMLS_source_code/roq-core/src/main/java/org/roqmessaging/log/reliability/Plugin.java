package org.roqmessaging.log.reliability;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;

import org.apache.log4j.Logger;
import org.roqmessaging.log.LogConfigDAO;
import org.roqmessaging.log.storage.LogManagement;
import org.roqmessaging.log.storage.MessageHandler;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;


public class Plugin implements Runnable {

	static Logger log = Logger.getLogger(Plugin.class.getName());
	private String name = null;
	private String nodeAddress;
	private int configPort = 0;
	private int dataPort = 0;
	private int replicationFactor = 0;
	private int counter = 0;
	private Context context = null;
	private Socket server = null;
	private Socket config = null;
	private Socket data = null;
	private Socket host = null;
	private ArrayList<Socket> subSubSocks = new ArrayList<Socket>();
	private Socket subPubSock = null;
	private PriorityList pl = null;
	private ArrayList<ConnectedPlugin> replicaList = new ArrayList<ConnectedPlugin>();
	private ArrayList<ConnectedPlugin> masters = new ArrayList<ConnectedPlugin>();
	private ArrayList<PluginInfo> crashedList = new ArrayList<PluginInfo>();
	private ZMQ.Poller items = null;
	private long hb = 0;
	private int debug = 1;
	private int debug3 = 1;
	private boolean debug2 = true;
	private Thread hbThread;
	private LogManagement lm = null;
	private MessageHandler mh = null;
	private int nbAcks = 0;
	private String topic = null;
	private HashMap<String, ArrayList<Msg>> packages = new HashMap<String, ArrayList<Msg>>();
	
	public Plugin(LogConfigDAO properties, Context ctx, String name, int replicationFactor, String nodeAddress, int configPort, int dataPort, String seeds, MessageHandler mh) {
		this.name = name;
		this.nodeAddress = nodeAddress;
		this.configPort = configPort;
		this.dataPort = dataPort;
		this.replicationFactor = replicationFactor;
		this.mh = mh;
		this.pl = new PriorityList(true);
		this.pl.setArrayList(decodeSeeds(seeds));
		System.out.println("number of unique seeds: "+pl.getArrayList().size());
		try {
			this.lm = new LogManagement(properties);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		context = ZMQ.context(1);
		
		host = ctx.socket(ZMQ.SUB);
		host.connect("inproc://"+name+"start"+configPort);
		host.subscribe("".getBytes());
		
		server = ctx.socket(ZMQ.PULL);
		server.connect("inproc://"+name+configPort);
		
		config = context.socket(ZMQ.DEALER);
		config.bind("tcp://"+nodeAddress+":"+configPort);
		
		data = context.socket(ZMQ.DEALER);
		data.bind("tcp://"+nodeAddress+":"+dataPort);
		
		subPubSock = context.socket(ZMQ.PUB);
		
		items = new ZMQ.Poller(100);
		items.register(host);
		items.register(server);
		items.register(config);
		items.register(data);

		hb = System.currentTimeMillis();
		connectToReplicas();
		Heartbeat hbt = new Heartbeat(name, 2000, replicaList, masters);
		hbThread = new Thread(hbt);
		hbThread.start();
		Timer tim = new Timer();
		tim.schedule(new HandleAck(this, replicaList, packages), 1500, 1500);
	}

	public void run() {
		int a = 0;
		while (a<pl.getArrayList().size()) {
			System.out.println(pl.getArrayList().get(a).getName() + "counter: "+pl.getArrayList().get(a).getCounter());
			a++;
		}
		
		while(true) {
			items.poll();
			if(items.pollin(0)) {
				//host
				System.out.println(name+ " "+new String(host.recv()));
			}
			if(items.pollin(1)) {
				topic = new String(server.recv());
				long offset = Long.valueOf(new String(server.recv()));
				int msgSize = Integer.valueOf(new String(server.recv()));
				String lf = new String(server.recv());
				ByteBuffer message = ByteBuffer.allocateDirect(msgSize);
				server.recvByteBuffer(message, 0);
				Msg msg = new Msg(topic, message, offset, msgSize);
				int i = 0;
				ArrayList<Msg> listPkgs = packages.get(topic);
				if(listPkgs == null) {
					packages.put(topic, new ArrayList<Msg>());
				}
				listPkgs = packages.get(topic);
				listPkgs.add(msg);
				while(i < replicaList.size()) {
					replicaList.get(i).addMsg(topic, msg);
					Socket data = replicaList.get(i).getData();
					data.send(name, ZMQ.SNDMORE);
					data.send(topic, ZMQ.SNDMORE);
					data.send(""+offset, ZMQ.SNDMORE);
					data.send(""+msgSize, ZMQ.SNDMORE);
					data.send(lf, ZMQ.SNDMORE);
					data.sendByteBuffer(message, 0);
					i++;
				}
			}
			if(items.pollin(2)) {
				//config
				String message = new String(config.recv());
				
				if(message.compareTo("ack") == 0) {
					String from = new String(config.recv());
					String tpc = new String(config.recv());
					long offset = Long.valueOf(new String(config.recv()));
					int i = 0;
					while(i<replicaList.size()) {
						if(from.compareTo(replicaList.get(i).getName()) == 0) {
							ArrayList<Msg> listPkgs = packages.get(topic);
							int j = 0;
							while(j < listPkgs.size())  {
								if(listPkgs.get(j).getOffset() == offset) {
									listPkgs.get(j).getReplicas().put(from, true);
									break;
								}
								j++;
							}
							break;
						}
						i++;
					}
					if(checkAcks(tpc, offset)) {
						//in this case we can remove the buffer from memory.
						ArrayList<Msg> pkgs = null;
						pkgs = packages.get(tpc);
						int j = 0;
						while(j >= pkgs.size()) {
							if(offset == pkgs.get(j).getOffset()) {
								try {
									destroyDirectByteBuffer(pkgs.get(j).getBB());
								} catch (Exception e) {
									e.printStackTrace();
								}
								pkgs.remove(j);
								break;
							}
							j++;
						}
					}	
				}
				
				if(message.compareTo("heartbeat") == 0) {
					String heartbeatName = new String(config.recv());
					int i = 0;
					while(i < replicaList.size()) {
						if(heartbeatName.compareTo(replicaList.get(i).getName()) == 0) {
							replicaList.get(i).setHeartbeat(System.currentTimeMillis());
						}
						i++;
					}
				}
				if(message.compareTo("announce crash") == 0) {
					String crashedName = new String(config.recv());
					log.info(name + " is informed that node crashed: "+crashedName);
					handleCrash(crashedName);
				}
				if(message.compareTo("announce new node") == 0) {
					String newNode = new String(config.recv());
					handleNewNode(newNode);
				}
				if(message.compareTo("announce new master node") == 0) {
					String newMasterNode = new String(config.recv());
					handleNewMasterNode(newMasterNode);
				}
				if(message.compareTo("ask for list of nodes") == 0) {
					
					String listOfNodes = encodeNodeInfoList(pl.getArrayList());
					config.send(listOfNodes);
				}
			}
			if(items.pollin(3)) {
				String masterName = new String(data.recv());
				String topic = new String(data.recv());
				long offset = Long.valueOf(new String(data.recv()));
				int msgSize = Integer.valueOf(new String(data.recv()));
				boolean lstf = Boolean.valueOf(new String(data.recv()));
				ByteBuffer buf = ByteBuffer.allocateDirect(msgSize);
				data.recvByteBuffer(buf, 0);
				try {
					lm.storeReplica(name, masterName, topic, buf, false, offset, lstf);
					destroyDirectByteBuffer(buf);
				} catch (Exception e) {
					e.printStackTrace();
				}
				int i = 0;
				while (i < masters.size()) {
					if(masterName.compareTo(masters.get(i).getName()) == 0) {
						Socket sock = masters.get(i).getConfig();
						sock.send("ack", ZMQ.SNDMORE);
						sock.send(name, ZMQ.SNDMORE);
						sock.send(topic, ZMQ.SNDMORE);
						sock.send(""+offset);
					}
					i++;
				}
			}			
			int i = 0;				

			if(replicaList.size() < replicationFactor) {
					connectToReplicas();
			}
			
			debug++;
			if(System.currentTimeMillis() - hb > 2000) {
				
				hb = System.currentTimeMillis();
			}
			
			
			i = 0;
			while(i < replicaList.size()) {
				if(System.currentTimeMillis() - replicaList.get(i).getHeartbeat() > 6000) {
					System.out.println("6 seconds passed");
					handleCrash(replicaList.get(i).getName());
				}
				i++;
			}
			
		}
	}
	
	public String getNodeName() {
		return this.name;
	}
	
	public boolean checkAcks(String topic, long offset) {
		ArrayList<Msg> listPkgs = packages.get(topic);
		int i = 0;
		while(i < listPkgs.size()) {
			if(listPkgs.get(i).getOffset() == offset) {
				Msg msg = listPkgs.get(i);
				Collection<Boolean> col = msg.getReplicas().values();
				Iterator<Boolean> it = col.iterator();
				while(it.hasNext()) {
					if(!it.next()) {
						return false;
					}
				}
			}
			i++;
		}
		return true;
	}
	
	public void initAcks(String topic) {
		int i = 0;
		while(i < replicaList.size()) {
			replicaList.get(i).getMsgs().remove(topic);
			i++;
		}
	}
	
	public String encodeNodeInfoList(ArrayList<PluginInfo> list) {
		String result = "";
		int i = 0;
		while(i < list.size()) {
			if(i == 0) {
				result = encodeNodeInfo(list.get(i));
			}
			else {
				result = result + "#" + encodeNodeInfo(list.get(i));
			}
			i++;
		}		
		return result;
	}
	
	public ArrayList<PluginInfo> decodeNodeInfoList(String message) {
		ArrayList<PluginInfo> list = new ArrayList<PluginInfo>();
		String[] items = message.split("#");
		int i = 0;
		while(i < items.length) {
			PluginInfo ni = decodeNodeInfo(items[i]);
			list.add(ni);
			i++;
		}
		return list;
	}
	
	public String encodeNodeInfo(PluginInfo ni) {
		String message = null;
		message = ni.getName()+" "+ni.getAddress()+" "+ni.getConfigPort()+" "+ni.getDataPort()+" "+ni.getCounter();
		return message;
	}
	
	public PluginInfo decodeNodeInfo(String message) {
		PluginInfo ni = null;
		String[] items = message.split(" ");
		ni = new PluginInfo(items[0], items[1], Integer.valueOf(items[2]), Integer.valueOf(items[3]));
		ni.setCounter(Integer.valueOf(items[4]));
		return ni;
	}
	
	private ArrayList<PluginInfo> decodeSeeds(String seeds) {
		ArrayList<PluginInfo> seedsList = new ArrayList<PluginInfo>();
		String[] s = seeds.split("#");
		List<String> list = Arrays.asList(s);
		Set<String> set = new HashSet<String>(list);
		String[] uniqueSeeds = new String[set.size()];
		set.toArray(uniqueSeeds);
		int i = 0;
		while(i < uniqueSeeds.length) {
			String[] seed = uniqueSeeds[i].split(":");
			if(seed.length >= 2) {
				String address = seed[0];
				int cport = Integer.valueOf(seed[1]);
				int dport = cport+1;
				
				PluginInfo ni = new PluginInfo("Node"+uniqueSeeds[i]+"x", address, cport, dport);
				seedsList.add(ni);
			}
			i++;
		}
		return seedsList;
	}
	
	public ArrayList<PluginInfo> askForListOfNodes() {
		ArrayList<PluginInfo> newList = new ArrayList<PluginInfo>();
		ArrayList<PluginInfo> mergedList = new ArrayList<PluginInfo>();
		String message = null;
		Socket socket = context.socket(ZMQ.DEALER);
		int j = 0;
		boolean connected = false;
		while(j < pl.getArrayList().size()) {
			int i = new Random().nextInt(pl.getArrayList().size());
			try{
				if(pl.getArrayList().get(i).getName().compareTo(name) != 0) {
					socket.connect("tcp://"+pl.getArrayList().get(i).getAddress()+":"+pl.getArrayList().get(i).getConfigPort());
					connected = true;
					break;
				}
			} catch(Exception e) {
				System.out.println("Cannot connect to ..."); //+pl.getArrayList().get(i).getAddress()+":"+pl.getArrayList().get(i).getConfigPort());
			}
			j++;
		}
		if (connected) {
			socket.send("ask for list of nodes");
			//System.out.println("test");
			message = new String(socket.recv());
		}
		else {
			System.out.println(name +" The connection is not working");
		}
		if(message != null) {
			newList = decodeNodeInfoList(message);
			
			System.out.println(name+ "size of mergedlist "+ mergedList.size());
		}
		//log.info(name + " received the list of nodes: "+ message);
		mergedList = mergeListsOfNodes(pl.getArrayList(), newList);
		return mergedList;
	}
	
	private ArrayList<PluginInfo> mergeListsOfNodes(ArrayList<PluginInfo> localList, ArrayList<PluginInfo> receivedList) {
		ArrayList<PluginInfo> mergedList = new ArrayList<PluginInfo> ();
		
		if(localList.size() == 0) {
			return receivedList;
		}
		if(receivedList.size() == 0) {
			return localList;
		}
		
		int i = 0;
		while(i < localList.size()) {
			int j = 0;
			while(j < receivedList.size()) {
				if(localList.get(i).getName().compareTo(receivedList.get(j).getName()) == 0) {
					if(receivedList.get(j).getCounter() >= localList.get(i).getCounter()) {
						mergedList.add(receivedList.get(j));
					}
					else {
						mergedList.add(localList.get(i));
					}
					break;
				}
				j++;
			}
			if(j==receivedList.size()) {
				mergedList.add(localList.get(i));
			}
			i++;
		}
		i=0;
		while(i<receivedList.size()) {
			int k = 0;
			while(k<mergedList.size()){
				if(receivedList.get(i).getName().compareTo(mergedList.get(k).getName()) == 0) {
					break;
				}
				k++;
			}
			if(k < mergedList.size()) {
				mergedList.add(receivedList.get(i));
			}
			i++;
		}
		
		return mergedList;
	}

	public void connectToReplicas() {
		
		ArrayList<PluginInfo> blackList = new ArrayList<PluginInfo>();
		PluginInfo thisNode = new PluginInfo(name, nodeAddress, configPort, dataPort);
		thisNode.setCounter(masters.size());
		int j=0;
		while(j<replicaList.size()) {
			PluginInfo ni = new PluginInfo(replicaList.get(j).getName(), "", 0, 0);
			blackList.add(ni);
			j++;
		}
		blackList.add(thisNode);
		
		
		int i = replicaList.size();
		while(i < replicationFactor) {
			PluginInfo ni = pl.pick(blackList);
			if(ni != null) {
				Socket config = context.socket(ZMQ.DEALER);
				config.connect("tcp://"+ni.getAddress()+":"+ni.getConfigPort());
				
				Socket data = context.socket(ZMQ.DEALER);
				data.connect("tcp://"+ni.getAddress()+":"+ni.getDataPort());
				
				ConnectedPlugin replica = new ConnectedPlugin(ni.getName(), config, data);
				replicaList.add(replica);
				blackList.add(ni);
				config.sendMore("announce new node");
				config.send(encodeNodeInfo(thisNode));
				config.sendMore("announce new master node");
				config.send(encodeNodeInfo(thisNode));
				log.info(name + " connected to replica "+ ni.getName());
			}
			i++;
		}
	}
	
	public void annouceCrash(String crashedNode) {
		int i = 0;
		while(i < replicaList.size()) {
			replicaList.get(i).getConfig().sendMore("announce crash");
			replicaList.get(i).getConfig().send(crashedNode);
			i++;
		}
	}
	
	public void handleNewNode(String newNode) {
		PluginInfo ni = decodeNodeInfo(newNode);
		int i = 0;
		while(i < pl.getArrayList().size()) {
			if(ni.getName().compareTo(pl.getArrayList().get(i).getName()) == 0) {
				break;
			}
			i++;
		}
		if (i == pl.getArrayList().size()) {
		i=0;
		pl.add(ni);
		while(i < replicaList.size()) {
			replicaList.get(i).getConfig().sendMore("announce new node");
			replicaList.get(i).getConfig().send(newNode);
			i++;
		}
		}
	}
	
	public void handleNewMasterNode(String newNode) {
		PluginInfo ni = decodeNodeInfo(newNode);
		Socket config = context.socket(ZMQ.DEALER);
		config.connect("tcp://"+ni.getAddress()+":"+ni.getConfigPort());
		
		Socket data = context.socket(ZMQ.DEALER);
		data.connect("tcp://"+ni.getAddress()+":"+ni.getDataPort());
		
		items.register(data);
		
		ConnectedPlugin cn = new ConnectedPlugin(ni.getName(), config, data);
		masters.add(cn);
		log.info(name + " new master connected : "+ni.getName() );
	}
	
	public void handleCrash(String crashedName) {
		PluginInfo crashedNode = null;
		int i = 0; 
		while(i < pl.getArrayList().size()) {
			if(crashedName.compareTo(pl.getArrayList().get(i).getName()) == 0) {
				crashedNode = pl.getArrayList().get(i);
			}
			i++;
		}
		if(crashedNode == null) {
			crashedNode = new PluginInfo(crashedName, "", 0, 0);
		}
		
		
		
		//take an action in case the crashed node is a raplica for this node.
		i = 0;
		while(i < replicaList.size()) {
			if(crashedName.compareTo(replicaList.get(i).getName()) == 0) {
				replicaList.remove(i);
			}
			i++;
		}
		
		//take an action in case the crashed node is a master.
		i = 0;
		while(i < masters.size()) {
			if(crashedName.compareTo(masters.get(i).getName()) == 0) {
				masters.remove(i);
			}
			i++;
		}
		
		//announce replicas node that a node has crashed if the node isn't in the crashed list.
		i = 0;
		while(i < crashedList.size()) {
			if(crashedName.compareTo(crashedList.get(i).getName()) == 0) {
				break;
			}
			i++;
		}
		if(i==crashedList.size()) {
			i = 0;
			while(i < replicaList.size()) {
				replicaList.get(i).getConfig().sendMore("announce crash");
				replicaList.get(i).getConfig().send(crashedName);
				i++;
			}
			i = 0;
			while(i < masters.size()) {
				masters.get(i).getConfig().sendMore("announce crash");
				masters.get(i).getConfig().send(crashedName);
				i++;
			}
			crashedList.add(crashedNode);
		}				
	}
	
	public static void destroyDirectByteBuffer(ByteBuffer toBeDestroyed) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		Method cleanerMethod = toBeDestroyed.getClass().getMethod("cleaner");
		cleanerMethod.setAccessible(true);
		Object cleaner = cleanerMethod.invoke(toBeDestroyed);
		Method cleanMethod = cleaner.getClass().getMethod("clean");
		cleanMethod.setAccessible(true);
		cleanMethod.invoke(cleaner);

	}
	
}
