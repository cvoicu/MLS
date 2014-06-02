package org.roqmessaging.log.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.roqmessaging.log.LogConfigDAO;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

/**
* @author Cristian Voicu
*/
public class LogManagement {

	private Logger logger = Logger.getLogger(LogManagement.class);
	
	LogConfigDAO properties = null;	
    
    private HashMap<String, LogFile> files = new HashMap<String, LogFile>();
    private HashMap<String, HashMap> mnames = new HashMap<String, HashMap>();
    
    private Socket inproc = null;
    
    String mytopic = "";
    boolean mysend = false;
    
	public LogManagement(LogConfigDAO properties) throws IOException {
		this.properties = properties;
		
	}
	
	public LogFile getLogFile(String topic) {
		return files.get(topic);
	}

	public void putMessage(String topic, ByteBuffer message, Socket inproc, String nodeName) {
		this.inproc = inproc;
		try {
			store(nodeName, nodeName, topic, message, true, 0, false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void storeReplica(String localName, String masterName, String topic, ByteBuffer message, boolean send, long of, boolean lf) throws IOException {
		//System.out.println("???? cvoicu: this method is called.");
		mytopic = topic;
		mysend = send;
		LogFile logFile=null;
		RandomAccessFile out = null;
		
		if(!mnames.containsKey(masterName)) {
			HashMap<String, LogFile> fl = new HashMap<String, LogFile>();
			mnames.put(masterName, fl);
		}
		
		HashMap<String, LogFile> fls = new HashMap<String, LogFile>();
				fls = mnames.get(masterName);
		
		if(fls.containsKey(topic)) {
			logFile = fls.get(topic);
			out = logFile.getOutputStream();
			out.getChannel().write(message);
			try {
				destroyDirectByteBuffer(message);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(lf) {
				out.close();
				this.createFile(localName, masterName, topic, ""+of, of, logFile,true);
			}
		}
		else {
			String dir = properties.getDirectory();
			Path directoryPath = Paths.get(dir+localName+"/"+masterName+"/"+topic);
			Path lastFile = null;
			if(send && Files.exists(directoryPath)) {
				//recovery
				Iterator<Path> it = Files.newDirectoryStream(directoryPath).iterator();
				while(it.hasNext()) {
					lastFile = it.next();
				}
				if(lastFile != null) {
					String filename = lastFile.getFileName().toString();
					long offset = Long.valueOf(filename.substring(0, filename.length()-4));
					offset = offset + lastFile.toFile().length();
					logFile = createFile(localName, localName, topic, ""+of, of, logFile, true);
					out = logFile.getOutputStream();
				}
				else {
					logFile = this.createFile(localName, masterName, topic, "0", 0L, logFile, true);
					out = logFile.getOutputStream();
				}
			}
			else {
				if(Files.exists(directoryPath)) {
					Iterator<Path> it = Files.newDirectoryStream(directoryPath).iterator();
					while(it.hasNext()) {
						Files.delete(it.next());
					}
				}
				logFile = this.createFile(localName, masterName, topic, ""+of, of, logFile, true);
				out = logFile.getOutputStream();
				
			}
			
			out.getChannel().write(message);
			try {
				destroyDirectByteBuffer(message);
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
	}
	
	public void store(String localName, String masterName, String topic, ByteBuffer message, boolean send, long of, boolean lf) throws IOException {
		mytopic = topic;
		mysend = send;
		LogFile logFile=null;
		RandomAccessFile out = null;
		
		if(files.containsKey(topic)) {
			logFile = files.get(topic);
			if(logFile.getFileSize()>properties.getMaxFileSize()) {
				logFile.getMappedBB().force();
				int bp = logFile.getMappedBB().position();
				logFile.getOutputStream().getChannel().truncate(logFile.getOutputStream().length()-650000 + logFile.getMappedBB().position());
				logFile.setCurrentOffset(logFile.getCurrentOffset()+(long)bp);
				logFile.setOffset(logFile.getOffset()+(long)bp);
				logFile.setFileSize(logFile.getFileSize()+(long)bp);
				logFile.getMappedBB().flip();
				
				if(send) {
					inproc.send(topic, ZMQ.SNDMORE);
					inproc.send(""+logFile.getOffset(), ZMQ.SNDMORE);
					inproc.send(""+bp, ZMQ.SNDMORE);
					inproc.send("true", ZMQ.SNDMORE);
					inproc.sendByteBuffer(logFile.getMappedBB(), 0);
				}
				try {
					destroyDirectByteBuffer(logFile.getMappedBB());
				} catch (Exception e) {
					e.printStackTrace();
				}
				logFile.getOutputStream().close();
				this.createFile(localName, masterName, topic, ""+logFile.getOffset(), logFile.getOffset(), logFile, false);
			}
			out = logFile.getOutputStream();
		}
		else {
			String dir = properties.getDirectory();
			Path directoryPath = Paths.get(dir+localName+"/"+masterName+"/"+topic);
			Path lastFile = null;
			if(send && Files.exists(directoryPath)) {
				//recovery
				Iterator<Path> it = Files.newDirectoryStream(directoryPath).iterator();
				while(it.hasNext()) {
					lastFile = it.next();
				}
				if(lastFile != null) {
					String filename = lastFile.getFileName().toString();
					long offset = Long.valueOf(filename.substring(0, filename.length()-4));
					offset = offset + lastFile.toFile().length();
					logFile = createFile(localName, localName, topic, ""+offset, offset, logFile, false);
					out = logFile.getOutputStream();
				}
				else {
					logFile = this.createFile(localName, masterName, topic, "0", 0L, logFile, false);
					out = logFile.getOutputStream();
				}
			}
			else {
				if(Files.exists(directoryPath)) {
					Iterator<Path> it = Files.newDirectoryStream(directoryPath).iterator();
					while(it.hasNext()) {
						Files.delete(it.next());
					}
				}
				logFile = this.createFile(localName, masterName, topic, ""+of, of, logFile, false);
				out = logFile.getOutputStream();
				
			}
		}
		MappedByteBuffer bufferToSend = logFile.getMappedBB();
		message.position(0);
		if (bufferToSend.remaining() >= message.capacity()+4) {
			if(send)
				bufferToSend.putInt(message.capacity());
			bufferToSend.put(message);
			if(send == false) {
				bufferToSend.force();
			}
			try {
				destroyDirectByteBuffer(message);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else {
			try {
				
				bufferToSend.force();
				int bufPos = bufferToSend.position();
				logFile.getOutputStream().getChannel().truncate(logFile.getOutputStream().length()-bufferToSend.capacity() + bufPos);
				bufferToSend.flip();
				if(send) {
					inproc.send(topic, ZMQ.SNDMORE);
					inproc.send(""+logFile.getOffset(), ZMQ.SNDMORE);
					inproc.send(""+bufPos, ZMQ.SNDMORE);
					inproc.send("false", ZMQ.SNDMORE);
					inproc.sendByteBuffer(bufferToSend, 0);
				}
				destroyDirectByteBuffer(bufferToSend);
				logFile.setCurrentOffset(logFile.getCurrentOffset()+(long)bufPos);
				logFile.setOffset(logFile.getOffset()+(long)bufPos);
				logFile.setFileSize(logFile.getFileSize()+(long)bufPos);
				logFile.setMappedBB(out.getChannel().map(FileChannel.MapMode.READ_WRITE, logFile.getCurrentOffset(), 650000));
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (logFile.getMappedBB().remaining() < message.capacity()+4) { //the message exceeds the capacity of the buffer
																   //so it will be sent as is.
				logFile.setCurrentOffset(logFile.getCurrentOffset()+(long)logFile.getMappedBB().capacity());
				logFile.setOffset(logFile.getOffset()+(long)logFile.getMappedBB().capacity());
				logFile.setFileSize(logFile.getFileSize()+(long)logFile.getMappedBB().capacity());
				MappedByteBuffer newBuffer = out.getChannel().map(FileChannel.MapMode.READ_WRITE, logFile.getCurrentOffset(), message.capacity()+4);
				if(send)
					newBuffer.putInt(message.capacity());
				newBuffer.put(message);
				try {
					newBuffer.position(0);
					newBuffer.force();
					if(send) {
						inproc.send(topic, ZMQ.SNDMORE);
						inproc.send(""+logFile.getOffset(), ZMQ.SNDMORE);
						inproc.send(""+newBuffer.capacity(), ZMQ.SNDMORE);
						inproc.send("false", ZMQ.SNDMORE);
						inproc.sendByteBuffer(newBuffer, 0);
						destroyDirectByteBuffer(newBuffer);
						logFile.setCurrentOffset(logFile.getCurrentOffset()+(long)newBuffer.capacity());
						logFile.setOffset(logFile.getOffset()+(long)newBuffer.capacity());
						logFile.setFileSize(logFile.getFileSize()+(long)newBuffer.capacity());
						logFile.setMappedBB(out.getChannel().map(FileChannel.MapMode.READ_WRITE, logFile.getCurrentOffset(), 650000));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else {
				if(send)
					logFile.getMappedBB().putInt(message.capacity());
				logFile.getMappedBB().put(message);
				if(send == false) {
					logFile.getMappedBB().force();
					logFile.getOutputStream().getChannel().truncate(logFile.getOutputStream().length()-65000 + logFile.getMappedBB().position());
				}
				try {
					destroyDirectByteBuffer(message);
				} catch (Exception e) {
					e.printStackTrace();
				} 
			}
		}
	}
	
	private LogFile createFile(String localName, String masterName, String Mtopic, String fileName, long offset, LogFile logFile, boolean replicas) throws IOException {
		RandomAccessFile out = null;
		String topic = null;
		if(logFile==null) {
			logFile = new LogFile(offset);
		}
			try {
				topic = Mtopic.trim();
				topic = topic.replace("\u0000", "");
				String dir = properties.getDirectory();
				Path newFile = null;
				Path directoryPath = Paths.get(dir+localName+"/"+masterName+"/"+topic);
				if(!Files.exists(directoryPath)){
					Files.createDirectories(directoryPath);
					logger.info("Directory created successfully!");
				}
				else {
					logger.info("Directory already existed!");
				} 
				
				Path filePath = Paths.get(dir+localName+"/"+masterName+"/"+topic+"/"+fileName+".log");
				if(!Files.exists(filePath)) {
					newFile = Files.createFile(filePath);

					logFile.addName(offset);
					logger.info("File created successfully!"+dir+localName+"/"+masterName+"/"+topic+"/"+fileName+".log");
				}
				else {
					newFile = filePath;
					logger.info("File already existed!");
				}
				out = new RandomAccessFile(newFile.toString(), "rw");
				
				logFile.setOutputStream(out);
				logFile.setFileSize(0L);
				logFile.setCurrentOffset(0L);
				if(!replicas)
					logFile.setMappedBB(out.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 650000));
				
				if(replicas) {
					HashMap<String, LogFile> hashm = mnames.get(masterName);
					hashm.put(topic, logFile);
				}
				else
				files.put(topic, logFile);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			return logFile;
	}

	@SuppressWarnings("unused")
	private void filesDurationCheck(String tpc) {
		int duration = properties.getDuration();
		long dur;
		if (duration > 0) {
			dur = duration * 3600000;
		}
		else dur = 3600000; // 1 hour.
		Iterator<String> topics = files.keySet().iterator();
		while (topics.hasNext()) {
			LogFile lf = files.get(topics.next());
			int i = 0;
			while (i < lf.getNames().size()) {
				long fn = lf.getNames().get(i);
				String dir = properties.getDirectory();
				Path filePath = Paths.get(dir+tpc+"/"+fn+".log");
				try {
					BasicFileAttributes attr = Files.readAttributes(filePath, BasicFileAttributes.class);
					if(System.currentTimeMillis() - attr.lastModifiedTime().toMillis() > dur) {
						Files.delete(filePath);
						lf.getNames().remove(i);
						i--;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				i++;
			}
			
		}
	}

	@SuppressWarnings("unused")
	private String getStringFromByteArray(byte[] message, int i, int j) {
		byte[] subMessage = null;
		if(message.length >= j) {
			subMessage= new byte[j-i+1];
			for(int k=0; k<subMessage.length; k++) {
				subMessage[k] = message[i];
				i++;
			}
		}
		return subMessage.toString();
	}


	public void closeFiles() {
		System.out.println("cvoicu: close files.");
		try {
			Iterator<LogFile> iter = files.values().iterator();
			while(iter.hasNext()) {
				LogFile logfile = iter.next();
				logfile.getMappedBB().force();
				int buffPos = logfile.getMappedBB().position();
				//System.out.println("cvoicu: buffer position: "+logfile.getMappedBB().position());
				logfile.getOutputStream().getChannel().truncate(logfile.getOutputStream().length()-650000 + logfile.getMappedBB().position());
				
				ByteBuffer mybuf = ByteBuffer.allocateDirect(buffPos);
				int indx = 0;
				while (indx < buffPos) {
				mybuf.put(logfile.getMappedBB().get(indx));
				
				indx++;
				}
				
				mybuf.position(0);
				if(mysend) {
					inproc.send(mytopic, ZMQ.SNDMORE);
					inproc.send(""+logfile.getOffset(), ZMQ.SNDMORE);
					inproc.send(""+buffPos, ZMQ.SNDMORE);
					inproc.send("false", ZMQ.SNDMORE);
					inproc.sendByteBuffer(mybuf, 0);
				}
				try {
					Thread.sleep(20000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if(mysend) {
					
				}
				logfile.getOutputStream().close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ByteBuffer load(String topic, long offset) {
		
		String dir = properties.getDirectory();
		
		String fileName = findFile(topic, offset);
		int messageLength;
		ByteBuffer message = null;
		
		if(fileName == null) {
			return null;
		}
		
		Path filePath = Paths.get(dir+topic+"/"+fileName+".log");
		if(!Files.exists(filePath)) {
			return null;
		}
		
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(filePath.toString(), "r");
			raf.seek(offset);
			messageLength = raf.readInt();
			message = ByteBuffer.allocateDirect(messageLength);
			int bytesRead = raf.getChannel().read(message);
			if(bytesRead != messageLength) {
				System.out.println("the message cannot be read from the file");
				raf.close();
				return null;
			}
			raf.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return message;
	}

	private String findFile(String topic, long offset) {
		
		long name;
		LogFile lf = files.get(topic);
		if(lf == null) {
			return null;
		}
		ArrayList<Long> names = lf.getNames();
		int i = 0;
		while(i < names.size()) {

			System.out.println("offset:"+offset+" names size: "+names.size() + " names.get(i)="+names.get(i)+" i="+i +"FileLog.getOffset:"+ files.get(topic).getOffset());
			if(i==names.size()-1) {
				if(names.get(i)<=offset & offset<files.get(topic).getOffset()) {
					name = names.get(i);

					return ""+name;
				}
				else {

					return null;
				}
			}
			else {
				if(names.get(i)<=offset & offset<names.get(i+1)) {
					name = names.get(i);

					return ""+name;
				}
			}
			i++;
		}
		return null;
	}
	public static void destroyDirectByteBuffer(ByteBuffer toBeDestroyed)
		    throws IllegalArgumentException, IllegalAccessException,
		    InvocationTargetException, SecurityException, NoSuchMethodException, NoSuchFieldException {


		Method cleanerMethod = toBeDestroyed.getClass().getMethod("cleaner");
		  cleanerMethod.setAccessible(true);
		  Object cleaner = cleanerMethod.invoke(toBeDestroyed);
		  Method cleanMethod = cleaner.getClass().getMethod("clean");
		  cleanMethod.setAccessible(true);
		  cleanMethod.invoke(cleaner);
		}
}