package org.roqmessaging.log.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
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
    
    private Socket inproc = null;
    
	public LogManagement(LogConfigDAO properties) throws IOException {
		this.properties = properties;
		
	}
	
	public LogFile getLogFile(String topic) {
		return files.get(topic);
	}

	public void putMessage(String topic, ByteBuffer message, Socket inproc, String nodeName) {
		this.inproc = inproc;
		try {
			store(nodeName, nodeName, topic, message, true, 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void store(String localName, String masterName, String topic, ByteBuffer message, boolean send, long of) throws IOException {
		LogFile logFile=null;
		RandomAccessFile out = null;
		
		if(files.containsKey(topic)) {
			logFile = files.get(topic);
			if(logFile.getFileSize()>properties.getMaxFileSize()) {
				logFile.getOutputStream().close();
				this.createFile(localName, masterName, topic, ""+logFile.getOffset(), logFile.getOffset(), logFile);
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
					logFile = createFile(localName, localName, topic, ""+offset, offset, logFile);
					out = logFile.getOutputStream();
				}
				else {
					logFile = this.createFile(localName, masterName, topic, "0", 0L, logFile);
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
				logFile = this.createFile(localName, masterName, topic, ""+of, of, logFile);
				out = logFile.getOutputStream();
				
			}
		}
		MappedByteBuffer bufferToSend = logFile.getMappedBB();
		message.position(0);
		if (bufferToSend.remaining() >= message.capacity()+4) {
			bufferToSend.putInt(message.capacity());
			bufferToSend.put(message);
		}
		else {
			try {
				bufferToSend.position(0);
				bufferToSend.force();
				if(send) {
					inproc.send(topic, ZMQ.SNDMORE);
					inproc.send(""+logFile.getOffset(), ZMQ.SNDMORE);
					inproc.send(""+bufferToSend.capacity(), ZMQ.SNDMORE);
					inproc.sendByteBuffer(bufferToSend, 0);
				}
				logFile.setCurrentOffset(logFile.getCurrentOffset()+(long)bufferToSend.capacity());
				logFile.setOffset(logFile.getOffset()+(long)bufferToSend.capacity());
				logFile.setFileSize(logFile.getFileSize()+(long)bufferToSend.capacity());
				logFile.setMappedBB(out.getChannel().map(FileChannel.MapMode.READ_WRITE, logFile.getCurrentOffset(), 65000));
			} catch (Exception e) {
				e.printStackTrace();
			}
			bufferToSend.clear();
			if (bufferToSend.remaining() < message.capacity()+4) { //the message exceeds the capacity of the buffer
																   //so it will be sent as is.
				//bufferToSend.force();
				logFile.setCurrentOffset(logFile.getCurrentOffset()+(long)bufferToSend.capacity());
				logFile.setOffset(logFile.getOffset()+(long)bufferToSend.capacity());
				logFile.setFileSize(logFile.getFileSize()+(long)bufferToSend.capacity());
				MappedByteBuffer newBuffer = out.getChannel().map(FileChannel.MapMode.READ_WRITE, logFile.getCurrentOffset(), message.capacity()+4);
				newBuffer.putInt(message.capacity());
				newBuffer.put(message);
				try {
					newBuffer.position(0);
					newBuffer.force();
					if(send) {
						inproc.send(topic, ZMQ.SNDMORE);
						inproc.send(""+logFile.getOffset(), ZMQ.SNDMORE);
						inproc.send(""+newBuffer.capacity(), ZMQ.SNDMORE);
						inproc.sendByteBuffer(newBuffer, 0);
						logFile.setCurrentOffset(logFile.getCurrentOffset()+(long)newBuffer.capacity());
						logFile.setOffset(logFile.getOffset()+(long)newBuffer.capacity());
						logFile.setFileSize(logFile.getFileSize()+(long)newBuffer.capacity());
						logFile.setMappedBB(out.getChannel().map(FileChannel.MapMode.READ_WRITE, logFile.getCurrentOffset(), 65000));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else {
				bufferToSend.putInt(message.capacity());
				bufferToSend.put(message);
			}
		}
	}
	
	private LogFile createFile(String localName, String masterName, String Mtopic, String fileName, long offset, LogFile logFile) throws IOException {
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
				out = new RandomAccessFile(newFile.toString(), "rw");//new FileOutputStream(newFile.toString(), true);
				
				logFile.setOutputStream(out);
				logFile.setFileSize(0L);
				logFile.setCurrentOffset(0L);
				logFile.setMappedBB(out.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 65000));
				files.put(topic, logFile);
				//filesDurationCheck(topic);
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
		try {
			Iterator<LogFile> iter = files.values().iterator();
			while(iter.hasNext()) {
				iter.next().getOutputStream().close();
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
}