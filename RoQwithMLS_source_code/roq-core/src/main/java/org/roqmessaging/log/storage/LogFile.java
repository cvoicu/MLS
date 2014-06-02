package org.roqmessaging.log.storage;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
/**
* @author Cristian Voicu
*/

public class LogFile {

	private RandomAccessFile output = null;
	private MappedByteBuffer mbb = null;
	private long fileSize = 0L;
	private long startOffset = 0L; 
	private long currentOffset = 0L;
	private long offset = 0L;
	
	private ArrayList<Long> names = new ArrayList<Long>();
	
	public LogFile(long offset) {
		this.offset = offset;
	}
	
	public ArrayList<Long> getNames() {
		return this.names;
	}
	
	public void setNames(ArrayList<Long> names) {
		this.names = names;
	}
	
	public RandomAccessFile getOutputStream() {
		return this.output;
	}

	public void setOutputStream(RandomAccessFile out) {
		this.output = out;
	}
	
	public MappedByteBuffer getMappedBB() {
		return this.mbb;
	}
	
	public void setMappedBB(MappedByteBuffer mbb) {
		this.mbb = mbb;
	}
	
	public long getFileSize() {
		return this.fileSize;
	}
	
	public long getStartOffset() {
		return this.startOffset;
	}
	
	public void setStartOffset(long startOffset) {
		this.startOffset = startOffset;
	}

	public long getOffset() {
		return this.offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}
	public void addName(long name) {
		this.names.add(name);
	}
	
	public long getCurrentOffset() {
		return this.currentOffset;
	}
	
	public void setCurrentOffset(long co) {
		this.currentOffset = co;
	}
	
}
