/**
 * Copyright 2012 EURANOVA
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package org.roqmessaging.log;

/**
 * Class HostConfigDAO
 * <p> Description: data object handling properties for the host configuration manager
 * 
 * @author sskhiri
 */
public class LogConfigDAO {
	
	private String directory = "/";
	private long maxFileSize = 1000L;
	private int duration = 50; // hours
	private int replicationFactor = 0;
	private boolean useLog = false;
	
	public String getDirectory() {
		return this.directory;
	}
	
	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public long getMaxFileSize() {
		return this.maxFileSize;
	}
	
	public void setMaxFileSize(long maxFileSize) {
		this.maxFileSize = maxFileSize;
	}

	public int getDuration() {
		return this.duration ;
	}
	public void setDuration(int duration) {
		this.duration = duration;
	}
	
	public boolean getUseLog() {
		return this.useLog;
	}
	
	public void setUseLog(boolean uselog) {
		this.useLog = uselog;
	}

	@Override
	public String toString() {
		return "Log configuration manager [useLog :"+useLog +"] ";
	}
	public int getReplicationFactor() {
		return replicationFactor;
	}
	public void setReplicationFactor(int rf) {
		this.replicationFactor = rf;
	}
}
