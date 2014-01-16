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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.roqmessaging.log.LogConfigDAO;

/**
 * Class FileConfigurationReader
 * <p>
 * Description: Read the configuration from an apache commons configuration
 * 
 * @author sskhiri
 */
public class FileConfigurationReader {
	private Logger logger = Logger.getLogger(this.getClass().getCanonicalName());

	/**
	 * @param file
	 *            the GCM property file
	 * @return the GCM dao for properties
	 * @throws ConfigurationException
	 */

	public LogConfigDAO loadLogConfiguration(String propertyFile) throws ConfigurationException {
		LogConfigDAO configDao = new LogConfigDAO();
		try {
			PropertiesConfiguration config = new PropertiesConfiguration();
			config.load(propertyFile);
			configDao.setDirectory(config.getString("directory"));
			configDao.setMaxFileSize(config.getLong("maxFileSize"));
			configDao.setDuration(config.getInt("duration"));
			configDao.setReplicationFactor(config.getInt("replicationFactor"));
			configDao.setUseLog(config.getBoolean("useLog"));
		} catch(Exception e) {
			logger.error("Error while reading configuration file - skipped but set the default configuration", e);
		}
		
		return configDao;
	}

}
