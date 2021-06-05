/*
 	Copyright (c) 2021 Lucio D Matos,  www.riodb.org
 
    This file is part of RioDB
    
    RioDB is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    any later version.

    RioDB is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    A copy of the GNU General Public License should be found in the root
    directory. If not, see <https://www.gnu.org/licenses/>.
 
*/

package org.riodb.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.riodb.access.ExceptionAccessMgt;
import org.riodb.sql.ExceptionSQLStatement;
import org.riodb.sql.SQLExecutor;
import org.riodb.sql.SQLParser;

import org.riodb.plugin.RioDBPluginException;

//import org.apache.log4j.xml.DOMConfigurator;

public class SystemSettings {

	// default config file location
	static final String DEFAULT_CONFIG_FILE = "riodb.conf";
	static final String DEFAULT_CONFIG_SUBDIR = "conf/";
	static final String DEFAULT_PLUGIN_SUBDIR = "plugins/";
	static final String DEFAULT_SQL_SUBDIR = "sql/";
	static final String RQL_FILE_EXTENSION = "sql";

	private static String pluginJarDirectory;

	public String getPluginDirectory() {
		return pluginJarDirectory;
	}

	// logger
	final static Logger logger = LogManager.getLogger(RioDB.class.getName());

	public Logger getLogger() {
		return logger;
	}

	// HTTP interface server (to receive SQL statement requests)
	private final static HTTPInterface httpInterface = new HTTPInterface();

	public HTTPInterface getHttpInterface() {
		return httpInterface;
	}

	// constructor
	public SystemSettings() {
		//
	}

	
	public final boolean loadConfig(String[] args) {

		boolean success = true;

		if (args.length > 0 && args[0].equals("-h")) {
			System.out.println("Welcome to RioDB. More information at www.riodb.org\n" + "\n-h\tPrints help."
					+ "\n-f\tSpecifies configuration file location. Default is conf/riodb.conf"
					+ "\n-v\tPrints RioDB version.");
			return false;
		}
		if (args.length > 0 && args[0].equals("-v")) {
			System.out.println("RioDB version " + RioDB.VERSION);
			return false;
		}

		String configFile = System.getProperty("user.home") + "/" + DEFAULT_CONFIG_SUBDIR + DEFAULT_CONFIG_FILE;
		configFile = adaptPathSlashes(configFile);

		if (args.length > 1 && args[0].equals("-f")) {
			configFile = args[1];
		}

		Path filePath = Path.of(configFile);

		try {
			ArrayList<String> fileContent;
			fileContent = (ArrayList<String>) Files.readAllLines(filePath);
			if (fileContent != null && fileContent.size() > 0) {
				success = runConfig(fileContent);
			} else {
				success = false;
				System.out.println("Error reading " + configFile);
			}

		} catch (IOException e) {
			System.out.println("Error reading " + configFile + "\n   " + e.getMessage());
			success = false;
		}

		Clock.quickPause();

		return success;

	}

	private static boolean runConfig(ArrayList<String> fileContent) {
		
		boolean success = true;
		int httpPort = 0;
		int httpsPort = 0;
		String httpsKeystoreFile = "";
		String httpsKeystorePwd = "";
		String credentialsFile = null;
		String sqlDirectory = System.getProperty("user.home") + "/" + DEFAULT_SQL_SUBDIR;
		pluginJarDirectory = System.getProperty("user.home") + "/" + DEFAULT_PLUGIN_SUBDIR;
		
		for (String line : fileContent) {
			line = line.replace('\t', ' ');
			line = line.trim();
			while (line.contains("  ")) {
				line = line.replace("  ", " ");
			}
			if (line.length() > 0 && line.charAt(0) != '#' && line.contains(" ")) {
				String words[] = line.split(" ");
				if (words.length > 1) {
					if (words[1].contains("#")) {
						words[1] = words[1].substring(0, words[1].indexOf("#"));
					} else if (words[0].equals("log4j_properties")) {
						
						String log4j2XMLfile = words[1];
						if(!log4j2XMLfile.contains(":") && !log4j2XMLfile.startsWith("/")) {
							log4j2XMLfile = System.getProperty("user.home") + "/" + log4j2XMLfile;
						}
						log4j2XMLfile = adaptPathSlashes(log4j2XMLfile);
						
						File file = new File(words[1]);
						LoggerContext context = (LoggerContext) LogManager.getContext(false);
						context.setConfigLocation(file.toURI());

						// DOMConfigurator.configure(words[1]);

						Thread.currentThread().setName("RIODB");

						Clock.quickPause();
					}
					if (words[0].equals("http_port")) {
						if (SQLParser.isNumber(words[1]))
							httpPort = Integer.valueOf(words[1]);
					} else if (words[0].equals("stmt_timeout")) {
						if (SQLParser.isNumber(words[1]))
							httpInterface.setTimeout(Integer.valueOf(words[1]));
						;
					} else if (words[0].equals("https_port")) {
						if (SQLParser.isNumber(words[1]))
							httpsPort = Integer.valueOf(words[1]);
					} else if (words[0].equals("https_keystore_file")) {
						httpsKeystoreFile = words[1];
					} else if (words[0].equals("https_keystore_pwd")) {
						httpsKeystorePwd = words[1];
					} else if (words[0].equals("credentials_file")) {
						credentialsFile = words[1];
					} else if (words[0].equals("sql_dir")) {
						sqlDirectory = words[1];
					} else if (words[0].equals("plugin_dir")) {
						pluginJarDirectory = words[1];
					}
				}
			}
			Clock.quickPause();
		}
		
		sqlDirectory = adaptPathSlashes(sqlDirectory);
		pluginJarDirectory = adaptPathSlashes(pluginJarDirectory);
		
		logger.info("RioDB " + RioDB.VERSION + " - Copyright (c) 2021 www.riodb.org");
		logger.debug("Java VM name: " + System.getProperty("java.vm.name"));
		logger.debug("Java Version: " + System.getProperty("java.version"));
		logger.debug("Java Home: " + System.getProperty("java.home"));
		logger.debug("Plugin dir: " + pluginJarDirectory);
		logger.debug("SQL file dir: " + sqlDirectory);

		success = loadSQLFiles(sqlDirectory);
		if(!success) {
			return false;
		}
		
		Clock.quickPause();
		if (httpPort > 0) {
			if (credentialsFile != null) {
				logger.error(
						"Security violation. Credentials cannot be used when HTTP API (unencrypted) is enabled.");
				return false;
			}
			success = httpInterface.startHttp(httpPort);
			if (!success)
				return false;
		}
		Clock.quickPause();
		if (httpsPort > 0 && httpsPort != httpPort && httpsKeystoreFile.length() > 0 && httpsKeystorePwd.length() > 0) {

			// load credentials configuration before opening ports.
			if (credentialsFile != null) {
				try {
					RioDB.rio.setUserMgr(credentialsFile);
				} catch (ExceptionAccessMgt e) {
					logger.error("Loading Credentials File: " + e.getMessage());
					return false;
				}
			}

			success = httpInterface.startHttps(httpsKeystoreFile, httpsKeystorePwd, httpsPort);
			if (!success) {
				return false;
			}
		}
		Clock.quickPause();
		return success;
	}

	public final static boolean loadSQLFiles(String sqlDirectory) {
		
		if(sqlDirectory == null)
			return true;

		boolean success = true;

		// Creates an array in which we will store the names of files and directories
		String[] sqlFiles;

		// Creates a new File instance by converting the given pathname string
		// into an abstract pathname
		File f = new File(sqlDirectory);

		if(f==null || f.length() == 0) {
			logger.warn("SQL directory '"+ sqlDirectory +"' not found.");
			return true;
		}
		
		// Populates the array with names of files and directories
		sqlFiles = f.list();

		// For each pathname in the pathnames array
		for (String file : sqlFiles) {
			// Print the names of files and directories
			if (file.toLowerCase().endsWith(("." + RQL_FILE_EXTENSION))) {
				
				Path fileName = Path.of(sqlDirectory + file);
				logger.info("Loading " + file + " ...");

				try {
					
					String fileContent = Files.readString(fileName);

					if (fileContent != null && fileContent.contains(";")) {

						SQLExecutor.execute(fileContent, "SYSTEM");

					}
				} catch (FileNotFoundException e) {
					logger.error("Error loading SQL file. FILE NOT FOUND.");
					success = false;
					break;
				} catch (IOException e) {
					logger.error("Error loading configuration. " + e.getMessage().replace("\n", "\\n"));
					success = false;
					break;
				} catch (ExceptionSQLStatement e) {
					logger.error("Error loading sql command. " + e.getMessage().replace("\n", "\\n"));
					success = false;
					break;
				} catch (RioDBPluginException e) {
					logger.error("Error loading stream plugin. " + e.getMessage().replace("\n", "\\n"));
					success = false;
					break;
				} catch (ExceptionAccessMgt e) {
					logger.error("Error loading user access command. " + e.getMessage().replace("\n", "\\n"));
					success = false;
					break;
				}

			}
		}

		return success;

	}
	
	// fix forward or back slashes based on OS
	private static String adaptPathSlashes(String path) {
		if(path == null) {
			return path;
		}
		String OS = System.getProperty("os.name").toLowerCase();
		if(OS.indexOf("win") >= 0) {
			return path.replace("/","\\");
		}
		return path.replace("\\","/");
	}

}
