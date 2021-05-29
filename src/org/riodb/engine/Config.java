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
import org.apache.logging.log4j.core.LoggerContext;
import org.riodb.access.ExceptionAccessMgt;
import org.riodb.sql.ExceptionSQLStatement;
import org.riodb.sql.SQLExecutor;
import org.riodb.sql.SQLParser;

import org.riodb.plugin.RioDBPluginException;

//import org.apache.log4j.xml.DOMConfigurator;

public final class Config {

	// default config file location
	static final String DEFAULT_CONFIG_PATH = "conf/";
	static final String DEFAULT_CONFIG_FILE = "riodb.conf";
	static final String RQL_FILE_EXTENSION = "sql";

	public final static boolean loadConfig(String[] args) {

		boolean success = true;

		if (args.length > 0 && args[0].equals("-h")) {
			System.out.println("Welcome to RioDB. More information at www.riodb.co\n"
					+ "\n-h\tPrints help."
					+ "\n-f\tSpecifies configuration file location. Default is conf/riodb.conf"
					+ "\n-v\tPrints RioDB version."
					);
			return false;
		}
		if (args.length > 0 && args[0].equals("-v")) {
			System.out.println("RioDB version "+ RioDB.VERSION);
			return false;
		}
		
		String configFile = DEFAULT_CONFIG_PATH + DEFAULT_CONFIG_FILE;
		if (args.length > 1 && args[0].equals("-f")) {
			configFile = args[1];
		}

		Path filePath = Path.of(configFile);
	
		try {
			ArrayList<String> fileContent;
			fileContent =  (ArrayList<String>) Files.readAllLines(filePath);
			if (fileContent != null && fileContent.size() > 0) {
				success = runConfig(fileContent);
			}
			else {
				success = false;
				System.out.println("Error reading "+ configFile);
			}

		} catch (IOException e) {
			System.out.println("Error reading "+ configFile + "\n   "+e.getMessage());
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
		String httpsKeystorePwd  = "";
		String credentialsFile  = null;
		String sqlPath = DEFAULT_CONFIG_PATH;
		for(String line : fileContent) {
			line = line.replace('\t',' ');
			line = line.trim();
			while (line.contains("  ")) {
				line = line.replace("  ", " ");
			}
			if(line.length() > 0 && line.charAt(0) != '#' && line.contains(" ")) {
				String words[] = line.split(" ");
				if(words.length > 1) {
					if(words[1].contains("#")) {
						words[1] = words[1].substring(0, words[1].indexOf("#"));
					}
					else if(words[0].equals("log4j_properties")) {

						File file = new File(words[1]);
						LoggerContext context = (LoggerContext) LogManager.getContext(false);
						context.setConfigLocation(file.toURI());
						
						//DOMConfigurator.configure(words[1]);
						
						Thread.currentThread().setName("RIODB");
						
						Clock.quickPause();
					}
					if(words[0].equals("http_port")) {
						if(SQLParser.isNumber(words[1]))
							httpPort = Integer.valueOf(words[1]);
					}
					else if(words[0].equals("stmt_timeout")) {
						if(SQLParser.isNumber(words[1]))
							RioDB.rio.getHttpInterface().setTimeout(Integer.valueOf(words[1]));;
					}
					else if(words[0].equals("https_port")) {
						if(SQLParser.isNumber(words[1]))
							httpsPort = Integer.valueOf(words[1]);
					}
					else if(words[0].equals("https_keystore_file")) {
						httpsKeystoreFile = words[1];
					}
					else if(words[0].equals("https_keystore_pwd")) {
						httpsKeystorePwd = words[1];
					}
					else if(words[0].equals("credentials_file")) {
						credentialsFile = words[1];
					}
					else if(words[0].equals("sql_dir")) {
						sqlPath = words[1];
					}
				}
			}
			Clock.quickPause();
		}
		
		RioDB.rio.getLogger().info("RioDB "+ RioDB.VERSION +" - Copyright (c) 2021 www.riodb.co");
		RioDB.rio.getLogger().debug("Java VM name: "+System.getProperty("java.vm.name"));
		RioDB.rio.getLogger().debug("Java Version: "+System.getProperty("java.version"));
		RioDB.rio.getLogger().debug("Java Home: "+System.getProperty("java.home"));
		
		
		success = loadSQLFiles(sqlPath);
		Clock.quickPause();
		if(httpPort > 0) {
			if(credentialsFile != null) {
				RioDB.rio.getLogger().error("Security violation. Credentials cannot be used when HTTP API (unencrypted) is enabled.");
				return false;
			}
			success =  RioDB.rio.getHttpInterface().startHttp(httpPort);
			if(!success)
				return false;
		}
		Clock.quickPause();
		if(httpsPort > 0 && httpsPort != httpPort
				&& httpsKeystoreFile.length()>0
				&& httpsKeystorePwd.length()>0) {
			
			// load credentials configuration before opening ports. 
			if(credentialsFile !=  null) {
				try {
					RioDB.rio.setUserMgr(credentialsFile);
				} catch (ExceptionAccessMgt e) {
					RioDB.rio.getLogger().error("Loading Credentials File: "+ e.getMessage());
					return false;
				}
			}
			
			success =  RioDB.rio.getHttpInterface().startHttps(httpsKeystoreFile, httpsKeystorePwd, httpsPort);
			if(!success) {
				return false;
			}
		}
		Clock.quickPause();
		return success;
	}
	
	
	
	public final static boolean loadSQLFiles(String configPath) {

		boolean success = true;

		// Creates an array in which we will store the names of files and directories
		String[] sqlFiles;

		// Creates a new File instance by converting the given pathname string
		// into an abstract pathname
		File f = new File(configPath);

		// Populates the array with names of files and directories
		sqlFiles = f.list();

		// For each pathname in the pathnames array
		for (String file : sqlFiles) {
			// Print the names of files and directories
			if (file.toLowerCase().endsWith(("." + RQL_FILE_EXTENSION))) {

				Path fileName = Path.of(configPath + file);
				RioDB.rio.getLogger().info("Loading "+file+ " ...");

				try {

					String fileContent = Files.readString(fileName);

					if (fileContent != null && fileContent.contains(";")) {

						SQLExecutor.execute(fileContent, "SYSTEM");

					}
				} catch (FileNotFoundException e) {
					RioDB.rio.getLogger().error("Error loading SQL file. FILE NOT FOUND.");
					success = false;
					break;
				} catch (IOException e) {
					RioDB.rio.getLogger().error("Error loading configuration. " + e.getMessage().replace("\n", "\\n"));
					success = false;
					break;
				} catch (ExceptionSQLStatement e) {
					RioDB.rio.getLogger().error("Error loading sql command. " + e.getMessage().replace("\n", "\\n"));
					success = false;
					break;
				} catch (RioDBPluginException e) {
					RioDB.rio.getLogger().error("Error loading stream plugin. " + e.getMessage().replace("\n", "\\n"));
					success = false;
					break;
				} catch (ExceptionAccessMgt e) {
					RioDB.rio.getLogger().error("Error loading user access command. " + e.getMessage().replace("\n", "\\n"));
					success = false;
					break;
				}

			}
		}

		return success;

	}


}
