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

/*

	A class for managing system settings, such as default directory paths
	
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
import org.riodb.classloaders.OutputClassLoader;
import org.riodb.plugin.RioDBPlugin;
import org.riodb.sql.ExceptionSQLStatement;
import org.riodb.sql.SQLExecutor;
import org.riodb.sql.SQLParser;

//import org.apache.log4j.xml.DOMConfigurator;

public class SystemSettings {

	// default config file location
	private static final String DEFAULT_CONFIG_FILE = "riodb.conf";

	// default config file subdirectory
	private static final String DEFAULT_CONFIG_SUBDIR = "conf";

	// default plugin jar file subdirectory
	private static final String DEFAULT_PLUGIN_SUBDIR = "plugins";

	// default sql file subdirectory
	private static final String DEFAULT_SQL_SUBDIR = "sql";

	// default sql file extension
	private static final String RQL_FILE_EXTENSION = "sql";

	// default ssl cert file
	private static final String DEFAULT_SSL_CERT_FILE = ".ssl/keystore.jks";

	// default password file
	// static final String DEFAULT_PASSWD_FILE = null; //".access/users.dat";
	// default persisted statements file
	private static final String DEFAULT_PERSISTED_STMT_FILE_NAME = "apistmt";

	// Directory path of where RioDB is running
	private static String javaRelativePath;

	// permanent plugin jar file directory path
	private static String pluginJarDirectory;

	// permanent SQL directory path
	private static String sqlDirectory;

	// permanent SQL directory path
	private static String persistedStatementsFile;

	// permanent password file path
	private static String passwdFile = null; // DEFAULT_PASSWD_FILE;

	// permanent ssl cert file path
	private static String sslCertFile = DEFAULT_SSL_CERT_FILE;

	// persist user statements to disk, and recover them on reboot/startup
	private static PersistedStatements persistedStatements;

	// default number of Threads allocated for executing output actions:
	private static int output_worker_threads = 1;

	public PersistedStatements getPersistedStatements() {
		return persistedStatements;
	}

	public String getPluginDirectory() {
		return pluginJarDirectory;
	}

	// logger object to be used by ALL RioDB
	final static Logger logger = LogManager.getLogger(RioDB.class.getName());

	// getter for logger.
	public Logger getLogger() {
		return logger;
	}

	// HTTP interface server (to receive SQL statement requests)
	private final static HTTPInterface httpInterface = new HTTPInterface();

	// getter for httpInterface
	public HTTPInterface getHttpInterface() {
		return httpInterface;
	}

	// constructor
	public SystemSettings() {
		//
	}

	// Method for loading configuration from .conf file
	public final boolean loadConfig(String[] args) {

		boolean success = true;

		// if started application with -h for help
		if (getArgValue(args, "-h") != null) {
			System.out.println("Welcome to RioDB. More information at www.riodb.org\n" + "\n-h\tPrints help."
					+ "\n-f\tSpecifies configuration file location. Default is conf/riodb.conf"
					+ "\n-v\tPrints RioDB version.");
			return false;
		}

		try {
			javaRelativePath = new File(".").getCanonicalPath();
//			if(javaRelativePath != null ){
//				if(javaRelativePath.contains("/")){
//					javaRelativePath = javaRelativePath.substring(0,javaRelativePath.lastIndexOf("/"));
//				} else if (javaRelativePath.contains("\\")){
//					javaRelativePath = javaRelativePath.substring(0,javaRelativePath.lastIndexOf("\\"));
//				}
//			} else {
			if(javaRelativePath == null ){
				System.out.println("Error obtaining directory where riodb.jar is running from: ");
				return false;
			}
		} catch (IOException e1) {
			System.out.println("Error obtaining directory where riodb.jar is running from: ");
			e1.printStackTrace();
			return false;
		}

		// set default directories:
		sqlDirectory = javaRelativePath + "/" + DEFAULT_SQL_SUBDIR;
		pluginJarDirectory = javaRelativePath + "/" + DEFAULT_PLUGIN_SUBDIR;
		sslCertFile = javaRelativePath + "/" + DEFAULT_SSL_CERT_FILE;

		// config file:
		String configFile = javaRelativePath + "/" + DEFAULT_CONFIG_SUBDIR + "/" + DEFAULT_CONFIG_FILE;
		
		configFile = adaptPathSlashes(configFile);

		// if user started application with -f to specify a config file location:
		String userConfigFile = getArgValue(args, "-f");
		if (userConfigFile != null) {
			configFile = userConfigFile;
		}

		// If config file path does not start from OS root directory:
		if (!configFile.contains(":") && !configFile.startsWith("/") && !configFile.startsWith("\\")) {
			// prefix it with a relative path
			configFile = javaRelativePath + "/" + configFile;
		}
		configFile = adaptPathSlashes(configFile);

		// if user started application with -v to see version
		if (getArgValue(args, "-v") != null) {
			System.out.println("RioDB version " + RioDB.VERSION);

			System.out.println("Reading config file: " + configFile);

			String pluginDir = getConfigFileProperty(configFile, "plugin_dir");
			if (pluginDir != null) {
				pluginJarDirectory = pluginDir;
			}
			// format plugin directory path
			if (!pluginJarDirectory.endsWith("/") && !pluginJarDirectory.endsWith("\\")) {
				pluginJarDirectory = pluginJarDirectory + "/";
			}
			pluginJarDirectory = adaptPathSlashes(pluginJarDirectory);
			System.out.println("Plugin Directory: " + pluginJarDirectory);
			printPluginVersions();
			return false;
		}

		// open config file.
		Path filePath = Path.of(configFile);
		try {
			ArrayList<String> fileContent;
			// read conf file lines and do what's needed:
			fileContent = (ArrayList<String>) Files.readAllLines(filePath);
			if (fileContent != null && fileContent.size() > 0) {
				success = runConfig(fileContent);
				logger.info("Loaded config file: " + configFile);
			} else {
				success = false;
				System.out.println("Error reading " + configFile);
			}

		} catch (IOException e) {
			System.out.println("Error reading " + configFile + " -   " + e.getMessage());
			success = false;
		}

		// quick pause after loading conf, for object to finish initializing
		Clock.sleep(10);

		return success;

	}

	// process config file lines:
	private static boolean runConfig(ArrayList<String> fileContent) {

		boolean success = true;
		int httpPort = 0;
		int httpsPort = 0;
		String httpsKeystorePwd = "";

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
						if (!log4j2XMLfile.contains(":") && !log4j2XMLfile.startsWith("/")
								&& !log4j2XMLfile.startsWith("\\")) {
							log4j2XMLfile = javaRelativePath + "/" + log4j2XMLfile;
						}
						log4j2XMLfile = adaptPathSlashes(log4j2XMLfile);
						
						File file = new File(log4j2XMLfile);
						LoggerContext context = (LoggerContext) LogManager.getContext(false);
						context.setConfigLocation(file.toURI());

						// DOMConfigurator.configure(words[1]);

						Thread.currentThread().setName("RIODB");

						/*
						 * Per the license, the terminal must FIRST display an attribution to the
						 * copyright of riodb.org If the project is forked, this copyright notice must
						 * be preserved and displayed first. If embedded in a GUI application, the
						 * copyright notice must appear in an ABOUT page or similar.
						 */
						logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
						logger.info(" _        _  _   RioDB " + RioDB.VERSION
								+ " - Copyright (c) 2021 info at www.riodb.org");
						logger.info("|_) o  _ | \\|_)  This program comes with no warranty.");
						logger.info("| \\ | (_)|_/|_)  This is free software licensed under GPL-3.0 license.");
						logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

						// logger.info("RioDB " + RioDB.VERSION + " - Copyright (c) 2021
						// www.riodb.org");
						logger.debug("Java VM name: " + System.getProperty("java.vm.name"));
						logger.debug("Java Version: " + System.getProperty("java.version"));
						logger.debug("Java Home: " + System.getProperty("java.home"));

						logger.debug("log4j2 config file: " + log4j2XMLfile);

						Clock.sleep(10);
					}
					if (words[0].equals("http_port")) {
						if (SQLParser.isNumber(words[1]) && Integer.valueOf(words[1]) > 0) {
							httpPort = Integer.valueOf(words[1]);
						} else {
							logger.fatal("Configuration error: 'http_port' must be a positive integer.");
							return false;
						}
					} else if (words[0].equals("stmt_timeout") && Integer.valueOf(words[1]) > 0) {
						if (SQLParser.isNumber(words[1])) {
							httpInterface.setTimeout(Integer.valueOf(words[1]));
						} else {
							logger.fatal("Configuration error: 'stmt_timeout' must be a positive integer.");
							return false;
						}
					} else if (words[0].equals("https_port") && Integer.valueOf(words[1]) > 0) {
						if (SQLParser.isNumber(words[1])) {
							httpsPort = Integer.valueOf(words[1]);
						} else {
							logger.fatal("Configuration error: 'https_port' must be a positive integer.");
							return false;
						}
					} else if (words[0].equals("https_keystore_file")) {
						sslCertFile = words[1];
					} else if (words[0].equals("https_keystore_pwd")) {
						httpsKeystorePwd = words[1];
					} else if (words[0].equals("credentials_file")) {
						passwdFile = words[1];
					} else if (words[0].equals("sql_dir")) {
						sqlDirectory = words[1];
					} else if (words[0].equals("plugin_dir")) {
						pluginJarDirectory = words[1];
					} else if (words[0].equals("output_workers")) {
						if (SQLParser.isNumber(words[1]) && Integer.valueOf(words[1]) > 0) {
							output_worker_threads = Integer.valueOf(words[1]);
							RioDB.rio.getEngine().setOutputWorkers(output_worker_threads);
						} else {
							logger.fatal("Configuration error: 'output_workers' must be a positive integer.");
							return false;
						}
					}
				}
			}
		}

		// format sqlDirectory path
		if (!sqlDirectory.endsWith("/") && !sqlDirectory.endsWith("\\")) {
			sqlDirectory = sqlDirectory + "/";
		}
		sqlDirectory = adaptPathSlashes(sqlDirectory);

		persistedStatementsFile = sqlDirectory + "." + DEFAULT_PERSISTED_STMT_FILE_NAME + "." + RQL_FILE_EXTENSION;
		persistedStatementsFile = adaptPathSlashes(persistedStatementsFile);

		// format plugin directory path
		if (!pluginJarDirectory.endsWith("/") && !pluginJarDirectory.endsWith("\\")) {
			pluginJarDirectory = pluginJarDirectory + "/";
		}
		pluginJarDirectory = adaptPathSlashes(pluginJarDirectory);

		logger.info("Plugin dir: " + pluginJarDirectory);
		logger.info("SQL file dir: " + sqlDirectory);
		logger.debug("Persistant stmt file: " + persistedStatementsFile);
		
		// Initialize statement persistance.
		persistedStatements = new PersistedStatements();

		success = loadSQLFiles();
		if (!success) {
			return false;
		}

		Clock.sleep(10);
		if (httpPort > 0) {
			if (passwdFile != null) {
				logger.error("Security violation. Credentials cannot be used when HTTP API (unencrypted) is enabled.");
				return false;
			}
			success = httpInterface.startHttp(httpPort);
			if (!success)
				return false;
		}
		Clock.sleep(10);
		if (httpsPort > 0 && httpsPort != httpPort && sslCertFile.length() > 0 && httpsKeystorePwd.length() > 0) {

			// load credentials configuration before opening ports.
			if (passwdFile != null) {

				if (!passwdFile.startsWith("/") && !passwdFile.startsWith("\\") && !passwdFile.contains(":")) {
					passwdFile = javaRelativePath + "/" + passwdFile;
				}
				passwdFile = adaptPathSlashes(passwdFile);
				logger.debug("Password file: " + passwdFile);

				try {
					RioDB.rio.setUserMgr(passwdFile);
				} catch (ExceptionAccessMgt e) {
					logger.error("Loading Credentials File: " + e.getMessage());
					return false;
				}
			}

			// format password file and ssl cert file paths:
			if (!sslCertFile.startsWith("/") && !sslCertFile.startsWith("\\") && !sslCertFile.contains(":")) {
				sslCertFile = javaRelativePath + "/" + sslCertFile;
			}
			sslCertFile = adaptPathSlashes(sslCertFile);
			logger.debug("SSL Cert file: " + sslCertFile);

			success = httpInterface.startHttps(sslCertFile, httpsKeystorePwd, httpsPort);
			if (!success) {
				return false;
			}
		}
		Clock.sleep(10);
		return success;
	}

	// Load initial SQL statements
	private final static boolean loadSQLFiles() {

		if (sqlDirectory == null) {
			return true;
		}

		boolean success = true;

		// Creates an array in which we will store the names of files and directories
		String[] sqlFiles;

		// Creates a new File instance by converting the given pathname string
		// into an abstract pathname
		
		File f = new File(sqlDirectory);

		// Populates the array with names of files and directories
		sqlFiles = f.list();

		// For each pathname in the pathnames array
		for (String file : sqlFiles) {
			
			// Print the names of files and directories
			if (true) {//file.toLowerCase().endsWith(("." + RQL_FILE_EXTENSION))) {

				String fileName = adaptPathSlashes(sqlDirectory + file);
				
				Path filePath = Path.of(fileName);
				logger.info("Loading " + fileName + " ...");

				try {

					String fileContent = Files.readString(filePath);

					if (fileContent != null && fileContent.contains(";")) {

						// queries from .apistmt.sql are supplied by user (API) and can be dropped
						// permanently.

						String loadOutput = "";
						if (persistedStatementsFile.equals(sqlDirectory + file)) {
							logger.debug("loading persisted statements");
							// true for persistStatement, false for respondWithDetails
							loadOutput = SQLExecutor.execute(fileContent, "SYSTEM", true, false);
							logger.info(loadOutput);
						}
						// queries from other .sql files can be dropped, but will execute again on
						// reboot.
						else {
							// false for persistStatement, false for respondWithDetails
							loadOutput = SQLExecutor.execute(fileContent, "SYSTEM", false, false); // permanent. Will
																									// always execute on
																									// reboot.
							logger.debug(loadOutput);
						}

						if (!loadOutput.contains("\"status\": 200")) {
							success = false;
							logger.error("Error loading configuration. " + fileName);
							break;
						}

					}
				} catch (FileNotFoundException e) {
					logger.error("Error loading SQL file. FILE NOT FOUND.");
					success = false;
					break;
				} catch (IOException e) {
					logger.error("Error loading configuration. " + e.getMessage().replace("\n", "\\n"));
					success = false;
					break;
				}
			}
		}

		return success;

	}

	// fix forward or back slashes based on OS
	private static String adaptPathSlashes(String path) {
		if (path == null) {
			return path;
		}
		String OS = System.getProperty("os.name").toLowerCase();
		if (OS.indexOf("win") >= 0) {
			return path.replace("/", "\\");
		}
		return path.replace("\\", "/");
	}

	public String getPersistedStmtFile() {
		return persistedStatementsFile;
	}

	// Used to get 1 property from the conf file:
	private static String getConfigFileProperty(String configFile, String propertyName) {

		Path filePath = Path.of(configFile);
		try {
			ArrayList<String> fileContent;
			// read conf file lines and do what's needed:
			fileContent = (ArrayList<String>) Files.readAllLines(filePath);
			if (fileContent == null || fileContent.size() == 0) {
				return null;
			}

			for (String line : fileContent) {
				if (line != null && line.contains("plugin_dir")) {

					if (!line.contains("#") || line.indexOf("#") > line.indexOf("plugin_dir")) {
						line = line.trim();
						String parts[] = line.split(" ");
						if (parts.length > 1 && parts[0].equals("plugin_dir") && !parts[1].startsWith("#")) {
							if (parts[1].contains("#")) {
								parts[1] = parts[1].substring(0, parts[1].indexOf("#"));
							}
							System.out.println(parts[1]);
							return parts[1];
						}
					}
				}
			}

		} catch (IOException e) {
			System.out.println("Error reading config file. " + e.getMessage());
		}
		return null;

	}

	private final static void printPluginVersions() {

		if (pluginJarDirectory == null) {
			System.out.println("plugin directory is NULL");
			return;
		}

		// Creates an array in which we will store the names of files and directories
		String[] jarFiles;

		// Creates a new File instance by converting the given pathname string
		// into an abstract pathname
		File f = new File(pluginJarDirectory);

		// Populates the array with names of files and directories
		jarFiles = f.list();
		
		if(jarFiles.length == 0) {
			System.out.println("No plugins in '" + pluginJarDirectory);
			return;
		} else {
			System.out.println("Plugins:");
		}

		// For each pathname in the pathnames array
		for (String file : jarFiles) {
			// Print the names of files and directories
			String path = adaptPathSlashes(pluginJarDirectory + file);
			String fileName = path.toLowerCase();

			if (fileName.endsWith(".jar")) {
				fileName = fileName.substring(0, fileName.length() - 4);
				if (fileName.contains("/") && fileName.lastIndexOf("/") < fileName.length()) {
					fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
				}
				if (fileName.contains("\\") && fileName.lastIndexOf("\\") < fileName.length()) {
					fileName = fileName.substring(fileName.lastIndexOf("\\") + 1);
				}
					try {
						RioDBPlugin plugin = OutputClassLoader.getOutputPlugin(fileName);
						System.out.println(plugin.getType() + " " + plugin.version());
					} catch (ExceptionSQLStatement | java.lang.NoClassDefFoundError e) {
						System.out.println("Error checking plugin '" + fileName + "' : " + e.getMessage());
					} catch (java.lang.AbstractMethodError e) {
						System.out.println("Error checking plugin '" + fileName + "' : invalid plugin or incompatible version.");
					}
				

			}

		}
	}
	
	private String getArgValue(String args[], String key) {
		
		for(int i = 0; i < args.length; i++) {
			if(args[i].equals(key)) {
				if(i < args.length-1) {
					return args[i+1];
				}
				return "true";
			}
		}
		return null;
	}

}
