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
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import ch.qos.logback.classic.util.ContextInitializer;
import org.riodb.access.ExceptionAccessMgt;
import org.riodb.classloaders.OutputClassLoader;
import org.riodb.plugin.RioDBPlugin;
import org.riodb.sql.ExceptionSQLStatement;
import org.riodb.sql.SQLExecutor;
import org.riodb.sql.SQLParser;


public class SystemSettings {

	// default config file location
	private static final String DEFAULT_CONFIG_FILE = "riodb.conf";

	// default logback.xml file location
	private static final String DEFAULT_LOGBACK_FILE = "conf/logback.xml";

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

	// get persisted statements
	public PersistedStatements getPersistedStatements() {
		return persistedStatements;
	}

	// get plugin directory
	public String getPluginDirectory() {
		return pluginJarDirectory;
	}

	// logger object to be used by ALL RioDB
	private static Logger logger ;
	final static Marker fatal = MarkerFactory.getMarker("FATAL");
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
			if (javaRelativePath == null) {
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

		TreeMap<String, String> confProperties = readConfigFile(configFile);

		if(confProperties == null) {
			return false;
		}
		// if user started application with -v to see version
		if (getArgValue(args, "-v") != null) {
			System.out.println("RioDB version " + RioDB.VERSION);

			System.out.println("Reading config file: " + configFile);

			String pluginDir = confProperties.get("plugin_dir");
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

		if (runConfig(confProperties)) {
			Clock.sleep(10);
			return true;
		}

		return false;

	}

	// process config file lines:
	private static boolean runConfig(TreeMap<String, String> confProperties) {

		boolean success = true;
		int httpPort = 0;
		int httpsPort = 0;
		String httpsKeystorePwd = "";

		///////////////// CONFIG LOGGING /////////////////////

		String logbackXMLfile = DEFAULT_LOGBACK_FILE;
		if (confProperties.containsKey("logback_properties")) {

			logbackXMLfile = confProperties.get("logback_properties");
		}

		if (!logbackXMLfile.contains(":") && !logbackXMLfile.startsWith("/") && !logbackXMLfile.startsWith("\\")) {
			logbackXMLfile = javaRelativePath + "/" + logbackXMLfile;
		}
		logbackXMLfile = adaptPathSlashes(logbackXMLfile);

		//File file = new File(logbackXMLfile);
		//LoggerContext context = (LoggerContext) LogManager.getContext(false);
		//context.setConfigLocation(file.toURI());
		System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackXMLfile);
		logger = LoggerFactory.getLogger(RioDB.class.getName());

		// DOMConfigurator.configure(words[1]);

		Thread.currentThread().setName("RIODB");

		/*
		 * Per the license, the terminal must FIRST display an attribution to the
		 * copyright of riodb.org If the project is forked, this copyright notice must
		 * be preserved and displayed first. If embedded in a GUI application, the
		 * copyright notice must appear in an ABOUT page or similar.
		 */
		logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		logger.info(" _        _  _   RioDB " + RioDB.VERSION + " - Copyright (c) 2021 info at www.riodb.org");
		logger.info("|_) o  _ | \\|_)  This program comes with no warranty.");
		logger.info("| \\ | (_)|_/|_)  This is free software licensed under GPL-3.0 license.");
		logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		// logger.info("RioDB " + RioDB.VERSION + " - Copyright (c) 2021
		// www.riodb.org");
		logger.debug("Java VM name: " + System.getProperty("java.vm.name"));
		logger.debug("Java Version: " + System.getProperty("java.version"));
		logger.debug("Java Home: " + System.getProperty("java.home"));

		logger.debug("logback config file: " + logbackXMLfile);

		//////////// CONFIG OTHER PROPERTIES ////////////////////

		if (confProperties.containsKey("http_port")) {
			if (SQLParser.isNumber(confProperties.get("http_port"))
					&& Integer.valueOf(confProperties.get("http_port")) > 0) {
				httpPort = Integer.valueOf(confProperties.get("http_port"));
			} else {
				logger.error(fatal,"Configuration error: 'http_port' must be a positive integer.");
				return false;
			}
		}

		if (confProperties.containsKey("host")) {

			httpInterface.setSourceAddress(confProperties.get("host"));

		}

		if (confProperties.containsKey("stmt_timeout") && Integer.valueOf(confProperties.get("stmt_timeout")) > 0) {
			if (SQLParser.isNumber(confProperties.get("stmt_timeout"))) {
				httpInterface.setTimeout(Integer.valueOf(confProperties.get("stmt_timeout")));
			} else {
				logger.error(fatal,"Configuration error: 'stmt_timeout' must be a positive integer.");
				return false;
			}
		}

		if (confProperties.containsKey("https_port") && Integer.valueOf(confProperties.get("https_port")) > 0) {
			if (SQLParser.isNumber(confProperties.get("https_port"))) {
				httpsPort = Integer.valueOf(confProperties.get("https_port"));
			} else {
				logger.error(fatal,"Configuration error: 'https_port' must be a positive integer.");
				return false;
			}
		}

		if (confProperties.containsKey("https_keystore_file")) {
			sslCertFile = confProperties.get("https_keystore_file");
		}

		if (confProperties.containsKey("https_keystore_pwd")) {
			httpsKeystorePwd = confProperties.get("https_keystore_pwd");
		}

		if (confProperties.containsKey("credentials_file")) {
			passwdFile = confProperties.get("credentials_file");
		}

		if (confProperties.containsKey("sql_dir")) {
			sqlDirectory = confProperties.get("sql_dir");
		}

		if (confProperties.containsKey("plugin_dir")) {
			pluginJarDirectory = confProperties.get("plugin_dir");
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
			if (file.toLowerCase().endsWith(("." + RQL_FILE_EXTENSION))) {

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

	// Used to ready the Config file and return it as a string.
	// returns null if the config-file path is invalid. 
	private static TreeMap<String, String> readConfigFile(String configFile) {

		if (configFile == null) {
			return null;
		}
		Path filePath = Path.of(configFile);
		try {
			ArrayList<String> fileContent;
			// read conf file lines:
			fileContent = (ArrayList<String>) Files.readAllLines(filePath);
			if (fileContent == null || fileContent.size() == 0) {
				return null;
			}

			TreeMap<String, String> properties = new TreeMap<String, String>();

			for (String line : fileContent) {
				if (line != null && (line.contains(" ") || line.contains("\t"))) {
					line = line.replace("\t", " ");
					while (line.contains("  ")) {
						line = line.replace("  ", " ");
					}
					line = line.trim();
					if (line.contains("#")) {
						line = line.substring(0, line.indexOf("#")).trim();
					}
					String parts[] = line.split(" ");
					if (parts.length > 1 && parts[1].length() > 0) {
						properties.put(parts[0], parts[1]);
//						System.out.println("property: " + parts[0] + " value: " + parts[1]);
					}
				}
			}

			return properties;

		} catch (IOException e) {
			System.out.println("Error reading config file. " + e.getMessage());
		}
		return null;

	}

	/// print plugin versions when arg -v is used.
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

		if (jarFiles.length == 0) {
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
					System.out.println(
							"Error checking plugin '" + fileName + "' : invalid plugin or incompatible version.");
				}

			}

		}
	}

	private String getArgValue(String args[], String key) {

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				if (i < args.length - 1) {
					return args[i + 1];
				}
				return "true";
			}
		}
		return null;
	}

}
