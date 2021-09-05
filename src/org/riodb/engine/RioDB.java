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

	RioDB is the MAIN.
	It declares the following:
		A final VERSION String
		SystemSettings object - for system variables
		an Engine - to orchestrate and run streams
		an UserManagemer object to manage user authentication and authorization
		
		Top level methods for start/pause/shutdown
		
*/

package org.riodb.engine;

import org.riodb.access.ExceptionAccessMgt;
import org.riodb.access.UserManager;
import org.riodb.plugin.RioDBPluginException;


public class RioDB {
	
	static final String VERSION = "v0.1";
	
	private final static SystemSettings settings = new SystemSettings();
	public SystemSettings getSystemSettings() {
		return settings;
	}
	
	
		// Engine that contains and runs streams, windows, queries, etc.
	private final Engine rioEngine = new Engine();
	public Engine getEngine() {
		return rioEngine;
	}
	
	// User manager for controlling authentication and privileges
	private static UserManager userMgr = null;
	public void setUserMgr(String credentialsFile) throws ExceptionAccessMgt {
		userMgr = new UserManager(credentialsFile);
	}
	public UserManager getUserMgr(){
		return userMgr;
	}
	
	// static self
	public static final RioDB rio = new RioDB();

	//RioDB() {}

	// start services
	public void start() throws RioDBPluginException {
		settings.getLogger().info("#############   Starting Engine...  #################");
		rioEngine.start();
		settings.getLogger().info("RioDB is ready");
	}

	// stop services
	private void pause() {
		settings.getLogger().info("#############   Stopping Engine...  #################");
		rioEngine.stop();
		Clock.quickPause();
	}

	// shutdown
	private void shutdown() {
		pause();
		Clock.quickPause();
		HTTPInterface.stop();
	}

	
	
	/// MAIN
	public static void main(String[] args) throws InterruptedException {
		

		/*
		 *  Per the license, the terminal must FIRST display an attribution to the copyright of riodb.org
		 *  If the project is forked, this copyright notice must be preserved and displayed first.
		 *  If turned into a GUI application, the copyright notice must appear in an ABOUT page or similar. 
		 */
		System.out.println(
				"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\r\n\n"+
				" _        _  _   RioDB "+ RioDB.VERSION +" - Copyright (c) 2021 info at www.riodb.org\r\n" + 
				"|_) o  _ | \\|_)  This program comes with no warranty.\r\n" + 
				"| \\ | (_)|_/|_)  This is free software licensed under GPL-3.0 license. \r\n\n"+
				"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");


		// Load initial configuration.
		if (!settings.loadConfig(args))
			System.exit(0);
		
		// Start services
		try {
			rio.start();
		} catch (RioDBPluginException e) {
			settings.getLogger().info("Error starting services: "+
		    e.getMessage().replace("\n", "").replace("\r", ""));
		}
		
		// creates a shutdown hook to catch kill request. 
		shutdownHook();

	}
	
	
	
	// shutdown hook to catch kill request. 
	private static void shutdownHook() throws InterruptedException {
		Runtime.getRuntime().addShutdownHook(new Thread("SHUTDOWN_HOOK") {
			@Override
			public void run() {
				rio.shutdown();
			}
		});
		while (true) {
			// just keep sleeping
			Thread.sleep(5000);
		}
	}

}
