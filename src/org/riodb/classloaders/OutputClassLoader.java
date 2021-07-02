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

	A class loader to load custom output plugins. 
	Just like input plugins, output plugins can be in the form of jar files, 
	which are loaded when a user submits a query that points to such jar file. 
	The jar file is loaded by this class loader. 

*/

package org.riodb.classloaders;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import org.riodb.engine.RioDB;
import org.riodb.sql.ExceptionSQLStatement;

import org.riodb.plugin.RioDBOutput;

public class OutputClassLoader {

	public static RioDBOutput getOutputPlugin(String pluginName) throws ExceptionSQLStatement {

		// check name of the plugin required.
		if(pluginName == null || pluginName.length() == 0)
			throw new ExceptionSQLStatement("OUTPUT name was blank");
		
		pluginName = pluginName.toUpperCase().trim();
		
		RioDB.rio.getSystemSettings().getLogger().debug("Plugin factory loading '" + pluginName + "'");
		
		// STDOUT is an embedded plugin that does not require external jar file. 
		// it's built-in to RioDB
		if(pluginName.equals("STDOUT"))
			return new STDOUT();
		
		
		// Getting the jar URL which contains target class
		URL[] classLoaderUrls;
		try {

			// Get file location of Output plugin jar. 
			String urlStr = "file:/" + RioDB.rio.getSystemSettings().getPluginDirectory() + pluginName.toLowerCase() + ".jar";
			RioDB.rio.getSystemSettings().getLogger().debug("URL:   " + urlStr);
			classLoaderUrls = new URL[] { new URL(urlStr) };

			// Create a new URLClassLoader
			URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);

			// Load the target class
			@SuppressWarnings("unchecked")
			Class<RioDBOutput> plugin = (Class<RioDBOutput>) urlClassLoader
					.loadClass("org.riodb.plugin." + pluginName.toUpperCase());

			// method 1
			RioDBOutput outputPlugin = (RioDBOutput) plugin.getDeclaredConstructor().newInstance();

			urlClassLoader.close();

			// return the new output plugin class
			return outputPlugin;

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		throw new ExceptionSQLStatement("Unable to load output plugin.");

	}

}
