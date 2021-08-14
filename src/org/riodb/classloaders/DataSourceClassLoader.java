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

	Custom class loader to load JAR files when user creates a stream.
	The stream loads a datasource, which is typically a jar file. 
	This class loads such jar files. 

*/


package org.riodb.classloaders;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import org.riodb.engine.RioDB;
import org.riodb.sql.ExceptionSQLStatement;

import org.riodb.plugin.RioDBDataSource;


public class DataSourceClassLoader {

	public static RioDBDataSource getInputPlugin(String pluginName) throws ExceptionSQLStatement {

		RioDB.rio.getSystemSettings().getLogger().debug("Plugin factory loading "+pluginName);
		// Getting the jar URL which contains target class
		URL[] classLoaderUrls;

		String urlStr = "file:/" + RioDB.rio.getSystemSettings().getPluginDirectory() + pluginName.toLowerCase() + ".jar";
		urlStr = urlStr.replace("file://","file:/");
		
		URLClassLoader urlClassLoader = null;

		try {
			
			// Get the file location of the plugin to load.
			RioDB.rio.getSystemSettings().getLogger().debug("URL:   " + urlStr);
			
			// url of class to be loaded
			classLoaderUrls = new URL[] { new URL(urlStr) };

			// Create a new URLClassLoader
			urlClassLoader = new URLClassLoader(classLoaderUrls);

			// Load the target class
			@SuppressWarnings("unchecked")
			Class<RioDBDataSource> plugin = (Class<RioDBDataSource>)  urlClassLoader.loadClass("org.riodb.plugin." + pluginName.toUpperCase());

			
			// method 1
			RioDBDataSource dataSource = (RioDBDataSource) plugin.getDeclaredConstructor().newInstance();
			
			
			/*
			//  method 2
			 
	        Constructor<?> constructor = plugin.getConstructor();
	        ListenerInterface dataSource = (ListenerInterface) constructor.newInstance();
	         
	        
			dataSource.someInterfaceInit(settings);
			
	        
	        // Getting a method from the loaded class and invoke it
	        Method method = plugin.getMethod("sayHello");
	        method.invoke(plugin);
	        
			*/
			
			
			// return the loaded datasource object
			return dataSource;

		} catch (MalformedURLException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Unable to load plugin '"+ pluginName +"' due to MalformedURLException");
			throw new ExceptionSQLStatement(e.getMessage());
		} catch (ClassNotFoundException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Unable to load plugin '"+ pluginName +"' due to ClassNotFoundException");
			throw new ExceptionSQLStatement(e.getMessage());
		} catch (NoSuchMethodException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Unable to load plugin '"+ pluginName +"' due to NoSuchMethodException");
			throw new ExceptionSQLStatement(e.getMessage());
		} catch (SecurityException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Unable to load plugin '"+ pluginName +"' due to SecurityException");
			throw new ExceptionSQLStatement(e.getMessage());
		} catch (InstantiationException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Unable to load plugin '"+ pluginName +"' due to InstantiationException");
			throw new ExceptionSQLStatement(e.getMessage());
		} catch (IllegalAccessException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Unable to load plugin '"+ pluginName +"' due to IllegalAccessException");
			throw new ExceptionSQLStatement(e.getMessage());
		} catch (IllegalArgumentException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Unable to load plugin '"+ pluginName +"' due to IllegalArgumentException");
			throw new ExceptionSQLStatement(e.getMessage());
		} catch (InvocationTargetException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Unable to load plugin '"+ pluginName +"' due to InvocationTargetException");
			throw new ExceptionSQLStatement(e.getMessage());
		}  finally{
			if(urlClassLoader != null) {
				try {
					urlClassLoader.close();
				} catch (IOException e) {
					
				}
			}
		}
		
	}

}
