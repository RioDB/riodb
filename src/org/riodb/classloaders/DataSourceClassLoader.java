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

		RioDB.rio.getLogger().debug("Plugin factory loading "+pluginName);
		// Getting the jar URL which contains target class
		URL[] classLoaderUrls;
		try {
			
			String urlStr = "file:/" +System.getProperty("user.dir").replace('\\','/') +  "/plugins/" + pluginName.toLowerCase() + ".jar";
			RioDB.rio.getLogger().debug("URL:   " + urlStr);
			classLoaderUrls = new URL[] { new URL(urlStr) };

			// Create a new URLClassLoader
			URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);

			// Load the target class
			@SuppressWarnings("unchecked")
			Class<RioDBDataSource> plugin = (Class<RioDBDataSource>)  urlClassLoader.loadClass("org.riodb.plugin." + pluginName.toUpperCase());

			
			// method 1
			RioDBDataSource li = (RioDBDataSource) plugin.getDeclaredConstructor().newInstance();
			
			
			// method 2
	        //Constructor<?> constructor = plugin.getConstructor();
	        //ListenerInterface li = (ListenerInterface) constructor.newInstance();
	         
	        
			//li.someInterfaceInit(settings);
			
	        
	        // Getting a method from the loaded class and invoke it
	        //Method method = plugin.getMethod("sayHello");
	        //method.invoke(plugin);

			urlClassLoader.close();

			return li;

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
		
		throw new ExceptionSQLStatement("error");
		
	}

}
