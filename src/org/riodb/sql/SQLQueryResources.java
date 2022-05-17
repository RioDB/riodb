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
 *  Resources (stream, windows, etc) used by a query.
 * 
 */
package org.riodb.sql;

import java.util.ArrayList;

import org.riodb.engine.RioDB;

public class SQLQueryResources {

	private ArrayList<SQLQueryResource> resources;
	private int drivingStreamId;
	private String drivingStreamAlias;

	SQLQueryResources(String fromStr) throws ExceptionSQLStatement {

		if (fromStr == null || fromStr.length() == 0) {
			throw new ExceptionSQLStatement("FROM - no resources specified. ");
		}

		drivingStreamId = -1;
		resources = new ArrayList<SQLQueryResource>();

		String[] fromResources = fromStr.split(",");

		for (int i = 0; i < fromResources.length; i++) {
			
			String code = fromResources[i].trim();
			String resourceName = code;
			String resourceAlias = code;
			if(code.contains(" ")) {
				resourceName = code.substring(0,code.indexOf(" ")).trim();
				resourceAlias  = code.substring(code.indexOf(" ")+1).trim();
			}
			
			if(SQLParser.isStreamName(resourceName)) {
				if(drivingStreamId != -1) {
					throw new ExceptionSQLStatement("Query can select from only ONE (explicit) driving stream.");
				}
				drivingStreamId = RioDB.rio.getEngine().getStreamId(resourceName);
				drivingStreamAlias = resourceAlias;
			}
			
			else if(SQLParser.isWindowName(resourceName)) {
				int windowStreamId = RioDB.rio.getEngine().getStreamIdOfWindow(resourceName);
				int windowId = RioDB.rio.getEngine().getStream(windowStreamId).getWindowMgr().getWindowId(resourceName);
				SQLQueryResource r = new SQLQueryResource(windowStreamId,windowId,code,resourceAlias);
				resources.add(r);	
			}
			else {
				throw new ExceptionSQLStatement("Resource not identified: "+ resourceName);
			}
		}
//		System.out.println(resources.size()+" resources");
		
		if(drivingStreamId == -1) {
			for(SQLQueryResource r : resources) {
				if(drivingStreamId == -1) {
					drivingStreamId = r.getStreamId();
					drivingStreamAlias = RioDB.rio.getEngine().getStream(drivingStreamId).getName();
				}
				else if(drivingStreamId != r.getStreamId()) {
					throw new ExceptionSQLStatement("Windows from different streams.\nConsider declaring a driving stream, such as 'SELECT ... FROM stocks s, window1 w1, window2 w1... '");
				}
			}
		}
	}

	public int getDrivingStreamId() {
		return drivingStreamId;
	}
	public String getDrivingStreamAlias() {
		return drivingStreamAlias;
	}

	public int countWindows() {
		return resources.size();
	}

	public int getResourceIdByAlias(String alias) {

		if (alias == null || alias.length() == 0)
			return -1;

		for (int i = 0; i < resources.size(); i++) {
			if (resources.get(i).getAlias().equals(alias))
				return i;
		}

		return -1;
	}
	
	public SQLQueryResource getResourceByAlias(String alias) {
		if (alias == null || alias.length() == 0)
			return null;

		for (SQLQueryResource r : resources) {
			if (r.getAlias().equals(alias))
				return r;
		}

		return null;
	}
	
	public SQLQueryResource getResourceById(int resourceId) {

		if (resourceId >= resources.size() || resourceId < 0)
			return null;

		return resources.get(resourceId);
	}
	
	public boolean containsWindowAlias(String alias) {

		for(SQLQueryResource r : resources) {
			if(r.getAlias().equals(alias)) {
				return true;
			}
		}
		return false;
	}
	
	public boolean containsStreamAlias(String alias) {
		if(drivingStreamAlias ==  null)
			return false;
		return drivingStreamAlias.equals(alias);
	}
	
	public boolean dependsOnStream(int streamId) {
		
		for(int i = 0; i < resources.size(); i++) {
			if(resources.get(i).getStreamId() == streamId) {
				return true;
			}	
		}
		
		return false;
	}
	
	public boolean dependsOnWindow(int streamId, int windowId) {
		
		for(int i = 0; i < resources.size(); i++) {
			if(resources.get(i).getStreamId() == streamId && resources.get(i).getWindowId() == windowId) {
				return true;
			}	
		}
		
		return false;
	}

}
