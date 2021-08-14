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
 *    The Engine is a container of:
 *       array of Stream (stores all Streams initialized by this RioDB instance)
 *       a final Clock (a global way to obtain the current rounded second 
 *       a final AtomicInteger counter (for ensuring unique naming of objects down the road)
 *        
 *    Engine is basically the top level orchestrator of RioDB
 */

package org.riodb.engine;

import java.util.concurrent.atomic.AtomicInteger;

import org.riodb.sql.ExceptionSQLStatement;

import org.riodb.plugin.RioDBPluginException;

public class Engine {

	/*
	 *   Stream array:
	 *   For performance reasons, we're not using ArrayList. Just a simple array of objects. 
	 *   This means that adding/removing require rebuilding the array. 
	 * 
	 */
	private Stream streams[];

	// constructor
	Engine() {
		streams = new Stream[0];
		online = false;
	}

	// online/offline flag
	private boolean online;
	// get online flag
	public boolean isOnline() {
		return online;
	}

	// Internal system clock
	private final Clock clock = new Clock();
	// getter for clock
	public final Clock getClock() {
		return clock;
	}

	// counter to ensure dynamic objects are always created with unique name;
	// It is atomic in case we have multiple threads updating it.
	private final AtomicInteger globalCounter = new AtomicInteger(0);
	// getter to increment and get the next counter value
	public int counterNext() {
		return globalCounter.incrementAndGet();
	}

	// Method for adding a new Stream
	// TODO: Security improvement to check requester permission here since the method is public
	public boolean addStream(Stream newStream) throws ExceptionSQLStatement, RioDBPluginException {

		// create a new array that has length + 1 to accommodate new Stream
		Stream newStreamArray[] = new Stream[streams.length + 1];
		for (int i = 0; i < streams.length; i++) {
			if (newStream.getName().equals(streams[i].getName()))
				// duplicate name
				return false;
			newStreamArray[i] = streams[i];
		}
		// add the new stream to the new stream Array
		newStreamArray[streams.length] = newStream;
		// replace old streams array with new (longer) stream array
		streams = newStreamArray;
		return true;
	}

	// describe an object
	public String describe(String objectToDescribe) {

		// if the object to describe is in streamName.windowName format, it's a window
		if (objectToDescribe.contains(".") && objectToDescribe.indexOf('.') < (objectToDescribe.length() - 2)) {
			String stream = objectToDescribe.substring(0, objectToDescribe.indexOf("."));
			String window = objectToDescribe.substring((objectToDescribe.indexOf(".") + 1));

			for (int i = 0; i < streams.length; i++) {
				if (streams[i] != null) {
					if (streams[i].getName().equals(stream)) {
						return streams[i].describeWindow(window);
					}
				}
			}
		} 
		// else, the object to describe is a stream.
		else {
			for (int i = 0; i < streams.length; i++) {
				if (streams[i] != null) {
					if (streams[i].getName().equals(objectToDescribe)) {
						return streams[i].describe();
					}
				}
			}
		}
		// if nothing was found...
		return "";
	}

	// get Stream by ID
	public Stream getStream(int index) {
		return streams[index];
	}

	// get Stream by Name
	public Stream getStream(String name) {
		if (name == null)
			return null;

		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if (name.equals(streams[i].getName())) {
					return streams[i];
				}
			}
		}

		return null;
	}

	// get Stream ID by Name
	public int getStreamId(String name) {
		if (name == null)
			return -1;

		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if (name.equals(streams[i].getName())) {
					return i;
				}
			}
		}

		return -1;
	}

	// count of streams
	public int getStreamCount() {
		int counter = 0;
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				counter++;
			}
		}
		return counter;
	}
	
	// gets the streamId of a window named... otherwise -1 if not found.
	public int getStreamIdOfWindow(String windowName) {
		if (streams.length == 0) {
			return -1;
		}
		for (int i = 0; i < streams.length; i++) {
			if (streams[i].getWindowMgr().hasWindow(windowName))
				return i;
		}
		return -1;
	}

	// get all Windows in a string
	public String listAllQueries() {
		String response = "[ ";
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if (i > 0) {
					response = response + ",";
				}
				response = response + streams[i].listAllQueries();
			}
		}
		response = response + "\n]";
		return response;
	}

	// get all Windows in a string
	public String listAllWindows() {
		String response = "[ ";
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if (i > 0) {
					response = response + ",";
				}
				response = response + streams[i].getWindowMgr().listAllWindows();
			}

		}
		response = response + "\n]";
		return response;
	}
	

	// get all streams in a string
	public String listStreams() {
		String response = "[ ";
		int notNullStreams = 0;
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if (notNullStreams++ > 0) {
					response = response + " , ";
				}
				notNullStreams++;

				response = response + "\"" + streams[i].getName() + "\"";
			}
		}
		response = response + " ]";
		return response;
	}

	/*
	// print counters for debugging
	public void printMsgCounters() {
		String s = "";
		for (int i = 0; i < streams.length; i++) {
			s = "\t" + "stream[" + i + "]: " + streams[i].getMsgCounter();
		}
		System.out.println(s);
	}
	*/

	/* 
	   remove a stream from array
	   we are not resizing the array. Only setting the decommissioned
	   stream slot to null. 
	*/
	public boolean removeStream(int streamId) {

		if (streamId >= 0 && streamId < streams.length) {
			RioDB.rio.getSystemSettings().getLogger().debug("Stopping stream.");
			streams[streamId].stop();
			Clock.quickPause();
			RioDB.rio.getSystemSettings().getLogger().debug("erasing stream.");
			streams[streamId] = null;
			Clock.quickPause();
			return true;
		}
		return false;
	}

	// start all stream processes
	public void start() {
		online = true;
		clock.start();
		Clock.quickPause();
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				streams[i].start();
			}
		}
	}

	// start ONE stream process
	public boolean start(int streamId) {
		if(!online) {
			return false;
		}
		clock.start();
		Clock.quickPause();
		if (streamId < streams.length) {
			streams[streamId].start();
		}
		return true;
	}

	// provide a system status string in JSON format
	public String status() {

		String stat = "offline";
		if(online) {
			stat = "online";
		}
			
		String response = "{\n   \"system_status\": \""+ stat +"\",\n   \"streams\": [\n   ";
		boolean first = true;
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if(first) {
					first = false;
				}
				else {
					response = response + ",";
				}
				response = response + "\n " + streams[i].status();
			}
		}
		response = response + "\n   ]\n}";
		return response;
	}

	// stop all stream processes
	public void stop() {
		online = false;

		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				streams[i].stop();
			}
		}
		
		RioDB.rio.getSystemSettings().getLogger().debug("RioDB is offline.");
	}

	// stop ONE stream process
	public void stop(int streamId) {
		if (streamId < streams.length) {
			streams[streamId].stop();
		}
		Clock.quickPause();
		clock.start();
		Clock.quickPause();
		RioDB.rio.getSystemSettings().getLogger().debug("RioDB is online.");
	}

	public void trimExpiredWindowElements(int currentSecond) {
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				streams[i].trimExpiredWindowElements(currentSecond);
			}
		}
	}


	/*
	 *  Drop a WIndow... 
	 */
	public boolean dropWindow(String windowName) {
		
		int streamId = getStreamIdOfWindow(windowName);
		
		if (streamId >=0 && streams[streamId].dropWindow(windowName)) {
					return true;
		}
		return false;
	}

	/*
	 *  Drop a query... 
	 *  We don't really now the address of a query here. So we loop through all
	 *  Streams asking each of them to drop the query if they find it. 
	 *  TODO: Possible enhancement to include StreamID has part of query ID:
	 *    1.5 would mean that query #5 belongs in Stream #1
	 *    Then, no more need for looping. 
	 */
	public boolean dropQuery(int queryId) {
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if (streams[i].dropQuery(queryId)) {
					return true;
				}
			}
		}
		return false;
	}
	
	// Check if any query depends on a stream
	public boolean hasQueryDependantOnStream(int streamId) {
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if (streams[i].hasQueryDependantOnStream(streamId)) {
					return true;
				}
			}
		}
		return false;
	}
	
	// Check if any query depends on a window
	public boolean hasQueryDependantOnWindow(int streamId, int windowId) {
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if (streams[i].hasQueryDependantOnWindow(streamId, windowId)) {
					return true;
				}
			}
		}
		return false;
	}
	
	// Check if any query depends on a stream
	public boolean hasWindowDependantOnStream(int streamId) {
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if (streams[i].hasWindowDependantOnStream(streamId)) {
					return true;
				}
			}
		}
		return false;
	}
	
}
