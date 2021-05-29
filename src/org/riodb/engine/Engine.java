package org.riodb.engine;

import java.util.concurrent.atomic.AtomicInteger;

import org.riodb.sql.ExceptionSQLStatement;

import org.riodb.plugin.RioDBPluginException;

/*
 *    StreamManager is nothing more than a container of Streams,
 *    to enable RioDB to handle more than 1 stream.   
 */

public class Engine {

	// default initial capacity for LinkedBlockingQueues
	// private static final int queueInitSize = 65536;

	// online/offline flag
	private boolean online;
	
	// Streams
	private Stream streams[];

	Engine() {
		streams = new Stream[0];
		online = false;
	}
	
	// get online flag
	public boolean isOnline() {
		return online;
	}
	
	// Internal system clock
	private final Clock clock = new Clock();
	public final Clock getClock() {
		return clock;
	}
	
	// counter to ensure dynamic objects are always created with unique name;
	private final AtomicInteger globalCounter = new AtomicInteger(0);
	public int counterNext() {
		return globalCounter.incrementAndGet();
	}

	public boolean addStream(Stream newStream) throws ExceptionSQLStatement, RioDBPluginException {

		Stream newStreams[] = new Stream[streams.length + 1];
		for (int i = 0; i < streams.length; i++) {
			if (newStream.getName().equals(streams[i].getName()))
				// duplicate name
				return false;
			newStreams[i] = streams[i];
		}
		newStreams[streams.length] = newStream;
		streams = newStreams;
		return true;
	}

	// get all streams in a string
	public String describe(String streamName) {

		if (streamName.contains(".") && streamName.indexOf('.') < (streamName.length() - 2)) {
			String stream = streamName.substring(0, streamName.indexOf("."));
			String window = streamName.substring((streamName.indexOf(".") + 1));

			for (int i = 0; i < streams.length; i++) {
				if(streams[i] != null) {
				if (streams[i].getName().equals(stream)) {
					return streams[i].describeWindow(window);
				}
				}
			}

		} else {
			for (int i = 0; i < streams.length; i++) {
				if(streams[i] != null) {
				if (streams[i].getName().equals(streamName)) {
					return streams[i].describe();
				}
				}
			}
		}
		return "";
	}

	// get Stream by ID
	public Stream getStream(int index) {
		return streams[index];
	}

	// get Stream by Name
	public int getStream(String name) {
		if (name == null)
			return -1;

		for (int i = 0; i < streams.length; i++) {
			if(streams[i] != null) {
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
			if(streams[i] != null) {
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
				response = response + streams[i].getWindowMgr().listAllWindows() ;
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

	// print counters for debugging
	public void printMsgCounters() {
		String s = "";
		for (int i = 0; i < streams.length; i++) {
			s = "\t" + "stream[" + i + "]: " + streams[i].getMsgCounter();
		}
		System.out.println(s);
	}

	public boolean removeStream(int streamId) {

		if (streamId >= 0 && streamId < streams.length) {
			streams[streamId].stop();
			// TODO: ensure graceful removal of query threads... 
			streams[streamId] = null;
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
	public void start(int streamId) {
		clock.start();
		Clock.quickPause();
		if (streamId < streams.length) {
			streams[streamId].start();
		}
	}

	public String status() {

		String response = "[ ";
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if (i > 0) {
					response = response + " , ";
				}
				response = response + "\n " + streams[i].status();
			}
		}
		response = response + "\n]";
		return response;
	}

	// start all stream processes
	public void stop() {
		online = false;/*
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


		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				streams[i].stop();
			}
		}
	}

	// start ONE stream process
	public void stop(int streamId) {
		if (streamId < streams.length) {
			streams[streamId].stop();
		}
		Clock.quickPause();
		clock.start();
		Clock.quickPause();
	}

	public void trimExpiredWindowElements(int currentSecond) {
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				streams[i].trimExpiredWindowElements(currentSecond);
			}
		}
	}
	
	public boolean dropQuery(int queryId) {
		for (int i = 0; i < streams.length; i++) {
			if (streams[i] != null) {
				if(streams[i].dropQuery(queryId)){
					return true;
				}
			}
		}
		return false;
	}
}
