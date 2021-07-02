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

	A Stream object to process stream events end-to-end
	
	It takes data from a dataSource and passes it through all dependent windows and queries	
	
	It declares:
		streamId: 		An identifier
		streamName: 	The name of the stream (used by queries and api requests)
		streamEventDef:		A definition of the event fields
		streamDataSource:	A data source that will be providing new events,
		streamWindowMgr:	A container of windows for this stream
		streamQueryMgr:		A container of queries for this stream
		streamThread: 		A Runnable thread for processing the stream end-to-end
		
*/

package org.riodb.engine;

import org.riodb.classloaders.DataSourceClassLoader;
import org.riodb.queries.EventWithSummaries;
import org.riodb.queries.Query;
import org.riodb.queries.QueryManager;
import org.riodb.sql.ExceptionSQLStatement;
import org.riodb.windows.WindowManager;
import org.riodb.windows.WindowSummary;
import org.riodb.windows.WindowWrapper;

import org.riodb.plugin.RioDBDataSource;
import org.riodb.plugin.RioDBPluginException;
import org.riodb.plugin.RioDBStreamEvent;
import org.riodb.plugin.RioDBStreamEventDef;

public class Stream implements Runnable {

	// Stream unique id
	private int streamId;

	// String name for the Stream (for query syntax)
	private String streamName;

	// Definition of the fields that the stream provides.
	private RioDBStreamEventDef streamEventDef;

	// Runnable data source to receive data and put into the Inbox queue;
	private RioDBDataSource streamDataSource;

	// Container of user-defined windows. is FINAL for performance
	private final WindowManager streamWindowMgr = new WindowManager();

	// Container of user-defined queries. is FINAL for performance
	private final QueryManager streamQueryMgr = new QueryManager();

	// Thread that this handler runs on.
	private Thread streamThread;
	// boolean to interrupt thread loop.
	private boolean interrupt;

	// for debugging...
	// private int counter;

	// Constructor
	public Stream(int streamId, String name, RioDBStreamEventDef def, String dataSourceType, String dataSourceParams)
			throws ExceptionSQLStatement, RioDBPluginException {
		RioDB.rio.getSystemSettings().getLogger().info("Creating stream[" + streamId + "]" + name);
		this.streamId = streamId;
		this.streamName = name;
		this.streamEventDef = def;

		// instead of passing StreamID into constructor, it's set afterwards due to
		// being final (can't construct again)
		streamWindowMgr.setStreamId(streamId);

		// instead of passing StreamID into constructor, it's set afterwards due to
		// being final (can't construct again)
		streamQueryMgr.setStreamId(streamId);

		// load the data source class (aka "input plugin")
		if (dataSourceType != null) {
			streamDataSource = DataSourceClassLoader.getInputPlugin(dataSourceType);
			streamDataSource.init(dataSourceParams, def);
		} else {
			RioDB.rio.getSystemSettings().getLogger()
					.error("Failed to create stream because dataSource Type is missing");
		}

	}

	// streamId getter
	public int getStreamId() {
		return streamId;
	}

	// streamName getter
	public String getName() {
		return streamName;
	}

	// describe stream in JSON format
	public String describe() {
		String s = "{ \"name\":\"" + streamName + "\",\n \"fields\":[" + streamEventDef.getFieldList()
				+ "],\n \"type\":\"" + streamDataSource.getType() + "\"," + "\n \"timestamp\" : \""
				+ (streamEventDef.getTimestampNumericFieldId() == -1 ? "clock" : streamEventDef.getTimestampFieldName())
				+ "\" }";
		return s;
	}

	// get status string in JSON format
	public String status() {

		String threadStatus = "running";
		if (interrupt)
			threadStatus = "stopped";

		String s = "{ \n   \"stream_name\":\"" + streamName + "\"," + "\n   \"_thread\": \"" + streamDataSource.status()
				+ "\"," + "\n   \"window_count\": " + streamWindowMgr.getWindowCount() + ","
				+ "\n   \"handler_thread\": \"" + threadStatus + "\","
				// + "\n \"event_queue_size\": " + streamPacketInbox.size() + ","
				+ "\n   \"query_thread\": \"" + streamQueryMgr.status() + "\"," + "\n   \"query_count\": "
				+ streamQueryMgr.queryCount() + "," + "\n   \"query_queue_size\": " + streamQueryMgr.inboxSize()
				+ "\n }";
		return s;
	}

	// describe stream windows in JSON format
	public String describeWindow(String windowName) {
		return streamWindowMgr.describeWindow(windowName);
	}

	// list queries in JSON format
	public String listAllQueries() {
		return streamQueryMgr.listAllQueries();
	}

	// drop a query from this stream if it exists. False if not found.
	public boolean dropQuery(int queryId) {
		return streamQueryMgr.dropQuery(queryId);
	}

	// Start this stream and all its dependencies
	public void start() {

		// start query
		streamQueryMgr.start();
		Clock.quickPause();

		// start stream thread
		// counter = 0;
		interrupt = false;
		streamThread = new Thread(this);
		streamThread.setName("STREAM_THREAD_" + streamId);
		streamThread.start();

		Clock.quickPause();

		// start data source
		try {
			streamDataSource.start();
		} catch (RioDBPluginException e) {
			RioDB.rio.getSystemSettings().getLogger()
					.info("Error starting input plugin: " + e.getMessage().replace("\n", "").replace("\r", ""));
		}

	}

	// Stop this steram and all its dependencies
	public void stop() {

		// stop data source first
		try {
			RioDB.rio.getSystemSettings().getLogger().debug("Stopping DataSource for stream " + streamId);
			streamDataSource.stop();
		} catch (RioDBPluginException e) {
			RioDB.rio.getSystemSettings().getLogger()
					.info("Error stopping input plugin: " + e.getMessage().replace("\n", "").replace("\r", ""));
		}
		Clock.quickPause();
		// stop stream
		interrupt = true;
		streamThread.interrupt();
		RioDB.rio.getSystemSettings().getLogger().debug("Stopping DataSource thread for stream " + streamId);
		// counter = 0;
		Clock.quickPause();
		// stop queries
		streamQueryMgr.stop();
		RioDB.rio.getSystemSettings().getLogger().debug("Stopping Query Mgr for stream " + streamId);
		Clock.quickPause();
	}

	// Getter for stream event field definition
	public RioDBStreamEventDef getDef() {
		return streamEventDef;
	}

	// Send an event with window results to the queries
	public void sendEventResultsRefToQueries(EventWithSummaries ews) {
		streamQueryMgr.putEventRef(ews);
	}

	// get size of awaiting queue in data source
	protected int inboxSize() {
		return streamDataSource.getQueueSize();
	}

	// Add window to this stream
	public void addWindowRef(WindowWrapper newWindow) {
		streamWindowMgr.addWindow(newWindow);
	}

	// remove window from this stream
	public boolean removeWindow(int index) {
		return streamWindowMgr.removeWindow(index);
	}

	// getter for the window manager
	public WindowManager getWindowMgr() {
		return streamWindowMgr;
	}

	// adds a query to the Stream
	public int addQueryRef(Query query) {
		return streamQueryMgr.addQueryRef(query);
	}

	/*
	 * Some queries run indefinitely. Some queries are one-time poking requests made
	 * from API. The following 2 methods are for submitting a query request and
	 * sending a response to the requester.
	 * 
	 */

	// Submit a query request, with a session ID and a timeout limit.
	public String requestQueryResponse(Integer sessionId, int timeout) throws InterruptedException {
		return streamQueryMgr.request(sessionId, timeout);
	}

	// send a response to a query request.
	public void sendQueryResponse(Integer sessionId, String reply) {
		streamQueryMgr.respond(sessionId, reply);
	}

	// Windows of time need to evict expired elements with each passing second.
	// the systemSettings clock calls this method ever second to evict expired
	// elements.
	public void trimExpiredWindowElements(int currentSecond) {
		streamWindowMgr.trimExpiredWindowElements(currentSecond);
	}

	// The Runnable run() method for executing the thead of this stream.
	@Override
	public void run() {
		RioDB.rio.getSystemSettings().getLogger().info("Starting Event Handler for Stream[" + streamId + "] ...");
		try {

			while (!interrupt) {
				// try {
				// Thread.sleep(100);
				// } catch (InterruptedException e1) {
				// // TODO Auto-generated catch block
				// e1.printStackTrace();
				// }

				// get next event from dataSource. non-blocking. Null can be returned.
				RioDBStreamEvent event = streamDataSource.getNextEvent();
				if (event != null) {

					/*
					 * Tell windowManager to run this event on ALL windows. Collect all windows
					 * responses (windowSummary) into array. This array is filled with the clone of
					 * each window summary, therefore, the windows can continue to change as they
					 * receive future events. The queries that will be processing the clones will
					 * not clash with future events updating the windows since the queries will be
					 * operating with a frozen clone.
					 * 
					 */
					final WindowSummary results[] = streamWindowMgr.putEventRef(event);

					// make new object that wraps the Event & window summaries together.
					final EventWithSummaries ews = new EventWithSummaries(event, results);

					// Send event + window summaries to Queries for processing.
					sendEventResultsRefToQueries(ews);

				} else {
					/*
					 * save CPU... There's no guarantee that the input plugin's getNextEvent will
					 * "wait" for data. Plugins are not required to provide countdownlatching. This
					 * was chosen to allow very full throttle streams with minimal overhead. So we
					 * have to pause the loop. For now, we are doing sleep(1). It responds well to
					 * busy streams and keeps CPU near 0% when there's no traffic.
					 */
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						;
					}
				}
			}
			RioDB.rio.getSystemSettings().getLogger().info("EventHandler for stream [" + streamId + "] stopped.");

		} catch (RioDBPluginException e) {
			RioDB.rio.getSystemSettings().getLogger().debug("plugin returned error: " + e.getMessage());
			// e.printStackTrace();
		}

		// if loop was broken by interrupt, log intentional stopping. 
		if (interrupt) {
			RioDB.rio.getSystemSettings().getLogger().info("EventHandler for stream [" + streamId + "] stopped.");
		} else {
			// log unintentional stopping. 
			RioDB.rio.getSystemSettings().getLogger()
					.info("EventHandler for stream [" + streamId + "] stopped unexpectedly.");
		}
	}

	// reset a window
	public void resetWindow(String windowName) {
		streamWindowMgr.resetWindow(windowName);
	}

	// reset all windows
	public void resetAllWindows() {
		streamWindowMgr.resetAllWindows();
	}
}
