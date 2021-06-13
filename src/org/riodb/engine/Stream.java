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

	// Container of user-defined windows
	private WindowManager streamWindowMgr;

	// Container of user-defined queries
	private QueryManager streamQueryMgr;

	// Thread that this handler runs on.
	private Thread streamThread;
	private boolean interrupt;

	// metrics
	private int counter;

	public Stream(int streamId, String name, RioDBStreamEventDef def, String dataSourceType, String dataSourceParams)
			throws ExceptionSQLStatement, RioDBPluginException {
		RioDB.rio.getSystemSettings().getLogger().info("Creating stream[" + streamId + "]" + name);
		this.streamId = streamId;
		this.streamName = name;
		this.streamEventDef = def;

		// streamPacketInbox = new
		// SpscChunkedArrayQueue<DatagramPacket>(QUEUE_INIT_CAPACITY, MAX_CAPACITY);

		streamWindowMgr = new WindowManager(streamId);

		streamQueryMgr = new QueryManager(streamId);

		if (dataSourceType != null) {
			streamDataSource = DataSourceClassLoader.getInputPlugin(dataSourceType);
			streamDataSource.init(dataSourceParams, def);
		} else {
			RioDB.rio.getSystemSettings().getLogger()
					.error("Failed to create stream because dataSource Type is missing");
		}

	}

	public int getStreamId() {
		return streamId;
	}

	public String getName() {
		return streamName;
	}

	public String describe() {
		String s = "{ \"name\":\"" + streamName + "\",\n \"fields\":[" + streamEventDef.getFieldList()
				+ "],\n \"type\":\"" + streamDataSource.getType() + "\"," + "\n \"timestamp\" : \""
				+ (streamEventDef.getTimestampNumericFieldId() == -1 ? "clock" : streamEventDef.getTimestampFieldName())
				+ "\" }";
		return s;
	}

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

	public String describeWindow(String windowName) {
		return streamWindowMgr.describeWindow(windowName);
	}

	public String listAllQueries() {
		return streamQueryMgr.listAllQueries();
	}

	public boolean dropQuery(int queryId) {
		return streamQueryMgr.dropQuery(queryId);
	}

	public void start() {

		// start query
		streamQueryMgr.start();
		Clock.quickPause();

		// start stream thread
		counter = 0;
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
		counter = 0;
		Clock.quickPause();
		// stop queries
		streamQueryMgr.stop();
		RioDB.rio.getSystemSettings().getLogger().debug("Stopping Query Mgr for stream " + streamId);
		Clock.quickPause();
	}

	public RioDBStreamEventDef getDef() {
		return streamEventDef;
	}

	public void sendEventResultsRefToQueries(EventWithSummaries ews) {
		streamQueryMgr.putEventRef(ews);
	}

	protected int inboxSize() {
		return streamDataSource.getQueueSize();
	}

	public int getMsgCounter() {
		return counter;
	}

	public void addWindowRef(WindowWrapper newWindow) {
		streamWindowMgr.addWindow(newWindow);
	}

	public boolean removeWindow(int index) {
		return streamWindowMgr.removeWindow(index);
	}

	public WindowManager getWindowMgr() {
		return streamWindowMgr;
	}

	public int addQueryRef(Query query) {
		return streamQueryMgr.addQueryRef(query);
	}

	public String requestQueryResponse(Integer sessionId, int timeout) throws InterruptedException {
		return streamQueryMgr.request(sessionId, timeout);
	}

	public void sendQueryResponse(Integer sessionId, String reply) {
		streamQueryMgr.respond(sessionId, reply);
	}

	public void trimExpiredWindowElements(int currentSecond) {
		streamWindowMgr.trimExpiredWindowElements(currentSecond);
	}

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
				RioDBStreamEvent event = streamDataSource.getNextEvent();
				if (event != null) {

					WindowSummary results[] = streamWindowMgr.putEventRef(event);

					EventWithSummaries ews = new EventWithSummaries(event, results);

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

		if (interrupt) {
			RioDB.rio.getSystemSettings().getLogger().info("EventHandler for stream [" + streamId + "] stopped.");
		} else {
			RioDB.rio.getSystemSettings().getLogger()
					.info("EventHandler for stream [" + streamId + "] stopped unexpectedly.");
		}
	}

	public void resetWindow(String windowName) {
		streamWindowMgr.resetWindow(windowName);
	}

	public void resetAllWindows() {
		streamWindowMgr.resetAllWindows();
	}
}
