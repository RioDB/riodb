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

package org.riodb.queries;

import java.util.ArrayList;
import java.util.Iterator;
import org.jctools.queues.SpscChunkedArrayQueue;
import org.riodb.engine.RioDB;
import org.riodb.sql.ExceptionSQLExecution;

public class QueryManager implements Runnable {
	
	public static final int QUEUE_INIT_CAPACITY = 244; // 10000;
	public static final int MAX_CAPACITY = 1000000;

	private int streamId;
	private final ArrayList<Query> queries = new ArrayList<Query>();
	private final SpscChunkedArrayQueue<EventWithSummaries> inbox = 
			new SpscChunkedArrayQueue<EventWithSummaries>(QUEUE_INIT_CAPACITY, MAX_CAPACITY);;

	private Thread queryMgrThread;
	private boolean interrupt;
	private boolean erroAlreadyCaught;
	
	
	// Sessions of API SELECT statements that are waiting for a reply. 
	private final QuerySessions sessions = new QuerySessions();
	
	public String request(Integer sessionId, int timeout) throws InterruptedException {
		return sessions.request(sessionId, timeout);
	}
	
	public void respond(Integer sessionId, String reply) {
		sessions.respond(sessionId, reply);
	}	

	public QueryManager(int streamId) {
		this.streamId = streamId;
		erroAlreadyCaught = false;
	}

	public synchronized int addQueryRef(Query query) {
		int slot = queries.size();
		queries.add(query);
		return slot;
	}

	public synchronized void removeQuery(int queryId) {
		queries.remove(queryId);
	}
	
	public int queryCount() {
		return queries.size();
	}

	// get all window names
	public String listAllQueries() {
		String response = "";
		for (int i = 0; i < queries.size(); i++) {
			response = response + "{\"id\":" + queries.get(i).getQueryId() + ", \"stream\":\""+ RioDB.rio.getEngine().getStream(streamId).getName() +"\", \"statement\": \"" +  queries.get(i).getQueryStr().replace("\"","\\\"") + "\"},";
		}
		if (response.length() > 2)
			response = response.substring(0, response.length() - 1);
		return response;
	}
	
	// drop a query
	public boolean dropQuery(int queryId) {
		for (int i = 0; i < queries.size(); i++) {
			if(queries.get(i).getQueryId() == queryId) {
				queries.remove(i);
				return true;
			}
		}
		return false;
	}

	// get all window names
	public String describeQuery(int queryId) {
		if(queryId < queries.size()-1)
			return queries.get(queryId).getQueryStr();  
		return null;
	}
	
	public void putEventRef(EventWithSummaries s) {
		inbox.offer(s);
	}
	
	public int inboxSize() {
		return inbox.size();
	}

	public void start() {
		interrupt = false;
		queryMgrThread = new Thread(this);
		queryMgrThread.setName("QUERY_MANAGER_THREAD");
		queryMgrThread.start();
	}

	public void stop() {
		interrupt = true;
		queryMgrThread.interrupt();
	}
	
	public String status() {
		if(interrupt)
			return "stopped";
		return "running";
	}

	public void run() {
		RioDB.rio.getSystemSettings().getLogger().info("Starting query manager for Stream[" + streamId + "] ...");
		while (!interrupt) {
			EventWithSummaries esum = inbox.poll();
			if (esum != null) {
				
				Iterator<Query> qItr = queries.iterator();
				while (qItr.hasNext()) {
					Query q = qItr.next();

					try {
						// call Query evaluation and get query status.
						// the query returns TRUE if it reached end-of-life. 
						if (q.evalAndGetStatus(esum)) {
							qItr.remove();
							RioDB.rio.getSystemSettings().getLogger().debug("Query removed... ");
						}
					} catch (ExceptionSQLExecution e) {
						if (!erroAlreadyCaught) {
							RioDB.rio.getSystemSettings().getLogger().debug("Error executing query.");
							RioDB.rio.getSystemSettings().getLogger().debug(e.getMessage());
							erroAlreadyCaught = true;
						}
					}

				}

			} else {
				// Save CPU cycles.
				// This implementation is more efficient than any blockingQueue,
				// as we're dealing with single producer / single consumer.
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					;
				}
			}
		}
		RioDB.rio.getSystemSettings().getLogger().info("Stopping query manager for Stream[" + streamId + "] ...");
	}
}
