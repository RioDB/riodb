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


	A container for Queries

 */



package org.riodb.queries;

import java.util.ArrayList;
import java.util.Iterator;
import org.jctools.queues.SpscChunkedArrayQueue;
import org.riodb.engine.RioDB;
import org.riodb.sql.ExceptionSQLExecution;

public class QueryManager implements Runnable {

	// initial capacity of chunked array queue, and max capacity
	// if unfamiliar, leave at 244 and 1000000
	// If you have 1,000,000 queries pending, it's time to review your architecture
	public static final int QUEUE_INIT_CAPACITY = 244;
	public static final int MAX_CAPACITY = 1000000;

	// streamId that this query is called from
	private int streamId;
	
	// ArrayList of queries
	private final ArrayList<Query> queries = new ArrayList<Query>();
	// queue of eventWithSummaries after processed by windows. 
	private final SpscChunkedArrayQueue<EventWithSummaries> queryInbox = 
			new SpscChunkedArrayQueue<EventWithSummaries>(QUEUE_INIT_CAPACITY, MAX_CAPACITY);;

	// Thread for running queries
	private Thread queryMgrThread;
	private boolean interrupt;
	private boolean erroAlreadyCaught;
	
	
	// Sessions of API SELECT statements that are waiting for a reply. 
	private final QuerySessions sessions = new QuerySessions();
	
	// constructor
	public QueryManager() {
		//this.streamId = streamId;
		erroAlreadyCaught = false;
	}
	
	// sets the streamId of this query mgr
	// It's not done in constructor because QueryManager is created as final
	public void setStreamId(int streamId) {
		this.streamId = streamId;
	}

	// For handling queries that came in via HTTP Request api
	public String request(Integer sessionId, int timeout) throws InterruptedException {
		return sessions.request(sessionId, timeout);
	}

	// For responding to a query that came in via HTTP Request API
	public void respond(Integer sessionId, String reply) {
		sessions.respond(sessionId, reply);
	}	

	// adds a query (sync in case of concurrent requests)
	public synchronized int addQueryRef(Query query) {
		int slot = queries.size();
		queries.add(query);
		return slot;
	}

	// get query count (of this stream only)
	public int queryCount() {
		return queries.size();
	}

	// get all queries in JSON format
	public String listAllQueries() {
		String response = "";
		for (int i = 0; i < queries.size(); i++) {
			response = response + "{\"id\":" + queries.get(i).getQueryId() + ", \"stream\":\""+ RioDB.rio.getEngine().getStream(streamId).getName() +"\", \"statement\": \"" +  queries.get(i).getQueryStr().replace("\"","\\\"") + "\"},";
		}
		if (response.length() > 2)
			response = response.substring(0, response.length() - 1);
		return response;
	}
	
	// drop a query (sync in case of concurrent requests)
	public synchronized boolean dropQuery(int queryId) {
		for (int i = 0; i < queries.size(); i++) {
			if(queries.get(i).getQueryId() == queryId) {
				queries.remove(i);
				return true;
			}
		}
		return false;
	}

	// Describe a query
	public String describeQuery(int queryId) {
		if(queryId < queries.size()-1)
			return queries.get(queryId).getQueryStr();  
		return null;
	}
	
	// This is for Stream to send eventWithSummaries to the Query mgr. 
	public void putEventRef(EventWithSummaries s) {
		queryInbox.offer(s);
	}
	
	// Get number of awaiting events to be processed by queries
	public int inboxSize() {
		return queryInbox.size();
	}

	// start Runnable thread - queryManager run its own thread.
	public void start() {
		interrupt = false;
		queryMgrThread = new Thread(this);
		queryMgrThread.setName("QUERY_MANAGER_THREAD");
		queryMgrThread.start();
	}

	// stop queryManager thread
	public void stop() {
		interrupt = true;
		queryMgrThread.interrupt();
	}
	
	// get status of QueryManager thread
	public String status() {
		if(interrupt)
			return "stopped";
		return "running";
	}

	// run method
	public void run() {
		RioDB.rio.getSystemSettings().getLogger().info("Starting query manager for Stream[" + streamId + "] ...");
		
		// for future enhancement, queries should be able to reference data from previous event. 
		//RioDBStreamEvent previousEvent = null;
		
		// loop until interrupted:
		while (!interrupt) {
			// poll next eventWithSummaries from query inbox for processing:
			EventWithSummaries esum = queryInbox.poll();
			if (esum != null) {
				
				// Iterator to loop through queries
				Iterator<Query> qItr = queries.iterator();
				while (qItr.hasNext()) {
					Query q = qItr.next();

					try {
						/*
						 for each query:
						 call Query evaluation and get query status.
						 the query returns TRUE if it reached end-of-life.
						 When there's a matched record, the query itself handles calling posting the output. 
						 This process does not need to collect selected values for output. 
						 
						 If the query hit end-of-life, remove it.
						  
						 */
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

				// for future enhancement, queries should be able to reference data from previous event. 
				// previousEvent = esum.getEventRef();

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
