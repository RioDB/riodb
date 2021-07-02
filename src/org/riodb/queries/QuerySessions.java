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

	Query Sessions is an object for keeping track of Query requests
	that come in via the HTTP Request API
	
	This queries are service and results are returned to the API request.

*/

package org.riodb.queries;

import java.util.HashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class QuerySessions {

	// a Map to track a session / requests.
	// The KEY is the session id integer.
	// The value is a synchronousQueue for sending a single object back and forth
	// between threads.
	private final HashMap<Integer, SynchronousQueue<String>> waitingStatements = new HashMap<Integer, SynchronousQueue<String>>();

	// constructor
	public QuerySessions() {
	}

	// Submit a request
	public String request(Integer sessionId, int timeout) throws InterruptedException {
		// create an k/v entry with the sessionId and an empty SynchronousQueue<string>
		// The synchronousQueue value is empty at this point and will be set by another
		// thread.
		waitingStatements.put(sessionId, new SynchronousQueue<String>());
		// now the query is running and when there is a match it will update the
		// synchronousQueue
		// The poll request below is blocking and will wait for the the update to
		// happen.
		String reply = waitingStatements.get(sessionId).poll(timeout, TimeUnit.SECONDS);
		// now that we have a response (or the poll timed out) remove the query.
		waitingStatements.remove(sessionId);
		// return reply to requester
		return reply;
	}

	// This method is called by the query to offer a reply into the empty
	// synchronousQueue
	// This happens while the method above is awaiting with a blocking poll()
	public void respond(Integer sessionId, String reply) {
		if (waitingStatements.containsKey(sessionId)) {
			try {
				waitingStatements.get(sessionId).offer(reply, 1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				//
			}
		}
	}

	// kill a session id.
	// It doesn't directly remove the session.
	// Instead, it sends a response that the query timed out.
	// The processing of hte response in request() method will remove the session.
	public void kill(Integer sessionId) {
		respond(sessionId, "Query timed out.");
	}

}
