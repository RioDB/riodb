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

import java.util.HashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class QuerySessions {

	private final HashMap<Integer, SynchronousQueue<String>> waitingStatements = new HashMap<Integer, SynchronousQueue<String>>();

	public QuerySessions() {
	}

	public String request(Integer sessionId, int timeout) throws InterruptedException {
		waitingStatements.put(sessionId, new SynchronousQueue<String>());
		String reply = waitingStatements.get(sessionId).poll(timeout,TimeUnit.SECONDS);
		waitingStatements.remove(sessionId);
		return reply;
	}

	public void respond(Integer sessionId, String reply) {
		if (waitingStatements.containsKey(sessionId)) {
			try {
				waitingStatements.get(sessionId).offer(reply, 1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				//
			}
		}
	}
	
	public void kill(Integer sessionId) {
		respond(sessionId, "Query timed out.");
	}

}
