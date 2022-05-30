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

	A query object that will process MessageWithSummaries and,
	if there's a match, invoke the RioDBPlugin.
  
 */

package org.riodb.queries;

import org.riodb.engine.RioDB;
import org.riodb.sql.ExceptionSQLExecution;
import org.riodb.sql.SQLQueryColumn;
import org.riodb.sql.SQLQueryCondition;
import org.riodb.sql.SQLQueryResources;
import org.riodb.plugin.RioDBPlugin;
import org.riodb.plugin.RioDBPluginException;

public class Query {

	// The condition of the query
	private SQLQueryCondition sqlQueryCondition;
	// The columns to be processed when a condition matches.
	private SQLQueryColumn columns[];
	// The output that will handle selected messages (that match condition)
	private RioDBPlugin output;

	// Does this query expired by time?
	private boolean limitByTime;
	// limit til expired
	private int limit;

	// Does this query take a timeout after each match?
	private boolean usesTimeout;
	// Is the timeout duration measured by time?
	private boolean timeoutByTime;
	// the timeout
	private int timeout;
	// is the query currently in timeout?
	private boolean currentlyInTimeout;
	// when the current timeout ends
	private int currentTimeoutTil;
	// list of resources used by this Query:
	private SQLQueryResources queryResources;

	// THIS query id
	private int queryId;

	// The original query statement?
	private String queryStr;

	// Query status / false = destroying..
	private boolean destroy = false;

	// constructor
	public Query(SQLQueryCondition condition, RioDBPlugin output, SQLQueryColumn columns[], int limit,
			boolean limitByTime, int timeout, boolean timeoutByTime, String queryStr,
			SQLQueryResources queryResources) {
		this.sqlQueryCondition = condition;
		this.output = output;
		this.columns = columns;
		this.queryStr = queryStr;
		this.limit = limit;
		this.limitByTime = limitByTime;
		this.timeout = timeout;
		this.timeoutByTime = timeoutByTime;
		this.queryId = RioDB.rio.getEngine().counterNext();
		this.queryResources = queryResources;

		currentlyInTimeout = false;
		currentTimeoutTil = 0;

		if (timeout >= 0) {
			usesTimeout = true;
		} else {
			usesTimeout = false;
		}
	}

	// run a query, and get status to see if it can be dropped
	// if 'true' is returned, the query will be removed.
	public boolean evalAndGetStatus(MessageWithSummaries esum) throws ExceptionSQLExecution {
		
		
		
		// print function for troubleshooting...
		// System.out.println("Evaluating query "+ queryId);
		// esum.printMessage();
		

		// PART 1, take care of expiring queries and queries in timeout

		// If the query is limited by time and the time is up, drop the query
		if (destroy || (limitByTime && limit < RioDB.rio.getEngine().getClock().getCurrentSecond())) {
			if(!destroy)
				RioDB.rio.getSystemSettings().getLogger().debug("Query "+ queryId +" reached age.");
			return true; // this Query is overdue and can be destroyed.
		}

		// If the query is currently in a timeout
		if (currentlyInTimeout) {
			// if it's timeout by time, check if the time is up
			if (timeoutByTime) {
				if (currentTimeoutTil <= RioDB.rio.getEngine().getClock().getCurrentSecond()) {
					currentlyInTimeout = false; // timeout ends
				} else {
					return false; // cut it short. But don't destroy query.
				}
			}
			// else, if timeout is message count, check if count is up.
			else {
				currentTimeoutTil--;
				if (currentTimeoutTil == 0) {
					currentlyInTimeout = false; // timeout ends
				} else {
					return false; // cut it short. but don't destroy query.
				}
			}
		}

		// PART 2: run the query

		// if the query is in timeout, we skip it. Otherwise, run it:
		if (!currentlyInTimeout && !destroy) {

			// if the condition is not null (some queries don't have a condition), check for
			// condition match:
			if (sqlQueryCondition != null
					&& !sqlQueryCondition.match(esum.getMessageRef(), esum.getWindowSummariesRef(), esum.getWindowSummariesRef_String())) {
				// the query is NOT a match the conditions
				return false;
			}
			// Continuing... Either the query condition has been matched, or it has no conditions to test...

			// Make array of SELECTed column values:
			String columnValues[] = new String[columns.length];
			for (int i = 0; i < columns.length; i++) {
				columnValues[i] = columns[i].getValue(esum.getMessageRef(), esum.getWindowSummariesRef(),esum.getWindowSummariesRef_String());
				if(columnValues[i] != null && columnValues[i].equals("NaN")) {
					columnValues[i] = "null";
				}
			}

			// send output to output plugin.
			output.sendOutput(columnValues);
			

			// set timeout if necessary for this query
			if (!limitByTime) {
				limit--;
				if (limit == 0) { // this query just hit it's limit end.
					return true; // destroy the query.
				}
			}
			if (usesTimeout) {
				currentlyInTimeout = true;
				if (timeoutByTime) {
					currentTimeoutTil = timeout + RioDB.rio.getEngine().getClock().getCurrentSecond();
				} else {
					currentTimeoutTil = timeout;
				}
			}
		}
		return destroy;
	}

	// get query limit
	public int getLimit() {
		return limit;
	}

	// get isLimitedByTime
	public boolean isLimitedByTime() {
		return limitByTime;
	}

	// get timeout
	public int getTimeout() {
		return timeout;
	}

	// get timeoutByTime
	public boolean isTimedoutByTime() {
		return timeoutByTime;
	}

	// get currentlyInTimeout
	public boolean isCurrentlyInTimeout() {
		return currentlyInTimeout;
	}

	// get queryId
	public int getQueryId() {
		return queryId;
	}

	// get queryStr
	public String getQueryStr() {
		return queryStr;
	}

	// checks if query depends on a stream:
	public boolean dependsOnStream(int streamId) {
		return queryResources.dependsOnStream(streamId);
	}

	// checks if query depends on a window
	// checks if query dependes on a stream:
	public boolean dependsOnWindow(int streamId, int windowId) {
		return queryResources.dependsOnWindow(streamId, windowId);
	}

	// mark query for removal
	public void removeQuery() {
		destroy = true;
	}

	// mark query for removal
	public boolean isDestroying() {
		return destroy;
	}
	
	// start plugin (in case it has a startup procedure)
	public void start() throws RioDBPluginException {
		RioDB.rio.getSystemSettings().getLogger().debug("Query.start(): starting query " + queryId);
		output.start();
	}
	
	// stop plugin (in case it has stop steps)
	public void stop() throws RioDBPluginException {
		RioDB.rio.getSystemSettings().getLogger().debug("Query.stop(): stopping query" + queryId);
		output.stop();
	}
}
