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

import org.riodb.engine.RioDB;
import org.riodb.sql.ExceptionSQLExecution;
import org.riodb.sql.SQLQueryColumn;
import org.riodb.sql.SQLQueryCondition;

import org.riodb.plugin.RioDBOutput;

public class Query {

	private SQLQueryCondition sqlQueryCondition;
	private SQLQueryColumn columns[];
	private RioDBOutput output;

	private boolean limitByTime;
	private int limit;

	private boolean usesTimeout;
	private boolean timeoutByTime;
	private int timeout;
	private boolean currentlyInTimeout;
	private int currentTimeoutTil;
	
	private int queryId;

	private String queryStr;

	public Query(SQLQueryCondition condition, RioDBOutput output, SQLQueryColumn columns[], int limit, boolean limitByTime,
			int timeout, boolean timeoutByTime, String queryStr) {
		this.sqlQueryCondition = condition;
		this.output = output;
		this.columns = columns;
		this.queryStr = queryStr;
		this.limit = limit;
		this.limitByTime = limitByTime;
		this.timeout = timeout;
		this.timeoutByTime = timeoutByTime;
		this.queryId = RioDB.rio.getEngine().counterNext();
		
		currentlyInTimeout = false;
		currentTimeoutTil = 0;

		if (timeout >= 0) {
			usesTimeout = true;
		} else {
			usesTimeout = false;
		}
	}

	public boolean evalAndGetStatus(EventWithSummaries esum) throws ExceptionSQLExecution {

		// returns true if this query can be destroyed. reached its limit.

		if (limitByTime && limit < RioDB.rio.getEngine().getClock().getCurrentSecond()) {
			RioDB.rio.getSystemSettings().getLogger().debug("query reached age");
			return true; // this Query is overdue and can be destroyed.
		}

		if (currentlyInTimeout) {
			if (timeoutByTime) {
				if (currentTimeoutTil <= RioDB.rio.getEngine().getClock().getCurrentSecond()) {
					currentlyInTimeout = false;
				} else {
					return false; // cut it short. But don't destroy query.
				}
			} else {
				currentTimeoutTil--;
				if (currentTimeoutTil == 0) {
					currentlyInTimeout = false;
				} else {
					return false; // cut it short. but don't destroy query.
				}
			}
		}

		if (!currentlyInTimeout) {
			
			if (sqlQueryCondition != null && !sqlQueryCondition.match(esum.getEventRef(), esum.getWindowSummariesRef())) {
				return false;
			}
			String columnValues[] = new String[columns.length];
			for (int i = 0; i < columns.length; i++) {
				columnValues[i] = columns[i].getValue(esum.getEventRef(), esum.getWindowSummariesRef());
			}
			output.send(columnValues);

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
		return false;
	}

	public int getLimit() {
		return limit;
	}

	public boolean isLimitedByTime() {
		return limitByTime;
	}

	public int getTimeout() {
		return timeout;
	}

	public boolean isTimedoutByTime() {
		return timeoutByTime;
	}

	public boolean isCurrentlyInTimeout() {
		return currentlyInTimeout;
	}

	public int getQueryId() {
		return queryId;
	}
	
	public String getQueryStr() {
		return queryStr;
	}
	
}
