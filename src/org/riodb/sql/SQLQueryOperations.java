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

package org.riodb.sql;

import org.riodb.engine.RioDB;
import org.riodb.queries.DefaultOutput;
import org.riodb.queries.Query;

import org.riodb.plugin.RioDBOutput;

public final class SQLQueryOperations {

	public static final String createQuery(String stmt, boolean persistStmt, String actingUser) throws ExceptionSQLStatement {

		String statement = SQLParser.formatStmt(stmt);
//		System.out.println("new:" + statement);

		String fromStr = SQLParser.getQueryFromStr(statement);
//		System.out.println(
//				"\n-------------------------------------------\n" + "SELECT FROM: " + SQLParser.textDecode(fromStr));
		SQLQueryResources queryResources = new SQLQueryResources(fromStr);

		String selectStr = SQLParser.getQuerySelectList(statement);
//		System.out.println("\n-------------------------------------------\nSELECT COLUMN: ");
//		System.out.println(selectStr);
		SQLQueryColumn queryColumns[] = SQLQueryColumnOperations.getSelectItems(selectStr, queryResources);
		String columnHeaders[] = new String[queryColumns.length];
		for(int i = 0; i < columnHeaders.length; i++) {
			columnHeaders[i] = queryColumns[i].getHeading();
		}

		String whenStr = SQLParser.getQueryWhenStr(statement);
//		System.out.println(
//				"\n-------------------------------------------\nSELECT WHEN: " + SQLParser.textDecode(whenStr));
		SQLQueryCondition queryCondition = null;
		if (whenStr != null && !whenStr.equals("-")) {
			queryCondition = SQLQueryConditionOperations.getQueryConditions(whenStr, queryResources);
		}

		String limitStr = SQLParser.getQueryLimitStr(statement);
		limitStr = SQLParser.getQueryLimitStr(stmt);
//		System.out.println("\n-------------------------------------------\nSELECT LIMIT: " + limitStr);
		int limit = -1;
		char limitUnit = 'q'; // quantity
		boolean limitByTime = false;
		if (limitStr != null && limitStr.length() > 0) {
			if (SQLParser.isNumber(limitStr)) {
				limit = Integer.valueOf(limitStr);
				limitByTime = false;
			} else if (limitStr.length() >= 2) {
				String limitNum = limitStr.substring(0, limitStr.length() - 1);
				if (SQLParser.isNumber(limitNum)) {
					limitByTime = true;
					limit = Integer.valueOf(limitNum);
					limitUnit = limitStr.toLowerCase().charAt(limitStr.length() - 1);
					if (limitUnit == 's') {
						;
					} else if (limitUnit == 'm') {
						limit = limit * 60;
					} else if (limitUnit == 'h') {
						limit = limit * 60 * 60;
					} else if (limitUnit == 'd') {
						limit = limit * 60 * 60 * 24;
					} else {
						throw new ExceptionSQLStatement(
								"To limit by time, the duration (integer) must be followed by unit, like 15s = fifteen seconds. Units allowed are s, m, h, d. ");
					}

					limit = RioDB.rio.getEngine().getClock().getCurrentSecond() + limit;

				} else {
					throw new ExceptionSQLStatement("limit could not be parsed to a valid number.");
				}
			} else {
				throw new ExceptionSQLStatement("limit '" + limitStr + "' is invalid.");
			}
		}

//		System.out.println("limit " + limit + " unit " + limitUnit);

		String timeoutStr = SQLParser.getQueryTimeoutStr(statement);
//		System.out.println("\n-------------------------------------------\nTIMEOUT: " + timeoutStr);
		int timeout = -1;
		char timeoutUnit = 'q'; // quantity
		boolean timeoutByTime = false;
		if (timeoutStr != null && timeoutStr.length() > 0) {
			if (SQLParser.isNumber(timeoutStr)) {
				timeout = Integer.valueOf(timeoutStr);
				timeoutByTime = false;
			} else if (timeoutStr.length() >= 2) {
				String timeoutNum = timeoutStr.substring(0, timeoutStr.length() - 1);
				if (SQLParser.isNumber(timeoutNum)) {
					timeoutByTime = true;
					timeout = Integer.valueOf(timeoutNum);
					timeoutUnit = timeoutStr.toLowerCase().charAt(timeoutStr.length() - 1);
					if (timeoutUnit == 's') {
						;
					} else if (timeoutUnit == 'm') {
						timeout = timeout * 60;
					} else if (timeoutUnit == 'h') {
						timeout = timeout * 60 * 60;
					} else if (timeoutUnit == 'd') {
						timeout = timeout * 60 * 60 * 24;
					} else {
						throw new ExceptionSQLStatement(
								"To set timeout by time, the duration (integer) must be followed by unit, like 15s = fifteen seconds. Units allowed are s, m, h, d. ");
					}
				} else {
					throw new ExceptionSQLStatement("timeout could not be parsed to a valid number.");
				}
			} else {
				throw new ExceptionSQLStatement("timeout '" + timeoutStr + "' is invalid.");
			}
		}
//		System.out.println("timeout " + timeout + " unit " + timeoutUnit);
		
		String outputStr = SQLParser.getQueryOutputStr(statement);

//		System.out.println(
//				"\n-------------------------------------------\nSELECT OUTPUT: " + SQLParser.textDecode(outputStr));
		
		int drivingStreamId = queryResources.getDrivingStreamId();
		
		if(outputStr == null || outputStr.length() == 0) {
			int sessionId = RioDB.rio.getEngine().counterNext();
			outputStr = String.valueOf(drivingStreamId) + "," + String.valueOf(sessionId);
			limit = 1;
			limitUnit = 'q';
			limitByTime = false;
			timeoutByTime = true;
			if(timeout == -1 || timeout > RioDB.rio.getSystemSettings().getHttpInterface().getTimeout()) {
				timeout = RioDB.rio.getSystemSettings().getHttpInterface().getTimeout();
			}
			
			//Output output = SQLQueryOutputOperations.getOutput(outputStr, columnHeaders);
			
			DefaultOutput output = new DefaultOutput(drivingStreamId, sessionId, columnHeaders);

			Query query = new Query(queryCondition, output, queryColumns, limit, limitByTime, timeout, timeoutByTime,
					statement, queryResources);

			int queryId = query.getQueryId();
			RioDB.rio.getEngine().getStream(drivingStreamId).addQueryRef(query);
			
			try {
				String msg = RioDB.rio.getEngine().getStream(drivingStreamId).requestQueryResponse(sessionId, timeout);
				if(msg == null || msg.length() == 0) {
					msg = "\"Query timed out.\"";
				}
				return msg;
			} catch (InterruptedException e) {
				return "\"Query "+ queryId + " process interrupted.";
			}
			
			
		}
		else {
			RioDBOutput output = SQLQueryOutputOperations.getOutput(outputStr, columnHeaders);

			Query query = new Query(queryCondition, output, queryColumns, limit, limitByTime, timeout, timeoutByTime,
					statement, queryResources);

			int queryId = query.getQueryId(); 
			RioDB.rio.getEngine().getStream(drivingStreamId).addQueryRef(query);
			
			
			if(persistStmt && limit == -1) {
				if(actingUser != null && actingUser.equals("SYSTEM")) {
					RioDB.rio.getSystemSettings().getPersistedStatements().loadQueryStmt(queryId, statement);
				} else {
					RioDB.rio.getSystemSettings().getPersistedStatements().addNewQueryStmt(queryId, statement);
				}
			}
			
			
			return "\"Created query "+ queryId + "\"";
			
		}
		
	}
	
	public static final void dropQuery(String stmt) throws ExceptionSQLStatement {

		String newStmt = SQLStreamOperations.formatSQL(stmt);
		//System.out.println(newStmt);

		String words[] = newStmt.split(" ");
		if(words.length >=3 &&
				words[0] != null && words[0].equals("drop") &&
				words[1] != null && words[1].equals("query") &&
				words[2] != null && words[2].length()>0 ) {
			
			
				// if window ID was provided, remove window by ID
				if(SQLParser.isNumber(words[2])) {
					
					int queryId = Integer.valueOf(words[2]);

					
					if(RioDB.rio.getEngine().dropQuery(queryId)) {
						// persist removal of window (so it doesn't get recreated after reboot)
						//String windowName = RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindowName(windowId);
						RioDB.rio.getSystemSettings().getPersistedStatements().dropQueryStmt(queryId);
						return;
					} else {
						throw new ExceptionSQLStatement("Query not found.");
					}
				}
			}
		
		throw new ExceptionSQLStatement("Statement error. Try 'DROP QUERY stream_name.query_id;' where query_id is a number.");
		
	}
	
	public static final String describeQuery(String stmt) throws ExceptionSQLStatement {

		String newStmt = SQLStreamOperations.formatSQL(stmt);
		
		String words[] = newStmt.split(" ");
		if(words.length >=3 &&
				words[0] != null && words[0].equals("describe") &&
				words[1] != null && words[1].equals("query") &&
				words[2] != null && words[2].length()>0 ) {
			
			words[2] = words[2].replace(".", ",");
			String id[] = words[2].split(",");
			
			if(id.length == 2) {
				int streamId = RioDB.rio.getEngine().getStreamId(id[0]);
				if (streamId == -1) {
					throw new ExceptionSQLStatement("Stream not found.");
				}
				
				// if window ID was provided, remove window by ID
				if(SQLParser.isNumber(id[1])) {
					
					int queryId = Integer.valueOf(id[1]);
					return RioDB.rio.getEngine().getStream(streamId).getQueryMgr().describeQuery(queryId);

				}
			}
		} 
		throw new ExceptionSQLStatement("Statement error. Try 'DESCRIBE QUERY stream_name.query_id;' where query_id is a number.");
	
	}
}
