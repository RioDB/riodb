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

import java.util.ArrayList;

import org.riodb.access.ExceptionAccessMgt;
import org.riodb.engine.RioDB;

import org.riodb.plugin.RioDBPluginException;

public final class SQLExecutor {

	public final static String execute(String stmt, String actingUser)
			throws ExceptionSQLStatement, RioDBPluginException, ExceptionAccessMgt {

		ArrayList<String> responseList = new ArrayList<String>();
		int responseCode = 0; // 0 = good. 1 = at least 1 command didn't work. 2 = fatal.

		if (stmt != null && stmt.contains(";")) {
			
			stmt = SQLParser.formatStripComments(stmt);

			String statements[] = stmt.split(";");

			for (String statement : statements) {
				
				String originalStatement = statement;
				statement = SQLParser.formatStmt(statement + ";");

				if (statement != null && !statement.equals(";")) {

					if (statement.startsWith("select ")) {
						if (RioDB.rio.getUserMgr() == null
								|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("QUERY")) {
							RioDB.rio.getSystemSettings().getLogger().debug("Statement: " + statement);
							
							responseList.add(SQLQueryOperations.createQuery(statement));
						} else {
							RioDB.rio.getSystemSettings().getLogger().debug("User not authorized to QUERY");
							responseList.add("\"User not authorized to QUERY.\"");
							responseCode = 1;
						}
					} else if (statement.startsWith("create ")) {
						if (statement.contains(" stream ")) {
							if (RioDB.rio.getUserMgr() == null
									|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("STREAM")) {
								if (SQLStreamOperations.createStream(statement)) {
									responseList.add("\"Stream created.\"");
									RioDB.rio.getSystemSettings().getLogger().info("Stream created.");
								} else {
									responseList.add("\"Stream creation failed.\"");
									RioDB.rio.getSystemSettings().getLogger().info("Stream creation failed.");
								}
							} else {
								RioDB.rio.getSystemSettings().getLogger().debug("User not authorized to manage streams.");
								responseList.add("\"User not authorized to manage streams\"");
								responseCode = 1;
							}
						} else if (statement.contains(" window ")) {
							if (RioDB.rio.getUserMgr() == null
									|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("WINDOW")) {
								if (SQLWindowOperations.createWindow(statement)) {
									responseList.add("\"Window created.\"");
									RioDB.rio.getSystemSettings().getLogger().info("Window created.");
								}
							} else {
								RioDB.rio.getSystemSettings().getLogger().debug("User not authorized to manage windows.");
								responseList.add("\"User not authorized to manage windows.\"");
								responseCode = 1;
							}

						} else if (statement.contains(" user ")) {
							if (RioDB.rio.getUserMgr() == null) {
								RioDB.rio.getSystemSettings().getLogger().debug("User Management is not enabled.");
								responseList.add("\"User Management is not enabled.\"");
								responseCode = 1;
							} else if (RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("ADMIN")) {
								RioDB.rio.getUserMgr().changeRequest(originalStatement, actingUser);
								responseList.add("\"User created.\"");
								RioDB.rio.getSystemSettings().getLogger().debug("User created.");
							} else {
								RioDB.rio.getSystemSettings().getLogger().debug("User not authorized with ADMIN.");
								responseList.add("\"User not authorized with ADMIN.\"");
								responseCode = 1;
							}
						} else {
							RioDB.rio.getSystemSettings().getLogger().debug("Unknown CREATE command.");
							throw new ExceptionSQLStatement("Unknown CREATE command.");
						}
					} else if (statement.startsWith("drop ")) {
						if (statement.contains(" stream ")) {
							if (RioDB.rio.getUserMgr() == null
									|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("STREAM")) {
								if (SQLStreamOperations.dropStream(statement)) {
									responseList.add("\"Stream dropped.\"");
									RioDB.rio.getSystemSettings().getLogger().info("Stream dropped.");
								} else {
									responseList.add("\"Stream not found.\"");
									responseCode = 1;
								}
							} else {
								RioDB.rio.getSystemSettings().getLogger().debug("User not authorized to manage streams.");
								responseList.add("\"User not authorized to manage streams.\"");
								responseCode = 1;
							}
						} else if (statement.contains(" window ")) {
							if (RioDB.rio.getUserMgr() == null
									|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("WINDOW")) {
								if (SQLWindowOperations.dropWindow(statement)) {
									responseList.add("\"Window dropped.\"");
									RioDB.rio.getSystemSettings().getLogger().info("window dropped.");
								} else {
									responseList.add("\"Window not found.\"");
									responseCode = 1;
								}
							} else {
								RioDB.rio.getSystemSettings().getLogger().debug("User not authorized to manage windows.");
								responseList.add("\"User not authorized to manage windows.\"");
								responseCode = 1;
							}
						} else if (statement.contains(" query ")) {
							if (RioDB.rio.getUserMgr() == null
									|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("QUERY")) {
								if (SQLQueryOperations.dropQuery(statement)) {
									responseList.add("\"Query dropped.\"");
									RioDB.rio.getSystemSettings().getLogger().info("Query dropped.");
								} else {
									responseList.add("\"Query not found.\"");
									responseCode = 1;
								}
							} else {
								RioDB.rio.getSystemSettings().getLogger().debug("User not authorized to manage queries.");
								responseList.add("\"User not authorized to manage queries.\"");
								responseCode = 1;
							}
						} else if (statement.contains(" user ")) {
							if (RioDB.rio.getUserMgr() == null) {
								responseList.add("\"User Management is not enabled.\"");
								responseCode = 1;
							} else if (RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("ADMIN")) {
								RioDB.rio.getUserMgr().changeRequest(statement, actingUser);
								responseList.add("User dropped.");
								RioDB.rio.getSystemSettings().getLogger().debug("User dropped.");
							} else {
								responseList.add("\"User not authorized to manage queries.\"");
								responseCode = 1;
							}
						} else {
							throw new ExceptionSQLStatement("Unknown drop command.");
						}
					} else if (statement.startsWith("kill ")) {
//						success = killQuery(statement);
//						response = response + "Query killed.\n";
					} else if (statement.startsWith("list ")) {
						if (statement.contains(" streams")) {
							responseList.add(RioDB.rio.getEngine().listStreams());
						} else if (statement.contains(" windows")) {
							responseList.add(RioDB.rio.getEngine().listAllWindows());
						} else if (statement.contains(" queries")) {
							responseList.add(RioDB.rio.getEngine().listAllQueries());
						} else if (statement.contains(" users")) {
							responseList.add(RioDB.rio.getUserMgr().listUsers());
						}
					} else if (statement.startsWith("describe ")) {
						String streamName = statement.substring(9);
						if (streamName.length() > 2 && streamName.indexOf(";") > 0) {
							if (RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("ADMIN")) {
								streamName = streamName.substring(0, streamName.indexOf(";")).trim();
								responseList.add(RioDB.rio.getEngine().describe(streamName));
							} else {
								responseList.add("\"User not authorized to manage streams.\"");
								responseCode = 1;
							}
						}
					} else if (statement.startsWith("status")) {
						responseList.add(RioDB.rio.getEngine().status());
					} else if (statement.startsWith("change user ")) {
						if (RioDB.rio.getUserMgr() == null) {
							responseList.add("\"User Management is not enabled.\"");
							responseCode = 1;
						} else if (RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("ADMIN")) {
							String r = RioDB.rio.getUserMgr().changeRequest(originalStatement, actingUser);
							responseList.add("\"" + r + "\"");
							RioDB.rio.getSystemSettings().getLogger().debug(r);
						} else {
							responseList.add("\"User not authorized to manage queries.\"");
							responseCode = 1;
						}
					}
				}
			}
		}

		String responseJson = "{\"code\": " + responseCode + ", \"message\": ";
		if (responseList.size() > 1) {
			responseJson = responseJson + "[";
		}
		for (String s : responseList) {
			if (s == null || s.length() == 0) {
				s = "null";
			}
			responseJson = responseJson + s + ",";
		}
		// remove last comma and close bracket
		responseJson = responseJson.substring(0, responseJson.length() - 1);
		if (responseList.size() > 1) {
			responseJson = responseJson + "]";
		}
		responseJson = responseJson + "}";

		return responseJson;
	}

}
