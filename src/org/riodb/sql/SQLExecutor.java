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
import org.riodb.engine.Clock;
import org.riodb.engine.RioDB;
import org.riodb.plugin.RioDBPluginException;

public final class SQLExecutor {

	public final static String execute(String stmt, String actingUser, boolean persistStmt,
			boolean respondWithDetails) {

		// time elapsed....
		long startTime = System.currentTimeMillis();

		ArrayList<String> responseList = new ArrayList<String>();
		String httpResponseStatus = "200";

		if (actingUser == null) {

			responseList.add("No user provided.");
			httpResponseStatus = "401";

		} else if (stmt != null && stmt.contains(";")) {

			if (!actingUser.equals("SYSTEM")) {
				actingUser = actingUser.toUpperCase();
				RioDB.rio.getSystemSettings().getLogger().trace("SQLExecutor received statement from '" + actingUser
						+ "' (" + RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).stringValue() + ")");
			}
			RioDB.rio.getSystemSettings().getLogger().trace("Removing comments...");
			stmt = SQLParser.removeComments(stmt);
			RioDB.rio.getSystemSettings().getLogger().trace("Encoding quoted text...");
			stmt = BASE64Utils.encodeQuotedText(stmt);
			// System.out.println(stmt);

			// split statements from stmt (in case there are multiple statements in one
			// request)
			String statements[] = stmt.split(";");

			// iterate through all statements.
			for (int i = 0; i < statements.length; i++) {

				String statement = statements[i];

				try {
					String originalStatement = statement;
					statement = SQLParser.formatStmt(statement + ";");

					if (statement != null && !statement.equals(";")) {

						RioDB.rio.getSystemSettings().getLogger().debug("  "+actingUser + " Statement " + (i + 1) + ": \""
								+ SQLParser.hidePassword(BASE64Utils.decodeQuotedText(statement)) + "\"");

						if (statement.startsWith("select ")) {
							if (RioDB.rio.getUserMgr() == null
									|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("QUERY")) {
								String queryResponse = SQLQueryOperations.createQuery(statement, persistStmt,
										actingUser);
								if (queryResponse == null || queryResponse.length() == 0) {
									queryResponse = "statement not understood.";
									httpResponseStatus = "400";
								} else if (queryResponse.contains("Query timed out.")) {
									httpResponseStatus = "408";
								}
								responseList.add(queryResponse);

							} else {
								RioDB.rio.getSystemSettings().getLogger().debug("User not authorized to QUERY");
								responseList.add("User not authorized to QUERY.");
								httpResponseStatus = "401";
							}
						} else if (statement.startsWith("create ")) {
							if (statement.contains(" stream ")) {
								if (RioDB.rio.getUserMgr() == null
										|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("STREAM")) {
									String response = SQLStreamOperations.createStream(statement, persistStmt,
											actingUser);
									if (response != null) {
										responseList.add(response);
										RioDB.rio.getSystemSettings().getLogger().info(response);
									} else {
										responseList.add("Stream creation failed.");
										RioDB.rio.getSystemSettings().getLogger().info("Stream creation failed.");
									}
								} else {
									RioDB.rio.getSystemSettings().getLogger()
											.debug("User not authorized to manage streams.");
									responseList.add("User not authorized to manage streams");
									httpResponseStatus = "401";
								}
							} else if (statement.contains(" window ")) {
								if (RioDB.rio.getUserMgr() == null
										|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("WINDOW")) {
									String response = SQLWindowOperations.createWindow(statement, persistStmt,
											actingUser);
									if (response != null) {
										responseList.add(response);
										RioDB.rio.getSystemSettings().getLogger().info(response);
									} else {
										responseList.add("Window creation failed.");
										RioDB.rio.getSystemSettings().getLogger().info("Window creation failed.");
									}
								} else {
									RioDB.rio.getSystemSettings().getLogger()
											.debug("User not authorized to manage windows.");
									responseList.add("User not authorized to manage windows.");
									httpResponseStatus = "401";
								}

							} else if (statement.contains(" user ")) {
								if (RioDB.rio.getUserMgr() == null) {
									RioDB.rio.getSystemSettings().getLogger().debug("User Management is not enabled.");
									responseList.add("User Management is not enabled.");
									httpResponseStatus = "400";
								} else if (RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("ADMIN")) {
									String response = RioDB.rio.getUserMgr().changeRequest(originalStatement,
											actingUser);
									responseList.add(response);
									RioDB.rio.getSystemSettings().getLogger().info(response);
								} else {
									RioDB.rio.getSystemSettings().getLogger().debug("User not authorized with ADMIN.");
									responseList.add("User not authorized with ADMIN.");
									httpResponseStatus = "401";
								}
							} else {
								RioDB.rio.getSystemSettings().getLogger().debug("Unknown CREATE command.");
								responseList.add("Unknown CREATE command..");
								httpResponseStatus = "400";
							}
						} else if (statement.startsWith("drop ")) {
							if (statement.contains(" stream ")) {
								if (RioDB.rio.getUserMgr() == null
										|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("STREAM")) {
									String response = SQLStreamOperations.dropStream(statement);
									responseList.add(response);
									RioDB.rio.getSystemSettings().getLogger().info(response);

								} else {
									RioDB.rio.getSystemSettings().getLogger()
											.debug("User not authorized to manage streams.");
									responseList.add("User not authorized to manage streams.");
									httpResponseStatus = "401";
								}
							} else if (statement.contains(" window ")) {
								if (RioDB.rio.getUserMgr() == null
										|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("WINDOW")) {
									String response = SQLWindowOperations.dropWindow(statement);
									responseList.add(response);
									RioDB.rio.getSystemSettings().getLogger().info(response);

								} else {
									RioDB.rio.getSystemSettings().getLogger()
											.debug("User not authorized to manage windows.");
									responseList.add("User not authorized to manage windows.");
									httpResponseStatus = "401";
								}
							} else if (statement.contains(" query ")) {
								if (RioDB.rio.getUserMgr() == null
										|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("QUERY")) {
									String response = SQLQueryOperations.dropQuery(statement);
									responseList.add(response);
									RioDB.rio.getSystemSettings().getLogger().info(response);
								} else {
									RioDB.rio.getSystemSettings().getLogger()
											.debug("User not authorized to manage queries.");
									responseList.add("User not authorized to manage queries.");
									httpResponseStatus = "401";
								}
							} else if (statement.contains(" user ")) {
								if (RioDB.rio.getUserMgr() == null) {
									responseList.add("User Management is not enabled.");
									httpResponseStatus = "400";
								} else if (RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("ADMIN")) {
									String response = RioDB.rio.getUserMgr().changeRequest(statement, actingUser);
									responseList.add(response);
									RioDB.rio.getSystemSettings().getLogger().info(response);
								} else {
									responseList.add("User not authorized to manage queries.");
									httpResponseStatus = "401";
								}
							} else {
								RioDB.rio.getSystemSettings().getLogger().debug("Unknown DROP command.");
								responseList.add("Unknown DROP command.");
								httpResponseStatus = "400";
							}
						} else if (statement.startsWith("list ")) {
							if (statement.contains(" streams")) {

								if (RioDB.rio.getUserMgr() == null
										|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("STREAM")) {
									responseList.add(RioDB.rio.getEngine().listStreams());
									RioDB.rio.getSystemSettings().getLogger().info("streams listed.");

								} else {
									RioDB.rio.getSystemSettings().getLogger()
											.debug("User not authorized to manage streams.");
									responseList.add("User not authorized to manage streams.");
									httpResponseStatus = "401";
								}

							} else if (statement.contains(" windows")) {

								if (RioDB.rio.getUserMgr() == null
										|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("WINDOW")) {
									responseList.add(RioDB.rio.getEngine().listAllWindows());
									RioDB.rio.getSystemSettings().getLogger().info("listed windows.");
								} else {
									RioDB.rio.getSystemSettings().getLogger()
											.debug("User not authorized to manage windows.");
									responseList.add("User not authorized to manage windows.");
									httpResponseStatus = "401";
								}

							} else if (statement.contains(" queries")) {

								if (RioDB.rio.getUserMgr() == null
										|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("QUERY")) {
									responseList.add(RioDB.rio.getEngine().listAllQueries());
									RioDB.rio.getSystemSettings().getLogger().info("queries listed.");
								} else {
									RioDB.rio.getSystemSettings().getLogger()
											.debug("User not authorized to manage queries.");
									responseList.add("User not authorized to manage queries.");
									httpResponseStatus = "401";
								}

							} else if (statement.contains(" users")) {

								if (RioDB.rio.getUserMgr() == null) {
									responseList.add("User Management is not enabled.");
									httpResponseStatus = "400";
								} else if (RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("ADMIN")) {
									responseList.add(RioDB.rio.getUserMgr().listUsers());
									RioDB.rio.getSystemSettings().getLogger().debug("users listed.");
								} else {
									responseList.add("User not authorized to manage queries.");
									httpResponseStatus = "401";
								}

							}
						} else if (statement.startsWith("describe ")) {
							String[] words = statement.split(" ");
							if (words.length == 3) {
								words[2] = words[2].substring(0, words[2].indexOf(";")).trim();
								if (words[1].equals("stream")) {
									if (RioDB.rio.getUserMgr() == null
											|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("STREAM")) {
										responseList.add(RioDB.rio.getEngine().describe(words[2]));
									} else {
										responseList.add("User not authorized to manage streams.");
										httpResponseStatus = "401";
									}
								} else if (words[1].equals("window")) {
									if (RioDB.rio.getUserMgr() == null
											|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("WINDOW")) {
										String description = SQLWindowOperations.describeWindow(statement);
										responseList.add(description);
									} else {
										RioDB.rio.getSystemSettings().getLogger()
												.debug("User not authorized to manage windows.");
										responseList.add("User not authorized to manage windows.");
										httpResponseStatus = "401";
									}
								} else if (words[1].equals("query")) {
									if (RioDB.rio.getUserMgr() == null
											|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("QUERY")) {
										String description = SQLQueryOperations.describeQuery(statement);
										responseList.add(description);

									} else {
										RioDB.rio.getSystemSettings().getLogger()
												.debug("User not authorized to manage windows.");
										responseList.add("User not authorized to manage windows.");
										httpResponseStatus = "401";
									}
								} else {
									RioDB.rio.getSystemSettings().getLogger()
											.debug("Bad describe command: " + statement);
									responseList.add(
											"Describe command should be like... DESCRIBE STREAM stream_name;  or DESCRIBE WINDOW stream_name.window_name; or DESCRIBE QUERY stream_name.0;");
									httpResponseStatus = "400";
								}

							} else {
								RioDB.rio.getSystemSettings().getLogger()
										.debug("Describe Query command with bad syntax.");
								responseList.add(
										"Describe command should be like... DESCRIBE STREAM stream_name;  or DESCRIBE WINDOW stream_name.window_name; or DESCRIBE QUERY stream_name.0;");
								httpResponseStatus = "400";
							}
						} else if (statement.startsWith("reset window ")) {
							if (RioDB.rio.getUserMgr() == null
									|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("WINDOW")) {

								String target = statement.substring(13);
								if (target != null && target.contains(".")) {
									String streamName = target.substring(0, target.indexOf(".") - 1).trim();
									if (RioDB.rio.getEngine().getStreamId(streamName) >= 0) {
										String windowName = target.substring(target.indexOf(".")).trim();
										if (windowName.equals("all")) {
											RioDB.rio.getEngine().getStream(streamName).resetAllWindows();
											responseList.add("Reset all windows for stream " + streamName + ".");
										} else if (RioDB.rio.getEngine().getStream(streamName).getWindowMgr()
												.hasWindow(windowName)) {
											RioDB.rio.getEngine().getStream(streamName).getWindowMgr()
													.resetWindow(windowName);
											responseList.add("Reset window " + target + ".");
										} else {
											responseList.add("Window " + target + " was not found.");
											httpResponseStatus = "400";
										}
									} else {
										responseList.add("Stream " + streamName + " does not exist.");
										httpResponseStatus = "400";
									}
								} else {
									responseList.add(
											"Invalid syntax for reset command. Try: reset window streamName.windowName; ");
									httpResponseStatus = "400";

								}

							} else {
								RioDB.rio.getSystemSettings().getLogger()
										.debug("User not authorized to manage windows.");
								responseList.add("User not authorized to manage windows.");
								httpResponseStatus = "401";
							}

						} else if (statement.startsWith("system ")) {
							String verb = statement.substring(7).replace(";", "");
							if (verb.equals("status")) {
								responseList.add(RioDB.rio.getEngine().status());
							} else if (RioDB.rio.getUserMgr() == null
									|| RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("ADMIN")) {

								if (verb.equals("start")) {
									if (!RioDB.rio.getEngine().isOnline()) {
										RioDB.rio.getEngine().start();
									}
									responseList.add("RioDB is online. Streams are running");
								} else if (verb.equals("stop")) {
									if (RioDB.rio.getEngine().isOnline()) {
										RioDB.rio.getEngine().stop();
									}
									responseList.add("RioDB is offline. Streams are stopped.");
								} else {
									responseList.add("System command '" + verb
											+ "' unknown. Try 'system start;' or 'system stop;'");
									httpResponseStatus = "400";
								}

							} else {
								responseList.add("User not authorized to manage system.");
								httpResponseStatus = "401";
							}

						} else if (statement.startsWith("change user ")) {
							if (RioDB.rio.getUserMgr() == null) {
								responseList.add("User Management is not enabled.");
								httpResponseStatus = "400";
							} else if (RioDB.rio.getUserMgr().getUserAccessLevel(actingUser).can("ADMIN")) {
								String response = RioDB.rio.getUserMgr().changeRequest(originalStatement, actingUser);
								responseList.add(response);
								RioDB.rio.getSystemSettings().getLogger().info(response);
							} else {
								responseList.add("User not authorized to manage queries.");
								httpResponseStatus = "401";
							}
						} else if (statement.startsWith("resetpwd ") || statement.startsWith("resetpassword ")) {
							if (RioDB.rio.getUserMgr() == null) {
								responseList.add("User Management is not enabled.");
								httpResponseStatus = "400";
							}
							String newStmt = originalStatement.replace("resetpwd ",
									"CHANGE user " + actingUser + " set password ");
							newStmt = newStmt.replace("resetpassword ", "CHANGE user " + actingUser + " set password ");

							String response = RioDB.rio.getUserMgr().changeRequest(newStmt, actingUser);
							responseList.add(response);
							RioDB.rio.getSystemSettings().getLogger().info(response);

						} else if (statement.startsWith("sleep ")) {
							statement = statement.replace(";", "").trim();
							String parts[] = statement.split(" ");
							if (parts.length == 2 && SQLParser.isNumber(parts[1])) {
								int ms = Integer.parseInt(parts[1]);
								if (ms > 0) {
									Clock.sleep(Integer.parseInt(parts[1]));
									responseList.add("slept " + parts[1]);
								} else {
									responseList.add("invalid syntax for 'sleep' statement. try  'sleep 50;' ");
									httpResponseStatus = "400";
								}
							} else {
								responseList.add("invalid syntax for 'sleep' statement. try  'sleep 50;' ");
								httpResponseStatus = "400";
							}

						}
					}

				} catch (ExceptionSQLStatement e) {
					RioDB.rio.getSystemSettings().getLogger().debug("FAILED statement "+ (i + 1) +". "+ e.getMessage().replace("\n"," "));
					responseList.add(e.getMessage().replace("\"", "\\\""));
					httpResponseStatus = "400";
				} catch (ExceptionAccessMgt e) {
					RioDB.rio.getSystemSettings().getLogger().debug("FAILED statement "+ (i + 1)+". "+ e.getMessage().replace("\n"," "));
					responseList.add(e.getMessage().replace("\"", "\\\""));
					httpResponseStatus = "401";
				} catch (RioDBPluginException e) {
					RioDB.rio.getSystemSettings().getLogger().debug("FAILED statement "+ (i + 1) +". "+ e.getMessage().replace("\n"," "));
					responseList.add(e.getMessage().replace("\"", "\\\""));
					httpResponseStatus = "500";
				}

			}
		} else if (stmt == null || stmt.length() == 0) {
			responseList.add("RioDB here. Tell me WHEN.");
			httpResponseStatus = "200";
		} else {
			responseList.add("Invalid statement. Make sure you finish each statement with a semicolon ';'");
			httpResponseStatus = "400";
		}

		long finishTime = System.currentTimeMillis();

		StringBuilder responseJson = new StringBuilder("{ \"status\": ");
		responseJson.append(httpResponseStatus).append(", \"statements\": ").append(responseList.size())
				.append(", \"milliseconds\": ").append((finishTime - startTime));

		if (respondWithDetails) {

			responseJson.append(",\"details\":[ ");

			for (int i = 0; i < responseList.size(); i++) {

				responseJson.append("\n  { \"statement\": ").append((i + 1)).append(",\n  \"response\": ");

				String response = responseList.get(i);
				if (response == null || response.length() == 0) {
					response = "null";
				}
				if (!response.startsWith("{") && !response.startsWith("[")) {
					response = "\"" + response + "\"";
				}
				responseJson.append(response.replace("\n", " ").replace("\r", " ").replace("\t", " ")).append("},");

			}
			// remove last comma and close bracket
			responseJson.deleteCharAt(responseJson.length() - 1);

			responseJson.append(" ]\n}");

		} else {
			responseJson.append(" }");
		}

		return responseJson.toString();
	}

}
