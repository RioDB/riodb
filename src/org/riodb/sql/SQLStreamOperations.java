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

import java.util.LinkedHashMap;

import org.riodb.engine.RioDB;
import org.riodb.engine.Stream;

import org.riodb.plugin.RioDBPluginException;
import org.riodb.plugin.RioDBStreamMessageDef;
import org.riodb.plugin.RioDBStreamFieldDef;

public final class SQLStreamOperations {

	public static final String createStream(String statement, boolean persistStmt, String actingUser)
			throws ExceptionSQLStatement, RioDBPluginException {
		
		RioDB.rio.getSystemSettings().getLogger().trace("SQLStreamOperations.createStream.");
		
		boolean success = true;

		String streamName = SQLStreamOperations.getStreamName(statement);
		RioDB.rio.getSystemSettings().getLogger().trace("STREAM_NAME: "+ streamName);
		if (streamName == null || streamName.length() == 0) {
			RioDB.rio.getSystemSettings().getLogger().error("ERROR: Stream must have a name and at least one field.");
			throw new ExceptionSQLStatement("ERROR: a stream name is required.");
		}
		
		if(RioDB.rio.getEngine().getStreamId(streamName) >= 0) {
			throw new ExceptionSQLStatement("ERROR: a stream with this name already exists.");
		}

		// get field definition
		LinkedHashMap<String, String> map = SQLStreamOperations.getFields(statement);
		RioDB.rio.getSystemSettings().getLogger().trace("STREAM_FIELDS: "+ map.size());
		
		if (map.size() > 0) {

			RioDBStreamMessageDef def = new RioDBStreamMessageDef();

			// field that will be the timestamp, if there is one.
			int timestampNumericFieldId = -1;
			// string format of the incoming timestamp field
			String timestampFormat = null;
			// if timestamp field is coming as epoch, is it milliseconds or seconds since 1970?
			boolean timestampMillis = false;

			int fieldCounter = 0;
			for (String key : map.keySet()) {
				boolean isNumber = false;
				if (map.get(key) != null && (map.get(key).equals("number") || map.get(key).startsWith("timestamp"))) {
					isNumber = true;
					if (map.get(key).startsWith("timestamp")) {
						if (timestampNumericFieldId >= 0) {
							RioDB.rio.getSystemSettings().getLogger().error("The stream can only have one 'timestamp' field. Others can be put in as 'number'.");
							throw new ExceptionSQLStatement(
									"The stream can only have one 'timestamp' field. Others can be put in as 'number'.");
						}

						timestampNumericFieldId = fieldCounter;
						String parts[] = map.get(key).split(" ");
						if(parts.length > 1) {
							if(parts[1].startsWith("'") && parts[1].endsWith("'")) {
								parts[1] = SQLParser.decodeQuotedText(parts[1]);
								timestampFormat = parts[1].substring(1,parts[1].length()-1);
								timestampFormat = timestampFormat.replace("''", "'");
								RioDB.rio.getSystemSettings().getLogger().trace("STREAM_TIMESTAMP_FORMAT: "+ timestampFormat);
							} else if(parts[1].equals("millis")){
								timestampMillis = true;
							} else {
								RioDB.rio.getSystemSettings().getLogger().error("Timestamp argument unknown: "+ parts[1]);
								throw new ExceptionSQLStatement(
										"Timestamp argument unknown: "+ parts[1]);
							}
							
						}
					}

				}
				RioDBStreamFieldDef f = new RioDBStreamFieldDef(key, isNumber);
				def.addField(f);
				fieldCounter++;
			}

			def.setTimestampNumericFieldId(timestampNumericFieldId, timestampFormat, timestampMillis);

			String inputType = SQLStreamOperations.getListenerType(statement);
			RioDB.rio.getSystemSettings().getLogger().trace("STREAM_LISTENER_TYPE: "+ inputType);
			String inputParams = SQLStreamOperations.getListenerParams(statement);
			RioDB.rio.getSystemSettings().getLogger().trace("STREAM_PARAMS: "+ inputParams);
			if(inputParams != null) {
				inputParams = SQLParser.decodeQuotedText(inputParams);
				RioDB.rio.getSystemSettings().getLogger().trace("STREAM_DECODED_INPUT_PARAMS: "+ inputParams);
			}
			Stream newStream = new Stream(RioDB.rio.getEngine().getStreamCounter(), streamName, def, inputType,
					inputParams);
			RioDB.rio.getSystemSettings().getLogger().trace("Adding stream to engine...");
			RioDB.rio.getEngine().addStream(newStream);
			RioDB.rio.getSystemSettings().getLogger().debug("New stream timestamp: "+ newStream.getDef().getTimestampFieldName() +" "+ newStream.getDef().getTimestampFormat());

			if (persistStmt) {
				if (actingUser != null && actingUser.equals("SYSTEM")) {
					// statement was run during boot. No need to save statement to file.
					RioDB.rio.getSystemSettings().getLogger().trace("Loading statement for tracking...");
					RioDB.rio.getSystemSettings().getPersistedStatements().loadStreamStmt(streamName, statement);
					
				} else {
					// new statement. Save to file. 
					RioDB.rio.getSystemSettings().getLogger().trace("Saving statement to SQL files...");
					RioDB.rio.getSystemSettings().getPersistedStatements().saveNewStreamStmt(streamName, statement);
				}
			}
			
			if (RioDB.rio.getEngine().isOnline()) {
				newStream.start();
			}


		} else {
			throw new ExceptionSQLStatement("SQL ERROR: CREATE STREAM . needs field declaration.");
		}

		if(success) {
			return "Created stream " + streamName;
		}
		
		return null;

	}

	public static final String dropStream(String stmt) throws ExceptionSQLStatement {
		
		RioDB.rio.getSystemSettings().getLogger().debug("SQLStreamOperations.dropStream.");

		String newStmt = SQLStreamOperations.formatSQL(stmt);

		String words[] = newStmt.split(" ");
		if (words.length >= 3 && words[0] != null && words[0].equals("drop") && words[1] != null
				&& words[1].equals("stream") && words[2] != null && words[2].length() > 0) {

			words[2] = words[2].replace(";", "").trim();
			int streamId = RioDB.rio.getEngine().getStreamId(words[2]);

			if (streamId == -1) {
				throw new ExceptionSQLStatement("Stream not found.");
			}

			if (RioDB.rio.getEngine().getStream(streamId).getQueryCount() > 0) {
				throw new ExceptionSQLStatement(
						"Stream " + words[2] + " has active queries. Drop queries and windows first.");
			}

			if (RioDB.rio.getEngine().getStream(streamId).getWindowCount() > 0) {
				throw new ExceptionSQLStatement("Stream " + words[2] + " has active windows. Drop windows first.");
			}

			if (RioDB.rio.getEngine().getStream(streamId).hasWindowDependantOnStream(streamId)) {
				throw new ExceptionSQLStatement("There are windows using this stream. Drop the windows first.");
			}

			if (RioDB.rio.getEngine().getStream(streamId).hasQueryDependantOnStream(streamId)) {
				throw new ExceptionSQLStatement("There are queries using this stream. Drop the queries first.");
			}

			if (RioDB.rio.getEngine().removeStream(streamId)) {
				RioDB.rio.getSystemSettings().getLogger().debug("Removing stream creation statememt from startup sql.");
				RioDB.rio.getSystemSettings().getPersistedStatements().dropStreamStmt(words[2]);
				return "Stream "+words[2]+ " dropped." ;
			}

		} 
		
		throw new ExceptionSQLStatement("Statement error. Try 'DROP STREAM stream_name;");
		
	}

	/*
	 * STRING FUNCTIONS
	 */

	public static final String formatSQL(String stmt) throws ExceptionSQLStatement {

		//TODO: Remove this function. 
		return stmt;
		/*
		if (stmt == null) {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(1, stmt));
		}
		String newStmt = stmt.trim(); // .toLowerCase();

		newStmt = newStmt.replace("\t", " ");
		newStmt = newStmt.replace("\n", " ");
		newStmt = newStmt.replace("\r", " ");
		newStmt = newStmt.replace(")", " ) ");
		newStmt = newStmt.replace("(", " ( ");
		while (newStmt.contains("  ")) {
			newStmt = newStmt.replace("  ", " ");
		}
		newStmt = newStmt.trim();

		if (newStmt.charAt(newStmt.length() - 1) != ';') {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(2, stmt));
		} else {
			newStmt = newStmt.substring(0, newStmt.length() - 1) + " ;";
		}

		// count parenthesis:
		int op = 0;
		int cp = 0;
		for (int i = 0; i < newStmt.length() - 1; i++) {
			if (newStmt.charAt(i) == '(') {
				op++;
			} else if (newStmt.charAt(i) == ')') {
				cp++;
			}
		}
		if (op > cp) {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(5, stmt));
		} else if (op < cp) {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(6, stmt));
		}
		return newStmt;
		*/
	}

	public static final String getStreamName(String stmt) throws ExceptionSQLStatement {

		String newStmt = formatSQL(stmt);

		String words[] = newStmt.split(" ");

		if (words.length < 6) {
			throw new ExceptionSQLStatement("SQL ERROR: CREATE STREAM . needs more info.");
		}
		if (words[0] != null && words[0].equals("create") && words[1] != null && words[1].equals("stream")) {
			return words[2];
		}
		return null;
	}

	public static final LinkedHashMap<String, String> getFields(String stmt) throws ExceptionSQLStatement {
		LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
		String newStmt = formatSQL(stmt);
		int fromIndex = newStmt.indexOf("(") + 1;
		if (fromIndex > 0) {
			int toIndex = newStmt.indexOf(")", fromIndex);
			if (toIndex > fromIndex) {
				String subs = newStmt.substring(fromIndex, toIndex);

				String fields[] = subs.split(",");
				for (int i = 0; i < fields.length; i++) {
					fields[i] = fields[i].trim();
					if (fields[i].contains(" ")) {
						String parts[] = fields[i].split(" ");
						if(parts.length > 2) {
							parts[1] = parts[1] + " " + parts[2];
						}
						map.put(parts[0], parts[1]);
					}
				}
			} else {
				throw new ExceptionSQLStatement("SQL ERROR: expected close parenthesis.");
			}
		} else {
			throw new ExceptionSQLStatement("SQL ERROR: Create Stream missing field declaration.");
		}

		return map;
	}

	public static final String getListenerType(String stmt) throws ExceptionSQLStatement {
		String newStmt = formatSQL(stmt);

		if (newStmt == null || !newStmt.contains(" input ")) {
			throw new ExceptionSQLStatement("SQL ERROR: Create Stream missing 'input' keyword.");
		}

		String listenerType = newStmt.substring(newStmt.indexOf(" input ") + 7).trim();
		listenerType = listenerType.substring(0, listenerType.indexOf(" ")).trim();

		return listenerType;
	}

	public static final String getListenerParams(String stmt) throws ExceptionSQLStatement {
		
		//String newStmt = formatSQL(stmt);
		//String newStmt = SQLParser.formatStmt(stmt);	
		
		String newStmt = stmt;
//		System.out.println("getListenerParams: "+ newStmt);
		
		if (newStmt == null || !newStmt.contains(" input ")) {
			throw new ExceptionSQLStatement("SQL ERROR: Create Stream missing 'input' keyword.");
		}

		String listenerParams = newStmt.substring(newStmt.indexOf(" input ") + 7).trim();
		listenerParams = listenerParams.substring(listenerParams.indexOf("(") + 1).trim();
		listenerParams = listenerParams.substring(0, listenerParams.lastIndexOf(")")).trim();

		return listenerParams;
	}

	public static final String getWhere(String stmt) throws ExceptionSQLStatement {

		String where = "-";

		int fromIndex = stmt.indexOf(" from ");

		int whereIndex = stmt.indexOf(" where ", fromIndex);
		if (whereIndex == -1)
			whereIndex = stmt.indexOf(" where(", fromIndex);

		if (whereIndex > fromIndex) {
			whereIndex = whereIndex + 6;
			if (stmt.indexOf(" having", whereIndex) > whereIndex) {
				where = stmt.substring(whereIndex, stmt.indexOf(" having ", whereIndex));
			} else if (stmt.indexOf(" partition ") > whereIndex) {
				where = stmt.substring(whereIndex, stmt.indexOf(" partition by ", whereIndex));
			} else if (stmt.indexOf(" limit ") > whereIndex) {
				where = stmt.substring(whereIndex, stmt.indexOf(" limit ", whereIndex));
			} else if (stmt.indexOf(" sleep ") > whereIndex) {
				where = stmt.substring(whereIndex, stmt.indexOf(" sleep ", whereIndex));
			} else {
				where = stmt.substring(whereIndex, stmt.indexOf(";", whereIndex));
			}
		}

		return where.trim();
	}

}
