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
import org.riodb.windows.Window;
import org.riodb.windows.WindowOfOne;
import org.riodb.windows.WindowOfOne_String;
import org.riodb.windows.WindowOfQuantity;
import org.riodb.windows.WindowOfQuantity_String;
import org.riodb.windows.WindowOfTimeComplex;
import org.riodb.windows.WindowOfTimeComplex_String;
import org.riodb.windows.WindowOfTimeSimple;
import org.riodb.windows.WindowOfTimeSimple_String;
import org.riodb.windows.WindowWrapper;
import org.riodb.windows.WindowWrapperPartitioned;
import org.riodb.windows.WindowWrapperPartitioned_String;
import org.riodb.windows.WindowWrapper_String;
import org.riodb.windows.Window_String;

public final class SQLWindowOperations {
	public static final String createWindow(String stmt, boolean persistStmt, String actingUser)
			throws ExceptionSQLStatement {

		if (!stmt.contains(" when ") && stmt.contains(" where ")) {
			throw new ExceptionSQLStatement("Did you mean WHEN instead of WHERE?");
		}

		// get window name
		String windowName = SQLParser.getWindowStr(stmt);
		if (RioDB.rio.getEngine().getStreamIdOfWindow(windowName) >= 0) {
			throw new ExceptionSQLStatement("A window named '" + windowName + "' already exists.");
		}

		RioDB.rio.getSystemSettings().getLogger().trace("\tWINDOW: " + windowName);

		// get window functions
		String functionStr = SQLParser.getWindowRunningFunctions(stmt);
		boolean functionsRequired[] = SQLAggregateFunctions.getFunctionsRequired(functionStr);

		String functionsRequiredStr = "";
		
		for (int i = 0; i < functionsRequired.length; i++) {
			if(functionsRequired[i]) {
				functionsRequiredStr += " " + SQLAggregateFunctions.getFunction(i);
			}
		}
			
		RioDB.rio.getSystemSettings().getLogger().trace("\tRUNNING:" + functionsRequiredStr);

		// get window Stream
		int streamId = SQLParser.getWindowStreamId(stmt);

		RioDB.rio.getSystemSettings().getLogger().trace("\tSTREAM: " + streamId);
		
		String fromStr = SQLParser.getWindowFromStr(stmt);
		
		int fieldId = -1;
		
		boolean windowOfNumbers = true;
		SQLWindowSourceExpression windowSourceExpression = null;
		
		String whereStr = SQLParser.getWindowWhereStr(stmt);
		boolean whereClauseRequiresPrevious = false;
		if(whereStr != null && whereStr.contains(" previous ")) {
			whereClauseRequiresPrevious = true;
		}
		
		if (fromStr.startsWith("number (")) {
			windowSourceExpression = SQLWindowSourceOperations.getWindowSource(fromStr, streamId, whereClauseRequiresPrevious);
			
			
		} 
		else if (fromStr.startsWith("string (")) {
			windowOfNumbers = false;
			windowSourceExpression = SQLWindowSourceOperations.getWindowSource(fromStr, streamId, whereClauseRequiresPrevious);
			
		} else {
			fieldId = SQLParser.getWindowFieldId(stmt);
			
			// user wants to create a window on a String field
			if (!RioDB.rio.getEngine().getStream(streamId).getDef().isNumeric(fieldId)) {
				windowOfNumbers = false;
			}
			RioDB.rio.getSystemSettings().getLogger().trace("\tfieldId: " + fieldId + " fieldIsNumeric: " + windowOfNumbers);

		}
	


		

		// get window condition (WHERE...)
		SQLWindowCondition whereClause = null;
		

		if (whereStr != null) {
			RioDB.rio.getSystemSettings().getLogger().trace("\tWHEN: " + whereStr);
			whereClause = SQLWindowConditionOperations.getWindowCondition(whereStr, streamId);
		}
		
		

		// get window RANGE information
		// default is range by quantity.
		boolean rangeByTime = false;
		boolean rangeByTimeIsTimestamp = false; // NOT CLOCK-based.
		int windowRange = 0;
		int windowRangeEnd = -1; // negative 1 means it's not used.

		String rangeStr = SQLParser.getWindowRangeStr(stmt);
		String rangeEndStr = null;

		if (rangeStr != null && rangeStr.contains(" - ")) {
			rangeEndStr = rangeStr.substring(rangeStr.indexOf(" - ") + 3).trim();
			rangeStr = rangeStr.substring(0, rangeStr.indexOf(" - "));
		}

		// if rangeStr is just a number, then it's a range of quantity (integer)
		if (SQLParser.isNumber(rangeStr)) {
			windowRange = Integer.valueOf(rangeStr);
			if (rangeEndStr != null && SQLParser.isNumber(rangeEndStr)) {
				windowRangeEnd = Integer.valueOf(rangeEndStr);
			}
		}
		// if clock or timestamp is specified, then it's range by time.
		else if (rangeStr != null && (rangeStr.contains("clock ") || rangeStr.contains("timestamp "))) {

			rangeByTime = true;

			String words[] = rangeStr.split(" ");
			if (words.length < 2) {
				throw new ExceptionSQLStatement("'range' clause is missing the range.");
			}
			if (words[0].equals("timestamp")) {
				rangeByTimeIsTimestamp = true;
				if (RioDB.rio.getEngine().getStream(streamId).getDef().getTimestampNumericFieldId() == -1) {
					throw new ExceptionSQLStatement("Stream '" + RioDB.rio.getEngine().getStream(streamId).getName()
							+ "' does not have a timestamp field.");
				}
			}
			rangeStr = words[1];

		} else {
			// then let's fallback to time "clock".
			rangeByTime = true;
		}

		if (rangeByTime) {
			windowRange = getRangeTime(rangeStr);
			if (rangeEndStr != null && rangeEndStr.length() > 0) {
				windowRangeEnd = getRangeTime(rangeEndStr);
			}
		}

		if (windowRange == 0) {
			throw new ExceptionSQLStatement("Range is required, and must be a positive intenger.");
		}

		RioDB.rio.getSystemSettings().getLogger().trace("\tRANGE: " + windowRange);

		// get window partition (numeric field from stream). -1 for none.
		int partitionFieldId = SQLParser.getWindowPartitionFieldId(stmt);

		int partitionExpiration = SQLParser.getWindowPartitionExpiration(stmt);

		if (windowRangeEnd >= windowRange) {
			throw new ExceptionSQLStatement(
					"The range expression goes from oldest to most recent. The value after the dash has to be smaller than the first value, like 100-10, or 10m-20s");
		}

		if (windowOfNumbers) {

			Window window;

			if (rangeByTime) {
				if (functionsRequired[SQLAggregateFunctions.getFunctionId("count_distinct")]
						|| functionsRequired[SQLAggregateFunctions.getFunctionId("median")]
						|| functionsRequired[SQLAggregateFunctions.getFunctionId("mode")]
						|| functionsRequired[SQLAggregateFunctions.getFunctionId("slope")]
						|| functionsRequired[SQLAggregateFunctions.getFunctionId("variance")]) {
					window = new WindowOfTimeComplex(windowRange, windowRangeEnd, functionsRequired,
							partitionExpiration);
				} else {
					window = new WindowOfTimeSimple(windowRange, windowRangeEnd, functionsRequired,
							partitionExpiration);
				}
			} else if (windowRange == 1) {
				window = new WindowOfOne(functionsRequired[SQLAggregateFunctions.getFunctionId("previous")],
						partitionExpiration);
			} else {
				window = new WindowOfQuantity(windowRange, windowRangeEnd, functionsRequired, partitionExpiration);
			}

			RioDB.rio.getSystemSettings().getLogger().trace("\twindow object created.");
			WindowWrapper wrapper;
			if (partitionFieldId == -1) {
				wrapper = new WindowWrapper(streamId, windowName, window, fieldId, whereClause, rangeByTime,
						rangeByTimeIsTimestamp, windowSourceExpression);
			} else if (RioDB.rio.getEngine().getStream(streamId).getDef().getStreamField(partitionFieldId)
					.isNumeric()) {

				throw new ExceptionSQLStatement(
						"Partition must be by String field. Partition by numeric field is not currently supported.");
			} else {
				wrapper = new WindowWrapperPartitioned(streamId, windowName, window, fieldId, whereClause, rangeByTime,
						rangeByTimeIsTimestamp, partitionFieldId, windowSourceExpression);
			}
			RioDB.rio.getSystemSettings().getLogger().trace("\twindow wrapper created.");

			RioDB.rio.getEngine().getStream(streamId).addWindowRef(wrapper);
			if (persistStmt) {
				if (actingUser != null && actingUser.equals("SYSTEM")) {
					RioDB.rio.getSystemSettings().getPersistedStatements().loadWindowStmt(windowName, stmt);
					;
				} else {
					RioDB.rio.getSystemSettings().getPersistedStatements().saveNewWindowStmt(windowName, stmt);
				}
			}

		}

		
		
		else {

			Window_String window;

			if (rangeByTime) {
				if (functionsRequired[SQLAggregateFunctions.getFunctionId("count_distinct")]
						|| functionsRequired[SQLAggregateFunctions.getFunctionId("median")]
						|| functionsRequired[SQLAggregateFunctions.getFunctionId("mode")]
						|| functionsRequired[SQLAggregateFunctions.getFunctionId("slope")]
						|| functionsRequired[SQLAggregateFunctions.getFunctionId("variance")]) {
					window = new WindowOfTimeComplex_String(windowRange, windowRangeEnd, functionsRequired,
							partitionExpiration);
				} else {
					window = new WindowOfTimeSimple_String(windowRange, windowRangeEnd, functionsRequired,
							partitionExpiration);
				}
			} else if (windowRange == 1) {
				window = new WindowOfOne_String(functionsRequired[SQLAggregateFunctions.getFunctionId("previous")],
						partitionExpiration);
			} else {
				window = new WindowOfQuantity_String(windowRange, windowRangeEnd, functionsRequired, partitionExpiration);
			}

			RioDB.rio.getSystemSettings().getLogger().trace("\twindow object created.");
			WindowWrapper_String wrapper;
			if (partitionFieldId == -1) {
				wrapper = new WindowWrapper_String(streamId, windowName, window, fieldId, whereClause, rangeByTime,
						rangeByTimeIsTimestamp, windowSourceExpression);
			} else if (RioDB.rio.getEngine().getStream(streamId).getDef().getStreamField(partitionFieldId)
					.isNumeric()) {

				throw new ExceptionSQLStatement(
						"Partition must be by String field. Partition by numeric field is not currently supported.");
			} else {
				wrapper = new WindowWrapperPartitioned_String(streamId, windowName, window, fieldId, whereClause, rangeByTime,
						rangeByTimeIsTimestamp, partitionFieldId, windowSourceExpression);
			}
			RioDB.rio.getSystemSettings().getLogger().trace("\twindow wrapper created.");

			RioDB.rio.getEngine().getStream(streamId).addWindowRef_String(wrapper);
			if (persistStmt) {
				if (actingUser != null && actingUser.equals("SYSTEM")) {
					RioDB.rio.getSystemSettings().getPersistedStatements().loadWindowStmt(windowName, stmt);
					;
				} else {
					RioDB.rio.getSystemSettings().getPersistedStatements().saveNewWindowStmt(windowName, stmt);
				}
			}

		}
		
		return "Created window " + windowName;
	}

	public static final String dropWindow(String stmt) throws ExceptionSQLStatement {

		String newStmt = SQLStreamOperations.formatSQL(stmt);
		// System.out.println(newStmt);

		String words[] = newStmt.split(" ");
		if (words.length >= 3 && words[0] != null && words[0].equals("drop") && words[1] != null
				&& words[1].equals("window")) {

			String windowName = words[2].replace(";", "").trim();

			// locate window (it could be under any stream)
			int streamId = RioDB.rio.getEngine().getStreamIdOfWindow(windowName);
			if (streamId == -1) {
				throw new ExceptionSQLStatement("Window not found.");
			}

			int windowId = RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindowId(windowName);

			if (RioDB.rio.getEngine().hasQueryDependantOnWindow(streamId, windowId)) {
				throw new ExceptionSQLStatement("There are queries using this window. Drop the queries first.");
			}

			RioDB.rio.getEngine().getStream(streamId).dropWindow(windowName);
			// persist removal of window (so it doesn't get recreated after reboot)
			RioDB.rio.getSystemSettings().getPersistedStatements().dropWindowStmt(windowName);

			return "Window " + windowName + " dropped.";

		}
		throw new ExceptionSQLStatement("Statement error. Try 'DROP WINDOW window_name;");

	}

	// describe a window
	public static final String describeWindow(String stmt) throws ExceptionSQLStatement {

		String newStmt = SQLStreamOperations.formatSQL(stmt);
		
		String words[] = newStmt.split(" ");

		if (words.length >= 3 && words[0] != null && words[0].equals("describe") && words[1] != null
				&& words[1].equals("window") && words[2] != null && words[2].length() > 0) {

			String windowName = words[2].replace(";", "").trim();;

			int streamId = RioDB.rio.getEngine().getStreamIdOfWindow(windowName);
			if (streamId == -1) {
				throw new ExceptionSQLStatement("Window not found.");
			}
			return RioDB.rio.getEngine().getStream(streamId).getWindowMgr().describeWindow(windowName);

		}
		throw new ExceptionSQLStatement("Statement error. Try 'DESCRIBE WINDOW stream_name.window_name;");

	}

	

	
	
	private static int getRangeTime(String rangeStr) throws ExceptionSQLStatement {
		int windowRange;
		if (rangeStr.charAt(rangeStr.length() - 1) == 's' || rangeStr.charAt(rangeStr.length() - 1) == 'm'
				|| rangeStr.charAt(rangeStr.length() - 1) == 'h' || rangeStr.charAt(rangeStr.length() - 1) == 'd') {

			String numberInRangeStr = rangeStr.substring(0, rangeStr.length() - 1);
			if (SQLParser.isNumber(numberInRangeStr)) {
				windowRange = Integer.valueOf(numberInRangeStr);
			} else {
				throw new ExceptionSQLStatement("could not find number in '" + rangeStr + "'");
			}

			if (rangeStr.charAt(rangeStr.length() - 1) == 's') {
			} else if (rangeStr.charAt(rangeStr.length() - 1) == 'm') {
				windowRange = windowRange * 60;
			} else if (rangeStr.charAt(rangeStr.length() - 1) == 'h') {
				windowRange = windowRange * 60 * 60;
			} else if (rangeStr.charAt(rangeStr.length() - 1) == 'd') {
				windowRange = windowRange * 60 * 60 * 24;
			}
		} else {
			throw new ExceptionSQLStatement("Missing range-by-time unit  2s, 2m, 2h, 2d ...");
		}
		return windowRange;
	}
	
	
	

}
