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

import org.riodb.engine.RioDB;
import org.riodb.windows.Window;
import org.riodb.windows.WindowOfOne;
import org.riodb.windows.WindowOfQuantity;
import org.riodb.windows.WindowOfTimeComplex;
import org.riodb.windows.WindowOfTimeSimple;
import org.riodb.windows.WindowWrapper;
import org.riodb.windows.WindowWrapperPartitioned;

public final class SQLWindowOperations {
	public static final boolean createWindow(String stmt, boolean persistStmt, String actingUser)
			throws ExceptionSQLStatement {

		boolean success = true;

		// get window name
		String windowName = SQLParser.getWindowStr(stmt);
		if (RioDB.rio.getEngine().getStreamIdOfWindow(windowName) >= 0)
			throw new ExceptionSQLStatement("A window named '" + windowName + "' already exists.");

		// get window functions
		String functionStr = SQLParser.getWindowRunningFunctions(stmt);
		boolean functionsRequired[] = SQLFunctionMap.getFunctionsRequired(functionStr);

		// get window Stream
		int streamId = SQLParser.getWindowStreamId(stmt);

		// get window field (numeric field from stream)
		int fieldId = SQLParser.getWindowFieldId(stmt);

		// get window condition (WHERE...)
		SQLWindowCondition whereClause = null;
		String whereStr = SQLParser.getWindowWhereStr(stmt);
		if (whereStr != null) {
			whereClause = getWhereClause(whereStr, streamId);
		}

		// get window RANGE information
		// default is range by quantity.
		boolean rangeByTime = false;
		boolean rangeByTimeIsTimestamp = false; // NOT CLOCK-based.
		int windowRange = 0;

		String rangeStr = SQLParser.getWindowRangeStr(stmt);

		// System.out.println("rangeStr: "+rangeStr);

		// if rangeStr is just a number, then it's a range of quantity (integer)
		if (SQLParser.isNumber(rangeStr)) {
			windowRange = Integer.valueOf(rangeStr);
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
		}

		if (windowRange == 0) {
			throw new ExceptionSQLStatement("Range is required, and must be a positive intenger.");
		}

		// get window partition (numeric field from stream). -1 for none.
		int partitionFieldId = SQLParser.getWindowPartitionFieldId(stmt);

		int partitionExpiration = SQLParser.getWindowPartitionExpiration(stmt);

		Window window;

		if (rangeByTime) {
			if (functionsRequired[SQLFunctionMap.getFunctionId("count_distinct")]
					|| functionsRequired[SQLFunctionMap.getFunctionId("median")]
					|| functionsRequired[SQLFunctionMap.getFunctionId("mode")]
					|| functionsRequired[SQLFunctionMap.getFunctionId("slope")]
					|| functionsRequired[SQLFunctionMap.getFunctionId("variance")]) {
				window = new WindowOfTimeComplex(windowRange, functionsRequired, partitionExpiration);
			} else {
				window = new WindowOfTimeSimple(windowRange, functionsRequired, partitionExpiration);
			}
		} else if (windowRange == 1) {
			window = new WindowOfOne(functionsRequired[SQLFunctionMap.getFunctionId("previous")], partitionExpiration);
		} else {
			window = new WindowOfQuantity(windowRange, functionsRequired, partitionExpiration);
		}

		WindowWrapper wrapper;
		if (partitionFieldId == -1) {
			wrapper = new WindowWrapper(streamId, windowName, window, fieldId, whereClause, rangeByTime,
					rangeByTimeIsTimestamp);
		} else if (RioDB.rio.getEngine().getStream(streamId).getDef().getStreamField(partitionFieldId).isNumeric()) {

			throw new ExceptionSQLStatement(
					"Partition must be by String field. Partition by numeric field is not currently supported.");
		} else {
			wrapper = new WindowWrapperPartitioned(streamId, windowName, window, fieldId, whereClause, rangeByTime,
					rangeByTimeIsTimestamp, partitionFieldId);
		}

		RioDB.rio.getEngine().getStream(streamId).addWindowRef(wrapper);
		if (persistStmt) {
			if (actingUser != null && actingUser.equals("SYSTEM")) {
				RioDB.rio.getSystemSettings().getPersistedStatements().loadWindowStmt(windowName, stmt);
				;
			} else {
				RioDB.rio.getSystemSettings().getPersistedStatements().addNewWindowStmt(windowName, stmt);
			}
		}

		return success;
	}

	public static final void dropWindow(String stmt) throws ExceptionSQLStatement {

		String newStmt = SQLStreamOperations.formatSQL(stmt);
		// System.out.println(newStmt);

		String words[] = newStmt.split(" ");
		if (words.length >= 3 && words[0] != null && words[0].equals("drop") && words[1] != null
				&& words[1].equals("window")) {

			String windowName = words[2];

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
			
			return;

		}
		throw new ExceptionSQLStatement("Statement error. Try 'DROP WINDOW window_name;");

	}

	// describe a window
	public static final String describeWindow(String stmt) throws ExceptionSQLStatement {

		String newStmt = SQLStreamOperations.formatSQL(stmt);
		// System.out.println(newStmt);

		String words[] = newStmt.split(" ");

		if (words.length >= 3 && words[0] != null && words[0].equals("describe") && words[1] != null
				&& words[1].equals("window") && words[2] != null && words[2].length() > 0) {

			String windowName = words[2];

			int streamId = RioDB.rio.getEngine().getStreamIdOfWindow(windowName);
			if (streamId == -1) {
				throw new ExceptionSQLStatement("Window not found.");
			}
			return RioDB.rio.getEngine().getStream(streamId).getWindowMgr().describeWindow(windowName);

		}
		throw new ExceptionSQLStatement("Statement error. Try 'DESCRIBE WINDOW stream_name.window_name;");

	}

	private static final SQLWindowCondition getWhereClause(String whereStr, int streamId) throws ExceptionSQLStatement {

		if (whereStr != null) {
			if (whereStr.equals("-")) {
				return null;
			}
			// MULTIPLE WHERE CONDITIONS. COMPLEX.
			else if (whereStr.toLowerCase().contains(" and ") || whereStr.toLowerCase().contains(" and(")
					|| whereStr.toLowerCase().contains(" or ") || whereStr.toLowerCase().contains(" or(")) {
				return makeComplexWhereClause(whereStr, streamId);
			}
			// SINGLE WHERE CONDITION. SIMPLE.
			else {
				return makeSimpleWhereClause(whereStr, streamId);
			}
		} else {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(20, whereStr));
		}
	}

	private static final SQLWindowCondition makeSimpleWhereClause(String whereStr, int streamId)
			throws ExceptionSQLStatement {

		if (whereStr == null) {
			return null;
		}

		// tuning. faster comparison with null checks
		if (whereStr.contains(" != #qq~#")) {
			whereStr = whereStr.replace(" = #qq~#", " is not null");
		} else if (whereStr.contains(" = #qq~#")) {
			whereStr = whereStr.replace(" = #qq~#", " is null");
		} else if (whereStr.contains(" not like #qq~#")) {
			whereStr = whereStr.replace(" not like #qq~#", " is not null");
		} else if (whereStr.contains(" like #qq~#")) {
			whereStr = whereStr.replace(" like #qq~#", " is null");
		}

		String operator = "";
		if (whereStr.contains("!=")) {
			operator = "!=";
		} else if (whereStr.contains(">=")) {
			operator = ">=";
		} else if (whereStr.contains("<=")) {
			operator = "<=";
		} else if (whereStr.contains(">")) {
			operator = ">";
		} else if (whereStr.contains("<")) {
			operator = "<";
		} else if (whereStr.contains("!=")) {
			operator = "!=";
		} else if (whereStr.contains("=")) {
			operator = "=";
		} else if (whereStr.toLowerCase().contains(" not in ")) {
			operator = " not in ";
		} else if (whereStr.toLowerCase().contains(" not in(")) {
			operator = " not in ";
			whereStr = whereStr.replace(" not in(", " not in (");
		} else if (whereStr.toLowerCase().contains(" in ")) {
			operator = " in ";
		} else if (whereStr.toLowerCase().contains(" in(")) {
			operator = " in ";
			whereStr = whereStr.replace(" in(", " in (");
		} else if (whereStr.toLowerCase().contains(" not like ")) {
			operator = " not like ";
		} else if (whereStr.toLowerCase().contains(" like ")) {
			operator = " like ";
		} else if (whereStr.toLowerCase().contains(" is null")) {
			operator = " is null";
		} else if (whereStr.toLowerCase().contains(" is not null")) {
			operator = " is not null";
		} else {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(21, whereStr));
		}

		if (operator.length() > 0) {

			String column = whereStr.substring(0, whereStr.indexOf(operator));
			column = column.trim();

			String pattern = "";
			if (operator.equals(" is null") || operator.equals(" is not null")) {
				pattern = "''";
			} else {
				pattern = whereStr.substring(whereStr.indexOf(operator) + operator.length());
				pattern.trim();
			}

			int columnId = -1;
			int columnId2 = -1;

			if (column != null && column.length() > 0) {
				columnId = RioDB.rio.getEngine().getStream(streamId).getDef().getFieldId(column);
			}
			if (pattern != null && pattern.length() > 0) {
				columnId2 = RioDB.rio.getEngine().getStream(streamId).getDef().getFieldId(pattern);
			}

			if (columnId < 0 || columnId2 >= 0) {
				return makeComplexWhereClause(whereStr, streamId);
			}
			//
			else {
				if (RioDB.rio.getEngine().getStream(streamId).getDef().isNumeric(columnId)) {
					if (!operator.equals(" is null") && !operator.equals(" is not null")) {
						try {
							float patternNum = Float.valueOf(pattern);
							SQLWindowConditionNumber clause = new SQLWindowConditionNumber(streamId, columnId, operator,
									patternNum, whereStr);
							return clause;
						} catch (NumberFormatException exception) {
							throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(25, whereStr));
						}
					} else {
						SQLWindowConditionNumber clause = new SQLWindowConditionNumber(streamId, columnId, operator,
								Float.NaN, whereStr);
						return clause;
					}

				} else {
					pattern = pattern.trim();
					pattern = SQLParser.textDecode(pattern);
					if ((pattern != null && pattern.length() >= 2)) {
						if (pattern.charAt(0) != '\'' || pattern.charAt(pattern.length() - 1) != '\'') {
							throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(26, whereStr));
						}
						return new SQLWindowConditionString(streamId, columnId, operator, pattern, whereStr);
					} else if (operator.equals(" is null") || operator.equals(" is not null")) {
						return new SQLWindowConditionString(streamId, columnId, operator, pattern, whereStr);
					} else {
						throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(27, whereStr));
					}

				}
			}
		} else {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(29, whereStr));
		}
	}

	private static final SQLWindowCondition makeComplexWhereClause(String whereStr, int streamId)
			throws ExceptionSQLStatement {

		String expression = whereStr;
		// String streamName = RioDB.rio.getStreamMgr().getStream(streamId).getName();

		// PREPARE JAVA EXPRESSION
		expression = expression.replace(" or ", " || ");
		expression = expression.replace(" and ", " && ");
		expression = expression.replace("(", " ( ");
		expression = expression.replace(")", " ) ");
		expression = expression.replace("is null", "is_null");
		expression = expression.replace("is not null", "is_not_null");

		while (expression.contains("  ")) {
			expression = expression.replace("  ", " ");
		}
		expression = expression.trim();

		// ArrayLists of StringLike and StringIn objects if needed
		ArrayList<SQLStringLIKE> likeList = new ArrayList<SQLStringLIKE>();
		ArrayList<SQLStringIN> inList = new ArrayList<SQLStringIN>();

		/// REPLACE fields with StreamEvent variables...
		String words[] = expression.split(" ");
		for (int i = 0; i < words.length; i++) {
			int fieldId = RioDB.rio.getEngine().getStream(streamId).getDef().getFieldId(words[i]);
			if (words[i].charAt(0) == '\''
					&& (words[i].charAt(words[i].length() - 1) == '\'' || words[i].endsWith("' )"))) {
				words[i] = words[i].replace("'", "\"").replace("\" )", "\")");
			} else {
				if (fieldId >= 0) {
					if (RioDB.rio.getEngine().getStream(streamId).getDef().isNumeric(fieldId)) {
						// the field is numeric
						int eventNumberFieldIndex = RioDB.rio.getEngine().getStream(streamId).getDef()
								.getNumericFieldIndex(fieldId);
						words[i] = "event.getDouble(" + String.valueOf(eventNumberFieldIndex) + ")";
						if (words[i + 1].equals("is_null")) {
							words[i + 1] = "!= Double.NaN";
						} else if (words[i + 1].equals("is_not_null")) {
							words[i + 1] = "= Double.NaN";
						}

					} else {
						// the field is string
						int eventStringIndex = RioDB.rio.getEngine().getStream(streamId).getDef()
								.getStringFieldIndex(fieldId);
						words[i] = "event.getString(" + String.valueOf(eventStringIndex) + ")";

						if (i < words.length - 3) {
							if (words[i + 1].equals("=")) {
								words[i + 1] = ".equals(";
								words[i + 2] = words[i + 2] + " )";
							} else if (words[i + 1].equals("!=")) {
								words[i] = "!" + words[i];
								words[i + 1] = ".equals(";
								words[i + 2] = words[i + 2] + " )";
							} else if (words[i + 1].equals("not")) {
								words[i] = "!" + words[i];
								words[i + 1] = "";
								i++;
							} else if (words[i + 1].equals("like")) {

							} else if (words[i + 1].equals("is_null")) {
								words[i + 1] = ".equals(\"\")";

							} else if (words[i + 1].equals("is_not_null")) {
								words[i] = "!" + words[i];
								words[i + 1] = ".equals(\"\")";
							}

						}

					}
				} else if (!SQLParser.isNumber(words[i])) {
					if (!SQLParser.isReservedWord(words[i]) && words[i].charAt(0) != '\'') {
						throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(35, words[i] + " at:\n\t " + whereStr));
					}

					if (words[i].equals("like")) {

						if (i < words.length - 2 && i > 0) {
							String val = words[i + 1].replace("'", "");
							val = SQLParser.textDecode(val);
							String likeCounter = String.valueOf(likeList.size());

							String source = words[i - 1];
							// was NOT
							if (source.equals("") && i > 1) {
								source = words[i - 2];
								if (source.charAt(0) == '!') {
									words[i - 2] = "!";
									source = source.substring(1);
								}
							} else {
								words[i - 1] = "";
							}
							// System.out.println("likeList["+likeCounter+"] pattern:"+ val);
							words[i] = "likeList[" + likeCounter + "].match(" + source + ")";
							SQLStringLIKE sl = new SQLStringLIKE(val);
							likeList.add(sl);
							words[i + 1] = "";
							i++;

						}
					} else if (words[i].equals("in")) {

						if (i < words.length - 2 && i > 0) {

							String val = "";
							int j = i + 2;
							while (words[j].indexOf(')') == -1) {
								val = val + words[j];
								words[j] = "";
								j++;
							}
							words[j] = "";

							val = "(" + SQLParser.textDecode(val) + ")";

							// System.out.println(val);

							String inCounter = String.valueOf(inList.size());
							String source = words[i - 1];

							// was NOT
							if (source.equals("") && i > 1) {
								source = words[i - 2];
								if (source.charAt(0) == '!') {
									words[i - 2] = "!";
									source = source.substring(1);
								}
							} else {
								words[i - 1] = "";
							}

							words[i] = "inList[" + inCounter + "].match(" + source + ")";
							// System.out.println(words[i]);
							SQLStringIN sl = new SQLStringIN(val);
							inList.add(sl);
							words[i + 1] = "";

							i = j;

						}
					} else if (SQLParser.isMathFunction(words[i])) {
						words[i] = "Math." + SQLParser.mathFunctionRegCase(words[i]);
					}
				}
			}
		}

		expression = "";
		for (String word : words) {
			expression = expression + word + " ";
		}
		expression = expression.trim();
		expression = expression.replace("=", "==");
		expression = expression.replace("!==", "!=");
		expression = expression.replace(">==", ">=");
		expression = expression.replace("<==", "<=");

		expression = expression.replace("( \"", "(\"");
		expression = expression.replace(") .equals(", ").equals(");
		while (expression.contains("  ")) {
			expression = expression.replace("  ", " ");
		}

		SQLStringLIKE[] likeArr = new SQLStringLIKE[likeList.size()];
		likeArr = likeList.toArray(likeArr);

		SQLStringIN[] inArr = new SQLStringIN[inList.size()];
		inArr = inList.toArray(inArr);

		expression = SQLParser.textDecode(expression).replace("''", "'");

		return new SQLWindowConditionExpression(expression, streamId, likeArr, inArr, whereStr);
	}

}
