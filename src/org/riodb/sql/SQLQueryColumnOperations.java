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
import java.util.TreeSet;

import org.riodb.engine.RioDB;

final public class SQLQueryColumnOperations {

	public static final SQLQueryColumn[] getSelectItems(String selectStr, SQLQueryResources queryResources)
			throws ExceptionSQLStatement {

		if (selectStr != null && selectStr.length() > 0) {

			String selectItemStr[] = SQLParser.splitColumns(selectStr);
			
			SQLQueryColumn[] selectItemArr = new SQLQueryColumn[selectItemStr.length];

			int streamId = queryResources.getDrivingStreamId();

			for (int i = 0; i < selectItemStr.length; i++) {
				selectItemStr[i] = selectItemStr[i].trim();
				
				RioDB.rio.getSystemSettings().getLogger().trace("  QueryColumn: "+selectItemStr[i]);

				String itemStrParts[] = selectItemStr[i].split(" ");
				boolean undefined = false;

				if (itemStrParts.length == 0 || (itemStrParts.length == 1 && itemStrParts[0].length() == 0)) {
					undefined = true;
				} else if (itemStrParts.length == 1) {
					if (SQLParser.isStreamField(streamId, itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemFromMessage(streamId, itemStrParts[0], itemStrParts[0]);
					} else if (SQLParser.isNumber(itemStrParts[0]) || SQLParser.isStringConstant(itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemConstant(itemStrParts[0], itemStrParts[0]);
					} else if (SQLParser.isNumber(itemStrParts[0]) || SQLParser.isStringConstant(itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemConstant(itemStrParts[0], itemStrParts[0]);
					} else if (SQLParser.isAggregateFunction(itemStrParts[0])) {
						if (queryResources.countWindows() == 1) {
							selectItemArr[i] = makeSelectItemFromWindow(itemStrParts[0], itemStrParts[0],
									queryResources);
						} else {
							throw new ExceptionSQLStatement(
									"This item does not indicate which window: " + selectItemStr[i]);
						}

					} else if (itemStrParts[0].contains(".")) {
						// if selected item starts with a period, we have a problem.
						if (itemStrParts[0].charAt(0) == '.') {
							undefined = true;
						} else {
							String alias = itemStrParts[0].substring(0, itemStrParts[0].indexOf("."));
							int windowId = queryResources.getResourceIdByAlias(alias);
							String item = itemStrParts[0].substring(itemStrParts[0].indexOf(".") + 1);

							if (windowId >= 0 && SQLParser.isAggregateFunction(item)) {
								selectItemArr[i] = makeSelectItemFromWindow(itemStrParts[0], itemStrParts[0],
										queryResources);
							} else if (windowId == -1 && SQLParser.isStreamField(streamId, item)) {
								selectItemArr[i] = makeSelectItemFromMessage(streamId, item, item);
							} else {
								undefined = true;
							}
						}
					} else {
						undefined = true;
					}
				}

				else if (itemStrParts.length == 2) {

					if (SQLParser.isStreamField(streamId, itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemFromMessage(streamId, itemStrParts[0], itemStrParts[1]);
					} else if (SQLParser.isNumber(itemStrParts[0]) || SQLParser.isStringConstant(itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemConstant(itemStrParts[0], itemStrParts[1]);
					} else if (SQLParser.isAggregateFunction(itemStrParts[0])) {
						if (queryResources.countWindows() == 1) {
							selectItemArr[i] = makeSelectItemFromWindow(itemStrParts[0], itemStrParts[1],
									queryResources);
						} else {
							throw new ExceptionSQLStatement(
									"This item does not indicate which window: " + selectItemStr[i]);
						}

					} else if (itemStrParts[0].contains(".")) {
						// if selected item starts with a period, we have a problem.
						if (itemStrParts[0].charAt(0) == '.') {
							undefined = true;
						} else {
							String alias = itemStrParts[0].substring(0, itemStrParts[0].indexOf("."));
							int windowId = queryResources.getResourceIdByAlias(alias);
							String item = itemStrParts[0].substring(itemStrParts[0].indexOf(".") + 1);

							if (windowId >= 0 && SQLParser.isAggregateFunction(item)) {
								selectItemArr[i] = makeSelectItemFromWindow(itemStrParts[0], itemStrParts[1],
										queryResources);
							} else if (windowId == -1 && SQLParser.isStreamField(streamId, item)) {
								selectItemArr[i] = makeSelectItemFromMessage(streamId, item, itemStrParts[1]);
							} else {
								undefined = true;
							}
						}
					} else {
						undefined = true;
					}
				}

				else if (itemStrParts.length == 3 && itemStrParts[1].equals("as")) {

					if (SQLParser.isStreamField(streamId, itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemFromMessage(streamId, itemStrParts[0], itemStrParts[2]);
					} else if (SQLParser.isNumber(itemStrParts[0]) || SQLParser.isStringConstant(itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemConstant(itemStrParts[0], itemStrParts[2]);
					} else if (SQLParser.isAggregateFunction(itemStrParts[0])) {
						if (queryResources.countWindows() == 1) {
							selectItemArr[i] = makeSelectItemFromWindow(itemStrParts[0], itemStrParts[2],
									queryResources);
						} else {
							throw new ExceptionSQLStatement(
									"This item does not indicate which window: " + selectItemStr[i]);
						}

					} else if (itemStrParts[0].contains(".")) {
						// if selected item starts with a period, we have a problem.
						if (itemStrParts[0].charAt(0) == '.') {
							undefined = true;
						} else {
							String alias = itemStrParts[0].substring(0, itemStrParts[0].indexOf("."));
							int windowId = queryResources.getResourceIdByAlias(alias);
							String item = itemStrParts[0].substring(itemStrParts[0].indexOf(".") + 1);

							if (windowId >= 0 && SQLParser.isAggregateFunction(item)) {
								selectItemArr[i] = makeSelectItemFromWindow(itemStrParts[0], itemStrParts[2],
										queryResources);
							} else if (windowId == -1 && SQLParser.isStreamField(streamId, item)) {
								selectItemArr[i] = makeSelectItemFromMessage(streamId, item, itemStrParts[2]);
							} else {
								undefined = true;
							}
						}
					} else {
						undefined = true;
					}
				}

				else if (itemStrParts.length >= 3) {
					if (itemStrParts[itemStrParts.length - 2].equals("as")) {
						String expression = selectItemStr[i].substring(0, selectItemStr[i].indexOf(" as "));
						String heading = itemStrParts[itemStrParts.length - 1]; // "Expression" + i;
						selectItemArr[i] = makeSelectItemExpression(expression, heading, queryResources);
					} else {
						String heading = "Column" + i;
						selectItemArr[i] = makeSelectItemExpression(selectItemStr[i], heading, queryResources);
					}
				} else {
					undefined = true;
				}

				if (undefined) {
					throw new ExceptionSQLStatement("Not sure what to select here: " + selectItemStr[i]);
				}
			}
			return selectItemArr;
		} else {
			throw new ExceptionSQLStatement("nothing selected.   SELECT <?> FROM ...");
		}
	}

	private static final SQLQueryColumn makeSelectItemFromMessage(int streamId, String selectItemStr, String heading)
			throws ExceptionSQLStatement {

		int fieldId = RioDB.rio.getEngine().getStream(streamId).getDef().getFieldId(selectItemStr);
		if (RioDB.rio.getEngine().getStream(streamId).getDef().isNumeric(fieldId)) {
			int floatFieldIndex = RioDB.rio.getEngine().getStream(streamId).getDef().getNumericFieldIndex(fieldId);
			return new SQLQueryColumnNumberFromMessage(floatFieldIndex, heading);
		} else {
			int stringFieldIndex = RioDB.rio.getEngine().getStream(streamId).getDef().getStringFieldIndex(fieldId);
			return new SQLQueryColumnStringFromMessage(stringFieldIndex, heading);
		}
	}

	private static final SQLQueryColumn makeSelectItemConstant(String selectItemStr, String heading)
			throws ExceptionSQLStatement {
		return new SQLQueryColumnConstant(selectItemStr, heading);
	}

	private static final SQLQueryColumn makeSelectItemFromWindow(String selectItemStr, String heading,
			SQLQueryResources queryResources) throws ExceptionSQLStatement {

		
		RioDB.rio.getSystemSettings().getLogger().trace("  selectItemStr: "+selectItemStr);
		
		if (selectItemStr.contains(".")) {

			int streamId = queryResources.getDrivingStreamId();

			String alias = selectItemStr.substring(0, selectItemStr.indexOf("."));
			int resourceId = queryResources.getResourceIdByAlias(alias);
			if (resourceId == -1) {
				throw new ExceptionSQLStatement("alias not identified: " + selectItemStr);
			}
			String function = selectItemStr.substring(selectItemStr.indexOf(".") + 1);
			int functionId = SQLAggregateFunctions.getFunctionId(function);
			if (functionId == -1) {
				throw new ExceptionSQLStatement("not a valid function: " + selectItemStr);
			}

			int windowStreamId = queryResources.getResourceById(resourceId).getStreamId();
			int windowId = queryResources.getResourceById(resourceId).getWindowId();

			// window is a foreign window
			if (windowStreamId != streamId) {
				// in the case of a foreign window, windowId is the ID in the foreign stream.

				// check if the foreign window has the requested function:
				if (!RioDB.rio.getEngine().getStream(windowStreamId).getWindowMgr().windowRequiresFunction(windowId,
						functionId)) {
					throw new ExceptionSQLStatement("window does not provide this function: " + selectItemStr);
				}
				
				RioDB.rio.getSystemSettings().getLogger().trace("    Column from foreign window: "+windowStreamId+"."+windowId);

				return new SQLQueryColumnFromForeignWindow(windowStreamId, windowId, functionId, heading);

			}
			// else, it's a window in the driving stream.
			else {

				// check if the foreign window has the requested function:
				if (!RioDB.rio.getEngine().getStream(streamId).getWindowMgr().windowRequiresFunction(windowId,
						functionId)) {
					throw new ExceptionSQLStatement("window does not provide this function: " + selectItemStr);
				}
				
				RioDB.rio.getSystemSettings().getLogger().trace("    Column from window: "+windowId);

				return new SQLQueryColumnFromWindow(windowId, functionId, heading);
			}

		} 
		// else if no stream/window provided. Only a function. But the query has only one window declared, so
		// we assume the user intended that window
		else if (queryResources.countWindows() == 1 && SQLAggregateFunctions.isFunction(selectItemStr)) {
			
			int functionId = SQLAggregateFunctions.getFunctionId(selectItemStr);

			int windowId = queryResources.getResourceById(0).getWindowId();
			
			// check if the only window has the requested function:
			int streamId = queryResources.getDrivingStreamId();
			if (!RioDB.rio.getEngine().getStream(streamId).getWindowMgr().windowRequiresFunction(windowId,
					functionId)) {
				throw new ExceptionSQLStatement("window does not provide this function: " + selectItemStr);
			}
			
			RioDB.rio.getSystemSettings().getLogger().trace("    Column from only window: "+windowId);
			
			return new SQLQueryColumnFromWindow(windowId, functionId, heading);

		}

		throw new ExceptionSQLStatement("Unable to determine this selected item: " + selectItemStr);
	}

	private static final SQLQueryColumn makeSelectItemExpression(String expression, String heading,
			SQLQueryResources queryResources) throws ExceptionSQLStatement {
		// System.out.println("Expression: "+ expression);

		int streamId = queryResources.getDrivingStreamId();

		
		// ArrayLists of StringLike and StringIn objects if needed
		ArrayList<SQLStringLIKE> likeList = new ArrayList<SQLStringLIKE>();
		ArrayList<SQLStringIN> inList = new ArrayList<SQLStringIN>();
		// get the list of all required Windows for this query column
		TreeSet<Integer> requiredWindows = new TreeSet<Integer>();

		String javaExpression = JavaGenerator.convertSqlToJava(expression, queryResources, streamId, likeList,
						inList, requiredWindows);
		
		RioDB.rio.getSystemSettings().getLogger().trace("\tcompiled: " + javaExpression);

		SQLStringLIKE[] likeArr = new SQLStringLIKE[likeList.size()];
		likeArr = likeList.toArray(likeArr);

		SQLStringIN[] inArr = new SQLStringIN[inList.size()];
		inArr = inList.toArray(inArr);
		
		RioDB.rio.getSystemSettings().getLogger().debug("SELECT expression: " + javaExpression);

		return new SQLQueryColumnFromExpression(javaExpression, heading, likeArr, inArr, requiredWindows);
	}

}
