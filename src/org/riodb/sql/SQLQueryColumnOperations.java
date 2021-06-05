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

final public class SQLQueryColumnOperations {

	public static final SQLQueryColumn[] getSelectItems(String selectStr, SQLQueryResources queryResources)
			throws ExceptionSQLStatement {

		if (selectStr != null && selectStr.length() > 0) {

			String selectItemStr[] = selectStr.split(",");
			SQLQueryColumn[] selectItemArr = new SQLQueryColumn[selectItemStr.length];

			int streamId = queryResources.getDrivingStreamId();

			for (int i = 0; i < selectItemStr.length; i++) {
				selectItemStr[i] = selectItemStr[i].trim();


				String itemStrParts[] = selectItemStr[i].split(" ");
				boolean undefined = false;

				if (itemStrParts.length == 0 || (itemStrParts.length == 1 && itemStrParts[0].length() == 0)) {
					undefined = true;
				} else if (itemStrParts.length == 1) {
					if (SQLParser.isStreamField(streamId, itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemFromEvent(streamId, itemStrParts[0], itemStrParts[0]);
					} else if (SQLParser.isNumber(itemStrParts[0]) || SQLParser.isStringConstant(itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemConstant(itemStrParts[0], itemStrParts[0]);
					} else if (SQLParser.isNumber(itemStrParts[0]) || SQLParser.isStringConstant(itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemConstant(itemStrParts[0], itemStrParts[0]);
					} else if (SQLParser.isSQLFunction(itemStrParts[0])) {
						if (queryResources.countWindows() == 1) {
							selectItemArr[i] = makeSelectItemFromWindow(itemStrParts[0], itemStrParts[0],
									queryResources);
						} else {
							throw new ExceptionSQLStatement("This item does not indicate which window: " + selectItemStr[i]);
						}

					} else if (itemStrParts[0].contains(".")) {
						// if selected item starts with a period, we have a problem.
						if (itemStrParts[0].charAt(0) == '.') {
							undefined = true;
						} else {
							String alias = itemStrParts[0].substring(0, itemStrParts[0].indexOf("."));
							int windowId = queryResources.getResourceIdByAlias(alias);
							String item = itemStrParts[0].substring(itemStrParts[0].indexOf(".") + 1);
							
							if(windowId >= 0  &&
									SQLParser.isSQLFunction(item)) {
								selectItemArr[i] = makeSelectItemFromWindow(itemStrParts[0],itemStrParts[0], queryResources);
							}
							else if (windowId == -1 &&
									SQLParser.isStreamField(streamId, item)	) {
								selectItemArr[i] = makeSelectItemFromEvent(streamId,item,item);
							}
							else {
								undefined = true;
							}
						}
					} else {
						undefined = true;
					}
				}

				else if (itemStrParts.length == 2) {
					if (SQLParser.isStreamField(streamId, itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemFromEvent(streamId, itemStrParts[0], itemStrParts[1]);
					} else if (SQLParser.isNumber(itemStrParts[0]) || SQLParser.isStringConstant(itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemConstant(itemStrParts[0], itemStrParts[1]);
					} else {
						undefined = true;
					}
				}

				else if (itemStrParts.length == 3 && itemStrParts[1].equals("as")) {
					if (SQLParser.isStreamField(streamId, itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemFromEvent(streamId, itemStrParts[0], itemStrParts[2]);
					} else if (SQLParser.isNumber(itemStrParts[0]) || SQLParser.isStringConstant(itemStrParts[0])) {
						selectItemArr[i] = makeSelectItemConstant(itemStrParts[0], itemStrParts[2]);
					} else {
						undefined = true;
					}
				}

				else if (itemStrParts.length >= 3) {
					if (itemStrParts[itemStrParts.length - 2].equals("as")) {
						String expression = selectItemStr[i].substring(0, selectItemStr[i].indexOf(" as "));
						String heading = "Expression" + i;
						selectItemArr[i] = makeSelectItemExpression(expression, heading, queryResources);
					} else {

						String heading = "Expression" + i;
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

	private static final SQLQueryColumn makeSelectItemFromEvent(int streamId, String selectItemStr, String heading)
			throws ExceptionSQLStatement {
		int fieldId = RioDB.rio.getEngine().getStream(streamId).getDef().getFieldId(selectItemStr);
		if (RioDB.rio.getEngine().getStream(streamId).getDef().isNumeric(fieldId)) {
			int floatFieldIndex = RioDB.rio.getEngine().getStream(streamId).getDef().getNumericFieldIndex(fieldId);
			return new SQLQueryColumnDoubleFromEvent(floatFieldIndex, heading);
		} else {
			int stringFieldIndex = RioDB.rio.getEngine().getStream(streamId).getDef().getStringFieldIndex(fieldId);
			return new SQLQueryColumnStringFromEvent(stringFieldIndex, heading);
		}
	}

	private static final SQLQueryColumn makeSelectItemConstant(String selectItemStr, String heading)
			throws ExceptionSQLStatement {
		return new SQLQueryColumnConstant(selectItemStr, heading);
	}

	private static final SQLQueryColumn makeSelectItemFromWindow(String selectItemStr, String heading,
			SQLQueryResources queryResources) throws ExceptionSQLStatement {

		if(selectItemStr.contains(".")) {
			
			int streamId = queryResources.getDrivingStreamId();
			
			String alias = selectItemStr.substring(0, selectItemStr.indexOf("."));
			int resourceId = queryResources.getResourceIdByAlias(alias);
			if(resourceId == -1) {
				throw new ExceptionSQLStatement("alias not identified: "+ selectItemStr);
			}
			String function = selectItemStr.substring(selectItemStr.indexOf(".") + 1);
			int functionId = SQLFunctionMap.getFunctionId(function);
			if(functionId == -1) {
				throw new ExceptionSQLStatement("not a valid function: "+ selectItemStr);
			}
			
			
			int windowStreamId = queryResources.getResourceById(resourceId).getStreamId();
			int windowId       = queryResources.getResourceById(resourceId).getWindowId();
			
			// window is a foreign window
			if(windowStreamId != streamId) {
				// in the case of a foreign window, windowId is the ID in the foreign stream.
				
				// check if the foreign window has the requested function:
				if(!RioDB.rio.getEngine().getStream(windowStreamId).getWindowMgr().windowRequiresFunction(windowId, functionId)) {
					throw new ExceptionSQLStatement("window does not provide this function: "+ selectItemStr);
				}
				
				return new SQLQueryColumnFromForeignWindow(windowStreamId, windowId, functionId, heading);
				
			}
			// else, it's a window in the driving stream.
			else {
				
				// check if the foreign window has the requested function:
				if(!RioDB.rio.getEngine().getStream(streamId).getWindowMgr().windowRequiresFunction(windowId, functionId)) {
					throw new ExceptionSQLStatement("window does not provide this function: "+ selectItemStr);
				}
				
				return new SQLQueryColumnFromWindow(windowId, functionId, heading);
			}
			
		}
		else if(queryResources.countWindows() == 1 &&
				SQLFunctionMap.isFunction(selectItemStr)) {
			
			int functionId = SQLFunctionMap.getFunctionId(selectItemStr);
			
			int windowId =queryResources.getResourceById(0).getWindowId();
			return new SQLQueryColumnFromWindow(windowId, functionId, heading);
			
		}

		throw new ExceptionSQLStatement("Unable to determine this selected item: "+ selectItemStr);
	}

	private static final SQLQueryColumn makeSelectItemExpression(String expression, String heading,
			SQLQueryResources queryResources) throws ExceptionSQLStatement {
		
		//System.out.println("Expression: "+ expression);
		
		int streamId = queryResources.getDrivingStreamId();
		
		String words[] = expression.split(" ");
		for(int i = 0; i < words.length; i++) {
			if(words[i].contains(".")) {
				String resourceAlias = words[i].substring(0,words[i].indexOf("."));
				String field = words[i].substring(words[i].indexOf(".")+1);
				
				if(resourceAlias != null && resourceAlias.equals(queryResources.getDrivingStreamAlias())) {
					// it's the stream
					if(SQLParser.isStreamField(streamId, field)) {
						words[i] = "event."+field;
					}
					else {
						throw new ExceptionSQLStatement("The stream doesn't have field: "+ field);
					}
				}
				else if(resourceAlias != null && queryResources.getResourceIdByAlias(resourceAlias) != -1) {
					// it's a WINDOW!!!
					int resourceId = queryResources.getResourceIdByAlias(resourceAlias);
					int windowStreamId = queryResources.getResourceById(resourceId).getStreamId();
					int windowId = queryResources.getResourceById(resourceId).getWindowId();
					int functionId = SQLFunctionMap.getFunctionId(field);
					if(RioDB.rio.getEngine().getStream(windowStreamId).getWindowMgr().getWindow(windowId).windowRequiresFunction(functionId)) {
						if(windowStreamId == streamId) {
							// window of the same stream. 
							words[i] = "windowSummaries["+windowId+"]."+ SQLFunctionMap.getFunctionCall(functionId);
						}
						else {
							// foreign window
							words[i] = "RioDB.rio.getStreamMgr().getStream("+ windowStreamId +").getWindowMgr().getWindow("+ windowId +")."+ SQLFunctionMap.getFunctionCall(functionId);
						}
					}
					else {
						throw new ExceptionSQLStatement("The window doesn't provide the function: "+ words[i]);
					}
				}
				else {
					throw new ExceptionSQLStatement("The selected item couldn't be identified: "+ words[i]);
				}
				
				
				
			}
			else if(SQLParser.isSQLFunction(words[i])  &&
					queryResources.countWindows() == 1) {
				int functionId = SQLFunctionMap.getFunctionId(words[i]);
				words[i] = "windowSummaries[0]."+ SQLFunctionMap.getFunctionCall(functionId);
			}
			else if(SQLParser.isStreamField(streamId, words[i]) ) {
				int fieldId = RioDB.rio.getEngine().getStream(streamId).getDef().getFieldId(words[i]);
				boolean isNumeric = RioDB.rio.getEngine().getStream(streamId).getDef().isNumeric(fieldId);
				if(isNumeric) {
					int floatFieldIndex = RioDB.rio.getEngine().getStream(streamId).getDef().getNumericFieldIndex(fieldId);
					words[i] = "event.getDouble("+ floatFieldIndex+")";
				}
				else {
					int stringFieldIndex = RioDB.rio.getEngine().getStream(streamId).getDef().getStringFieldIndex(fieldId);
					words[i] = "event.getString("+ stringFieldIndex+")";
				}
			}
			else if(SQLParser.isReservedWord(words[i]) || SQLParser.isNumber(words[i])) {
				;
			}
			else {
				throw new ExceptionSQLStatement("Unable to determine this selected item: "+ words[i]);
			}
		}
		String newExpression = "";
		for(String s : words) {
			newExpression = newExpression + s + " ";
		}
		RioDB.rio.getSystemSettings().getLogger().debug("Statement: "+ newExpression);
		
		return new SQLQueryColumnFromExpression(newExpression, heading);
	}

}
