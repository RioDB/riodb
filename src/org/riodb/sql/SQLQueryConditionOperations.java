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

final public class SQLQueryConditionOperations {

	public static SQLQueryCondition getQueryConditions(String whenStr, SQLQueryResources queryResources)
			throws ExceptionSQLStatement {
		/*
		 * int drivingStreamId = queryResources.getDrivingStreamId();
		 * 
		 * if (whenStr == null || whenStr.contentEquals("-")) { return null; } if
		 * (whenStr.contains(" and ") || whenStr.contains(" or ")) { return
		 * getQueryConditionExpression(whenStr, queryResources); } else {
		 * if(SQLParser.containsOperator(whenStr)) { String operator =
		 * SQLParser.getOperator(whenStr); String part1 = whenStr.substring(0,
		 * whenStr.indexOf(operator)); String part2 =
		 * whenStr.substring(whenStr.indexOf(operator)+operator.length());
		 * 
		 * if(queryResources.containsStreamAlias(part1)) {
		 * if(SQLParser.isStreamField(drivingStreamId, part2)) {
		 * 
		 * } else { throw new ExceptionSQLStatement("stream "+ part1 +
		 * " does not have field "+part2); } } else
		 * if(queryResources.containsWindowAlias(part1)) {
		 * 
		 * }
		 * 
		 * 
		 * } }
		 * 
		 */
		return getQueryConditionExpression(whenStr, queryResources);
	}

	private static SQLQueryCondition getQueryConditionExpression(String whenStr, SQLQueryResources queryResources)
			throws ExceptionSQLStatement {

		int drivingStreamId = queryResources.getDrivingStreamId();

		String expression = formatExpressionPre(whenStr);

		RioDB.rio.getSystemSettings().getLogger().debug("expression PRE:" + expression);

		// ArrayLists of StringLike and StringIn objects if needed
		ArrayList<SQLStringLIKE> likeList = new ArrayList<SQLStringLIKE>();
		ArrayList<SQLStringIN> inList = new ArrayList<SQLStringIN>();
		
		// get the list of all required Windows for this query condition
		TreeSet<Integer> requiredWindows = new TreeSet<Integer>();

		/// REPLACE fields with StreamEvent variables and window functions.
		String words[] = expression.split(" ");
		for (int i = 0; i < words.length; i++) {

			if (SQLParser.isNumber(words[i])) {
			//	System.out.println(words[i] + " is number");

			}
			/*
			 * 
			 * 
			 */
			else if (SQLParser.isStringConstant(words[i])) {
				//String s = words[i] + " is string constant";
				words[i] = words[i].replace("'", "\"").replace("\" )", "\")");
				//System.out.println(s + words[i]);

			}
			/*
			 * 
			 * 
			 */
			else if (SQLParser.isStreamField(drivingStreamId, words[i])) {
				
				int fieldId = RioDB.rio.getEngine().getStream(drivingStreamId).getDef().getFieldId(words[i]);
				if (RioDB.rio.getEngine().getStream(drivingStreamId).getDef().isNumeric(fieldId)) {
					int eventFloatIndex = RioDB.rio.getEngine().getStream(drivingStreamId).getDef().getNumericFieldIndex(fieldId);
					words[i] = "event.getDouble(" + String.valueOf(eventFloatIndex) + ")";
					if (words[i + 1].equals("is_null")) {
						words[i + 1] = "!= Float.NaN";
					} else if (words[i + 1].equals("is_not_null")) {
						words[i + 1] = "= Float.NaN";
					}
				} else { // the field is string
					int eventStringIndex = RioDB.rio.getEngine().getStream(drivingStreamId).getDef().getStringFieldIndex(fieldId);
					words[i] = "event.getString(" + String.valueOf(eventStringIndex) + ")";
					if (i < words.length - 2) {
						if (words[i + 1].equals("=")) {
							words[i + 1] = ".equals(";
							words[i + 2] = words[i + 2].replace('\'', '"') + ")";
							i+=2;
						} else if (words[i + 1].equals("!=")) {
							words[i] = "!" + words[i];
							words[i + 1] = ".equals(";
							words[i + 2] = words[i + 2].replace('\'','"') + " )";
							i+=2;
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
				
			}
			/*
			 * 
			 * 
			 */
			else if (SQLParser.isMathFunction(words[i])) {
				//String s = words[i] + " is math function, modified to: ";
				words[i] = "Math." + SQLParser.mathFunctionRegCase(words[i]);
				//System.out.println(s + words[i]);

			}
			/*
			 * 
			 * 
			 */
			else if (words[i].contains(".")) {
				//String s = words[i] + " is composite, modified to: ";
				words[i] = modifyCompositeWord(words[i], drivingStreamId, queryResources, requiredWindows);
				//System.out.println(s + words[i]);

			}
			/*
			 * 
			 * 
			 */
			else if (words[i].equals("like")) {
				String s = words[i] + " is LIKE, modified to: ";

				if (i < words.length - 1 && i > 0) {
					String val = words[i + 1].replace("'", "");
					val = SQLParser.textDecode(val);
					String likeCounter = String.valueOf(likeList.size());

					String source = words[i - 1]; // preceeded by NOT
					if (source.equals("not")) {
						source = words[i - 2];
						words[i - 2] = "!";
					}
					words[i - 1] = "";

					words[i] = "likeList[" + likeCounter + "].match(" + source + ")";
					SQLStringLIKE sl = new SQLStringLIKE(val);
					likeList.add(sl);
					words[i + 1] = "";
					s = s + words[i - 1] + " " + words[i] + " " + words[i + 1];
					i++;
				} else {
					//System.out.println("i = " + i + " words.length = " + words.length);
				}

				// System.out.println("new word: "+ words[i] +" followed by "+ words[i + 1]);
				//System.out.println(s);

			}
			/*
			 * 
			 * 
			 */
			else if (words[i].equals("in")) {
				//String s = words[i] + " is IN, modified to: ";

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

					words[i] = "inList[" + inCounter + "].match(" + source + ")"; //
					//System.out.println(words[i]);
					SQLStringIN sl = new SQLStringIN(val);
					inList.add(sl);
					words[i + 1] = "";

					i = j;

				}

				//System.out.println(s + words[i] + words[i + 1]);
				//System.out.println("IN list size: " + inList.size());

			}
			/*
			 * 
			 * 
			 */
			else if (queryResources.containsWindowAlias(words[i])) {
			//	System.out.println(words[i] + " is window alias");
				int windowId = queryResources.getResourceByAlias(words[i]).getWindowId();
				requiredWindows.add(windowId);	
			}
			/*
			 * 
			 * 
			 */
			else if (queryResources.containsStreamAlias(words[i])) {
				//System.out.println(words[i] + " is stream alias");

			}
			/*
			 * 
			 * 
			 */
			else if (SQLParser.isOperator(words[i])) {
				//String s = words[i] + " is Operator";
				//System.out.println(s);

			}
			/*
			 * 
			 * 
			 */
			else if (SQLParser.isDelimiter(words[i])) {
				//String s = words[i] + " is Delimiter";
				//System.out.println(s);

			}
			/*
			 * 
			 * 
			 */
			else if (!SQLParser.isReservedWord(words[i]) && words[i].charAt(0) != '\'') {
				throw new ExceptionSQLStatement("unknown identifier: " + words[i]);

			}
			/*
			 * 
			 * 
			 */
			else {
				throw new ExceptionSQLStatement("No clue: " + words[i]);

			}
		}

		expression = "";
		for (String word : words) {
			expression = expression + word + " ";
		}
		expression = formatExpressionPost(expression);

		SQLStringLIKE[] likeArr = new SQLStringLIKE[likeList.size()];
		likeArr = likeList.toArray(likeArr);

		SQLStringIN[] inArr = new SQLStringIN[inList.size()];
		inArr = inList.toArray(inArr);

		expression = SQLParser.textDecode(expression).replace("''", "'");
		//System.out.println("new expression: " + expression);
		
		RioDB.rio.getSystemSettings().getLogger().debug("expression POST:" + expression);

		return new SQLQueryConditionExpression(expression, likeArr, inArr, whenStr, requiredWindows);
	}

	private static String formatExpressionPre(String expression) {

		expression = expression.replace(" or ", " || ");
		expression = expression.replace(" and ", " && ");
		expression = expression.replace("(", " ( ");
		expression = expression.replace(")", " ) ");
		expression = expression.replace("is null", "is_null");
		expression = expression.replace("is not null", "is_not_null");
		while (expression.contains("  ")) {
			expression = expression.replace("  ", " ");
		}
		expression = expression.replace(" .", ".");
		expression = expression.replace(". ", ".");

		return expression.trim();
	}

	private static String formatExpressionPost(String expression) {

		expression = expression.trim();
		expression = expression.replace("=", "==");
		expression = expression.replace("( \"", "(\"");
		expression = expression.replace("!==", "!=");
		expression = expression.replace(">==", ">=");
		expression = expression.replace("<==", "<=");
		expression = expression.replace("===", "==");
		expression = expression.replace(") .equals(", ").equals(");
		while (expression.contains("  ")) {
			expression = expression.replace("  ", " ");
		}
		return expression.trim();
	}

	private static String modifyCompositeWord(String word, int drivingStreamId, SQLQueryResources queryResources, TreeSet<Integer> requiredWindows)
			throws ExceptionSQLStatement {
		if (word.indexOf(".") > 0 && word.indexOf(".") < word.length() - 1) {
			String alias = word.substring(0, word.indexOf("."));
			String fieldName = word.substring(word.indexOf(".") + 1);
			if (queryResources.containsStreamAlias(alias)) {
				if (!RioDB.rio.getEngine().getStream(drivingStreamId).getDef().containsField(fieldName)) {
					throw new ExceptionSQLStatement("Stream " + alias + " does not have field " + fieldName);
				}
				int fieldId = RioDB.rio.getEngine().getStream(drivingStreamId).getDef().getFieldId(fieldName);
				if (RioDB.rio.getEngine().getStream(drivingStreamId).getDef().isNumeric(fieldId)) {
					int floatFieldIndex = RioDB.rio.getEngine().getStream(drivingStreamId).getDef().getNumericFieldIndex(fieldId);
					word = "event.getDouble(" + floatFieldIndex + ")";
				} else {
					int stringFieldIndex = RioDB.rio.getEngine().getStream(drivingStreamId).getDef().getStringFieldIndex(fieldId);
					word = "event.getString(" + stringFieldIndex + ")";
				}

			} else if (queryResources.containsWindowAlias(alias)) {
				int windowId = queryResources.getResourceByAlias(alias).getWindowId();
				requiredWindows.add(windowId);
				int windowStreamId = queryResources.getResourceByAlias(alias).getStreamId();
				if (!SQLParser.isSQLFunction(fieldName)) {
					throw new ExceptionSQLStatement(fieldName + " is not a valid function.");
				} else {
					int functionId = SQLFunctionMap.getFunctionId(fieldName);
					if (!RioDB.rio.getEngine().getStream(windowStreamId).getWindowMgr().getWindow(windowId)
							.windowRequiresFunction(functionId)) {
						throw new ExceptionSQLStatement("Window " + alias + " does not provide function " + fieldName);
					}

					// if window is local to stream
					if (windowStreamId == drivingStreamId) {
						word = "windowSummaries[" + windowId + "]." + SQLFunctionMap.getFunctionCall(functionId);
					}
					// else - foreign window from other stream
					else {
						word = "RioDB.rio.getStreamMgr().getStream(" + windowStreamId + ").getWindowMgr().getWindow(" + windowId + ")."
								+ SQLFunctionMap.getFunctionCall(functionId);
					}
				}
			}
		} else {
			throw new ExceptionSQLStatement("word not recognized. " + word);
		}
		return word;
	}
	

}
