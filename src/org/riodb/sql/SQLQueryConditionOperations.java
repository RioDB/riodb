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
		return getQueryConditionExpression(whenStr, queryResources);
	}

	private static SQLQueryCondition getQueryConditionExpression(String whenStr, SQLQueryResources queryResources)
			throws ExceptionSQLStatement {

		int drivingStreamId = queryResources.getDrivingStreamId();

		String expression = formatExpressionPre(whenStr);

		// ArrayLists of StringLike and StringIn objects if needed
		ArrayList<SQLStringLIKE> likeList = new ArrayList<SQLStringLIKE>();
		ArrayList<SQLStringIN> inList = new ArrayList<SQLStringIN>();

		// get the list of all required Windows for this query condition
		TreeSet<Integer> requiredWindows = new TreeSet<Integer>();

		/// REPLACE ENTITIES:
		String words[] = expression.split(" ");

		for (int i = 0; i < words.length; i++) {

			if (SQLParser.isNumber(words[i])) {
				// leave it. So it doesn't get picked up by contains(".") later...
			}

			// decode constant strings from BASE64
			else if (SQLParser.isStringConstant(words[i])) {
				// words[i] = "\"" +
				// SQLParser.decodeText(words[i].substring(0,words[i].length()-1)) + "\"";
			}

			else if (SQLParser.isStreamField(drivingStreamId, words[i])) {

				int fieldId = RioDB.rio.getEngine().getStream(drivingStreamId).getDef().getFieldId(words[i]);
				// if the field is numeric...
				if (RioDB.rio.getEngine().getStream(drivingStreamId).getDef().isNumeric(fieldId)) {
					int messageFloatIndex = RioDB.rio.getEngine().getStream(drivingStreamId).getDef()
							.getNumericFieldIndex(fieldId);
					words[i] = "message.getDouble(" + String.valueOf(messageFloatIndex) + ")";
				} else { // the field is string
					int messageStringIndex = RioDB.rio.getEngine().getStream(drivingStreamId).getDef()
							.getStringFieldIndex(fieldId);
					words[i] = "message.getString(" + String.valueOf(messageStringIndex) + ")";
				}
			}

			else if (SQLParser.isMathFunction(words[i])) {
				words[i] = "Math." + SQLParser.mathFunctionRegCase(words[i]);
			}

			else if (words[i].contains(".")) {
				words[i] = modifyCompositeWord(words[i], drivingStreamId, queryResources, requiredWindows);
			}

			else if (queryResources.containsWindowAlias(words[i])) {
				int windowId = queryResources.getResourceByAlias(words[i]).getWindowId();
				requiredWindows.add(windowId);
			}

		}

		// REPLACE OPERATORS
		for (int i = 1; i < words.length; i++) {

			if (words[i].equals("=")) {
				if (words[i - 1].contains("getDouble(") || SQLParser.isNumber(words[i - 1])) {
					words[i] = "==";
				} else {
					words[i] = ".equals(";
					words[i + 1] = words[i + 1] + ")";
				}
			}

			if (words[i].equals("!==")) {
				words[i] = "!=";
			}

			if (words[i].equals(">==")) {
				words[i] = ">=";
			}

			if (words[i].equals("<==")) {
				words[i] = "<=";
			}

			if (words[i].equals("===")) {
				words[i] = "==";
			}

			else if (words[i].equals("!=")) {
				if (words[i - 1].contains("getDouble(") || SQLParser.isNumber(words[i - 1])) {
					;
				} else {
					words[i - 1] = "!" + words[i - 1];
					words[i] = ".equals(";
					words[i + 1] = words[i + 1] + ")";
				}
			}

			else if (words[i].equals("is_null")) {
				if (words[i - 1].contains("getDouble(") || SQLParser.isNumber(words[i - 1])) {
					words[i] = "!= Float.NaN";
				} else {
					words[i] = ".equals(\"\")";
				}
			}

			else if (words[i].equals("is_not_null")) {
				if (words[i - 1].contains("getDouble(") || SQLParser.isNumber(words[i - 1])) {
					words[i] = "= Float.NaN";
				} else {
					words[i] = ".length() > 0";
				}
			}

			else if (SQLParser.isMathFunction(words[i])) {
				words[i] = "Math." + SQLParser.mathFunctionRegCase(words[i]);
			}

			else if (words[i].equals("like")) {
				// String s = words[i] + " is LIKE, modified to: ";

				if (i < words.length - 1 && i > 0) {
					// String val = words[i + 1];

					// System.out.println("next word is "+ words[i + 1]);

					if (words[i - 1].contains("getDouble(") || SQLParser.isNumber(words[i - 1])) {
						throw new ExceptionSQLStatement(
								"LIKE operator cannot be used for numeric variables.");
					}
					
					if (!SQLParser.isStringConstant(words[i + 1])) {
						throw new ExceptionSQLStatement(
								"Condition word LIKE must be followed by value in single quotes. Example: symbol LIKE 'tsla%'");
					}
					words[i + 1] = words[i + 1].replace("'", "");
					words[i + 1] = SQLParser.decodeText(words[i + 1]);
					String likeCounter = String.valueOf(likeList.size());

					String source = words[i - 1]; // preceeded by NOT
					if (source.equals("not")) {
						source = words[i - 2];
						words[i - 2] = "!";
					}
					words[i - 1] = "";

					words[i] = "likeList[" + likeCounter + "].match(" + source + ")";
					SQLStringLIKE sl = new SQLStringLIKE(words[i + 1]);
					likeList.add(sl);
					words[i + 1] = "";
					// s = s + words[i - 1] + " " + words[i] + " " + words[i + 1];
					i++;
				} else {
					throw new ExceptionSQLStatement(
							"Condition word LIKE must be followed by value in single quotes. Example: symbol LIKE 'tsla%'");
				}

			}

			else if (words[i].equals("in")) {
				// String s = words[i] + " is IN, modified to: ";

				if (i < words.length - 2 && i > 0) {

					if (words[i - 1].contains("getDouble(") || SQLParser.isNumber(words[i - 1])) {
						throw new ExceptionSQLStatement(
								" IN operator cannot be used for numeric variables.");
					}
					
					String val = "";
					int j = i + 2;
					while (words[j].indexOf(')') == -1) {
						val = val + words[j];
						words[j] = "";
						j++;
					}
					words[j] = "";

					val = "(" + SQLParser.decodeQuotedText(val) + ")";

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
					SQLStringIN sl = new SQLStringIN(val);
					inList.add(sl);
					words[i + 1] = "";
					i = j;

				}

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

		expression = SQLParser.decodeQuotedTextToDoubleQuoted(expression).replace("''", "'");
		
		RioDB.rio.getSystemSettings().getLogger().debug("EXPRESSION: " + expression);

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
		expression = expression.replace("( \"", "(\"");
		expression = expression.replace(") .equals(", ").equals(");
		while (expression.contains("  ")) {
			expression = expression.replace("  ", " ");
		}
		return expression.trim();
	}

	private static String modifyCompositeWord(String word, int drivingStreamId, SQLQueryResources queryResources,
			TreeSet<Integer> requiredWindows) throws ExceptionSQLStatement {
		if (word.indexOf(".") > 0 && word.indexOf(".") < word.length() - 1) {
			String alias = word.substring(0, word.indexOf("."));
			String fieldName = word.substring(word.indexOf(".") + 1);
			if (queryResources.containsStreamAlias(alias)) {
				if (!RioDB.rio.getEngine().getStream(drivingStreamId).getDef().containsField(fieldName)) {
					throw new ExceptionSQLStatement("Stream " + alias + " does not have field " + fieldName);
				}
				int fieldId = RioDB.rio.getEngine().getStream(drivingStreamId).getDef().getFieldId(fieldName);
				if (RioDB.rio.getEngine().getStream(drivingStreamId).getDef().isNumeric(fieldId)) {
					int floatFieldIndex = RioDB.rio.getEngine().getStream(drivingStreamId).getDef()
							.getNumericFieldIndex(fieldId);
					word = "message.getDouble(" + floatFieldIndex + ")";
				} else {
					int stringFieldIndex = RioDB.rio.getEngine().getStream(drivingStreamId).getDef()
							.getStringFieldIndex(fieldId);
					word = "message.getString(" + stringFieldIndex + ")";
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
						word = "RioDB.rio.getStreamMgr().getStream(" + windowStreamId + ").getWindowMgr().getWindow("
								+ windowId + ")." + SQLFunctionMap.getFunctionCall(functionId);
					}
				}
			}
		} else {
			throw new ExceptionSQLStatement("word not recognized. " + word);
		}
		return word;
	}

}
