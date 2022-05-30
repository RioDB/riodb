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

/*
 *   A final class for converting SQL code into Java code.
 */

package org.riodb.sql;

import java.util.ArrayList;
import java.util.TreeSet;

import org.riodb.engine.RioDB;

public final class JavaGenerator {

	public static String convertSqlToJava(String sqlExpression, SQLQueryResources queryResources, int drivingStreamId,
			ArrayList<SQLStringLIKE> likeList, ArrayList<SQLStringIN> inList, TreeSet<Integer> requiredWindows)
			throws ExceptionSQLStatement {

		if (sqlExpression == null) {
			return null;
		}

		String javaExpression = formatExpressionPre(sqlExpression);

		/// REPLACE ENTITIES:
		String words[] = javaExpression.split(" ");

		for (int i = 0; i < words.length; i++) {

			if (SQLParser.isNumber(words[i])) {
				// leave this check here. So it doesn't get picked up by contains(".") later...
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

				// if previous stream.field
				if (i > 0 && words[i - 1].equals("previous")) {
					words[i - 1] = "";
					words[i] = words[i].replace("message.get", "previousMessage.get");
				}
			}

			else if (SQLParser.isMathFunction(words[i])) {
				words[i] = "Math." + SQLParser.mathFunctionRegCase(words[i]);
			}

			else if (SQLScalarFunctions.isScalarFunction(words[i].toLowerCase())) {
				words[i] = "SQLScalarFunctions." + words[i].toLowerCase();
			}

			else if (words[i].contains(".")) {

				words[i] = replaceWindowNameWithObjectName(words[i], drivingStreamId, queryResources, requiredWindows);

				// if previous stream.field
				if (i > 0 && words[i - 1].equals("previous")) {
					words[i - 1] = "";
					words[i] = words[i].replace("message.get", "previousMessage.get");
//					requiresPrevious = true;
				}

			}

			else if (queryResources != null && queryResources.containsWindowAlias(words[i])) {

				int windowId = queryResources.getResourceByAlias(words[i]).getWindowId();
				requiredWindows.add(windowId);
			}

			else if (SQLAggregateFunctions.isFunction(words[i])) {

				if (queryResources != null && queryResources.countWindows() == 1) {
					int windowId = queryResources.getResourceById(0).getWindowId();
					int functionId = SQLAggregateFunctions.getFunctionId(words[i]);
					String alias = queryResources.getResourceById(0).getAlias();

					if ((windowId >= 0 && !RioDB.rio.getEngine().getStream(drivingStreamId).getWindowMgr()
							.getWindow(windowId).windowRequiresFunction(functionId))
							|| (windowId < 0 && !RioDB.rio.getEngine().getStream(drivingStreamId).getWindowMgr()
									.getWindow_String(windowId).windowRequiresFunction(functionId))) {
						throw new ExceptionSQLStatement("Window " + alias + " does not provide function " + words[i]);
					}

					if (windowId >= 0) {
						words[i] = "windowSummaries[0]." + SQLAggregateFunctions.getFunctionCall(functionId);
					} else {
						words[i] = "windowSummaries_String[0]." + SQLAggregateFunctions.getFunctionCall(functionId);
					}
				}
			}

		}

		if (words[0].equals("not")) {
			words[0] = "!";
		}

		// REPLACE OPERATORS
		for (int i = 1; i < words.length; i++) {

			if (words[i].equals("not")) {
				words[i] = "!";
			}

			else if (words[i].equals("=") || words[i].equals("!=")) {

				// = sign is a bit messy, because for everything that is a number, it needs to
				// be converted into "=="
				// but for everything that is a string, it needs to be converted into
				// ".equals()"

				boolean preceededByString = false;

				if (SQLParser.isStringResource(words[i - 1], drivingStreamId)) {
					preceededByString = true;
					if (words[i].equals("!=")) {
						words[i - 1] = "!" + words[i - 1];
					}

				} else if (words[i - 1].equals(")")) {
					int openingParenthesisIndex = SQLParser.getIndexOfOpeningParenthesis(words, i - 1);
					if (openingParenthesisIndex == -1) {
						throw new ExceptionSQLStatement("Could not identify opening parenthesis in query condition.");
					} else if (openingParenthesisIndex > 0
							&& words[openingParenthesisIndex - 1].startsWith("SQLScalarFunctions.")) {
						preceededByString = true;
						if (words[i].equals("!=")) {
							words[openingParenthesisIndex - 1] = "!" + words[openingParenthesisIndex - 1];
						}
					}

				}

				if (preceededByString) {

					words[i] = ".equals(";
					if (i < words.length - 2 && words[i + 1].startsWith("SQLScalarFunctions.")
							&& words[i + 2].equals("(")) {
						int closingParenthesisIndex = SQLParser.getIndexOfClosingParenthesis(words, i + 2);
						if (closingParenthesisIndex == -1) {
							throw new ExceptionSQLStatement(
									"Could not identify closing parenthesis in query condition.");
						}
						words[closingParenthesisIndex] = ") )";
					} else {
						words[i + 1] = words[i + 1] + ")";
					}

				}
				// else we are dealling with numbers.
				else if (words[i].equals("=")) {
					// = in SQL is converted into == in java
					words[i] = "==";
				}

			}

			else if (words[i].equals("<") || words[i].equals(">") || words[i].equals("<=") || words[i].equals(">=")) {

				String sign = words[i]; // < or > or <= or >=

				// = sign is a bit messy, because for everything that is a number, it needs to
				// be converted into "=="
				// but for everything that is a string, it needs to be converted into
				// ".equals()"

				boolean preceededByString = false;

				if (SQLParser.isStringResource(words[i - 1], drivingStreamId)) {
					preceededByString = true;
				} else if (words[i - 1].equals(")")) {
					int openingParenthesisIndex = SQLParser.getIndexOfOpeningParenthesis(words, i - 1);
					if (openingParenthesisIndex == -1) {
						throw new ExceptionSQLStatement("Could not identify opening parenthesis in query condition.");
					} else if (openingParenthesisIndex > 0
							&& words[openingParenthesisIndex - 1].startsWith("SQLScalarFunctions.")) {
						preceededByString = true;
					}

				}

				if (preceededByString) {

					words[i] = ".compareTo(";
					if (i < words.length - 2 && words[i + 1].startsWith("SQLScalarFunctions.")
							&& words[i + 2].equals("(")) {
						int closingParenthesisIndex = SQLParser.getIndexOfClosingParenthesis(words, i + 2);
						if (closingParenthesisIndex == -1) {
							throw new ExceptionSQLStatement(
									"Could not identify closing parenthesis in query condition.");
						}
						words[closingParenthesisIndex] = ") ) " + sign + " 0";
					} else {
						words[i + 1] = words[i + 1] + ") " + sign + " 0";
					}

				}

			}

			else if (words[i].equals("!==")) {
				words[i] = "!=";
			}

			else if (words[i].equals(">==")) {
				words[i] = ">=";
			}

			else if (words[i].equals("<==")) {
				words[i] = "<=";
			}

			else if (words[i].equals("===")) {
				words[i] = "==";
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

					if (words[i - 1].contains("getDouble(") || SQLParser.isNumber(words[i - 1])) {
						throw new ExceptionSQLStatement("LIKE operator cannot be used for numeric variables.");
					}

					if (!SQLParser.isStringConstant(words[i + 1])) {
						throw new ExceptionSQLStatement(
								"Condition word LIKE must be followed by value in single quotes. Example: symbol LIKE 'tsla%'");
					}
					words[i + 1] = words[i + 1].replace("'", "");
					words[i + 1] = BASE64Utils.decodeText(words[i + 1]);
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
						throw new ExceptionSQLStatement(" IN operator cannot be used for numeric variables.");
					}

					String val = "";
					int j = i + 2;
					while (words[j].indexOf(')') == -1) {
						val = val + words[j];
						words[j] = "";
						j++;
					}
					words[j] = "";

					val = "(" + BASE64Utils.decodeQuotedText(val) + ")";

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

		javaExpression = "";
		for (String word : words) {
			javaExpression = javaExpression + word + " ";
		}

		javaExpression = formatExpressionPost(javaExpression);

		javaExpression = BASE64Utils.decodeQuotedTextToDoubleQuoted(javaExpression).replace("''", "'");

		return javaExpression;
	}

	// method to go back in array of words and find index of an opening parenthesis
	// note: accounts for nested parenthesis pairs in between
	public static int getIndexOfOpeningParenthesis(String words[], int indexOfClosingParenthesis) {

		if (indexOfClosingParenthesis == 0) {
			return -1;
		}

		int parenthesisDepth = 1;

		for (int i = indexOfClosingParenthesis - 1; i >= 0; i--) {

			if (words[i].equals(")")) {
				parenthesisDepth++;
			} else if (words[i].equals("(") && parenthesisDepth > 1) {
				parenthesisDepth--;
			} else if (words[i].equals("(")) {
				return i;
			}

		}
		return 0;
	}

	// method to go forward in array of words and find index of a closing
	// parenthesis
	// note: accounts for nested parenthesis pairs in between
	public static int getIndexOfClosingParenthesis(String words[], int indexOfOpeningParenthesis) {

		if (indexOfOpeningParenthesis == words.length - 1) {
			return -1;
		}

		int parenthesisDepth = 1;

		for (int i = indexOfOpeningParenthesis + 1; i < words.length; i++) {

			if (words[i].equals("(")) {
				parenthesisDepth++;
			} else if (words[i].equals(")") && parenthesisDepth > 1) {
				parenthesisDepth--;
			} else if (words[i].equals(")")) {
				return i;
			}

		}
		return -1;
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

	private static String replaceWindowNameWithObjectName(String word, int drivingStreamId,
			SQLQueryResources queryResources, TreeSet<Integer> requiredWindows) throws ExceptionSQLStatement {

		if (queryResources != null && word.indexOf(".") > 0 && word.indexOf(".") < word.length() - 1) {

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
				if (!SQLParser.isAggregateFunction(fieldName)) {
					throw new ExceptionSQLStatement(fieldName + " is not a valid function.");
				} else {
					int functionId = SQLAggregateFunctions.getFunctionId(fieldName);
					if ((windowId >= 0 && !RioDB.rio.getEngine().getStream(windowStreamId).getWindowMgr()
							.getWindow(windowId).windowRequiresFunction(functionId))
							|| (windowId < 0 && !RioDB.rio.getEngine().getStream(windowStreamId).getWindowMgr()
									.getWindow_String(windowId).windowRequiresFunction(functionId))) {
						throw new ExceptionSQLStatement("Window " + alias + " does not provide function " + fieldName);
					}

					// if window is local to stream
					if (windowStreamId == drivingStreamId) {
						if (windowId >= 0) {
							word = "windowSummaries[" + windowId + "]."
									+ SQLAggregateFunctions.getFunctionCall(functionId);
						} else {
							word = "windowSummaries_String[" + (windowId + 1) * -1 + "]."
									+ SQLAggregateFunctions.getFunctionCall(functionId);
						}
					}
					// else - foreign window from other stream
					else {
						if (windowId >= 0) {
							word = "RioDB.rio.getStreamMgr().getStream(" + windowStreamId
									+ ").getWindowMgr().getWindow(" + windowId + ")."
									+ SQLAggregateFunctions.getFunctionCall(functionId);
						} else {
							word = "RioDB.rio.getStreamMgr().getStream(" + windowStreamId
									+ ").getWindowMgr().getWindow_String(" + windowId + ")."
									+ SQLAggregateFunctions.getFunctionCall(functionId);
						}
					}
				}
			}
		}

		else if (word.contains(".") && !SQLParser.isNumber(word)) {

			String streamName = word.substring(0, word.indexOf("."));
			String fieldName = word.substring(word.indexOf(".") + 1);

			// if the alias requested is the Driving Stream:
			if (streamName != null && SQLParser.isStreamName(streamName)) {

				int sId = RioDB.rio.getEngine().getStreamId(streamName);

				if (fieldName != null && SQLParser.isStreamField(sId, fieldName)) {

					int fieldId = RioDB.rio.getEngine().getStream(sId).getDef().getFieldId(fieldName);

					drivingStreamId = sId;

					boolean isNumeric = RioDB.rio.getEngine().getStream(drivingStreamId).getDef().isNumeric(fieldId);
					if (isNumeric) {
						int floatFieldIndex = RioDB.rio.getEngine().getStream(drivingStreamId).getDef()
								.getNumericFieldIndex(fieldId);
						word = "message.getDouble(" + floatFieldIndex + ")";
					} else {
						int stringFieldIndex = RioDB.rio.getEngine().getStream(drivingStreamId).getDef()
								.getStringFieldIndex(fieldId);
						word = "message.getString(" + stringFieldIndex + ")";
					}

				}
			}

		}

		else {
			throw new ExceptionSQLStatement("word not recognized. " + word);
		}
		return word;
	}

}
