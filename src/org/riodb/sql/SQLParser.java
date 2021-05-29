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
 *   SQLParser is a collection of FINAL functions for processing SQL statements. 
 */

package org.riodb.sql;

import org.riodb.engine.RioDB;

public final class SQLParser {

	private static final String delimiterWords[] = { " ", "(", ")", "||", "&&", "'", "," };

	private static final String mathOperatorWords[] = { "+", "-", "*", "/" };

	private static final String mathWords[] = { "abs", "acos", "addExact", "asin", "atan", "atan2", "cbrt", "ceil",
			"copySign", "cos", "cosh", "decrementExact", "exp", "expm1", "floor", "floorDiv", "floorMod", "getExponent",
			"hypot", "IEEEremainder", "incrementExact", "log", "log10", "log1p", "greatest", "least", "multiplyExact",
			"negateExact", "nextAfter", "nextDown", "nextUp", "pow", "random", "rint", "round", "scalb", "signum",
			"sin", "sinh", "sqrt", "subtractExact", "tan", "tanh", "toDegrees", "toIntExactv", "toRadians", "ulp" };

	private static final String operatorWords[] = { ">", ">=", "<", "<=", "=", "!=", "in", "not_in", "like", "not_like", 
			"is_null", "is_not_null", "not", "null", ".equals(", ".equals (", ".equals(\"\")", "!= Float.NaN",
			"= Float.NaN" };

	public static final boolean containsOperator(String word) {
		if(word == null || word.length() == 0)
			return false;
		
		for(String operator : operatorWords) {
			if(word.contains(operator))
				return true;
		}
		
		return false;
	}

	public static final String formatStmt(String stmt) throws ExceptionSQLStatement {

		if (stmt == null) {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(1, stmt));
		}
		String newStmt = textEncode(stmt); // .toLowerCase();
		newStmt = formatStripComments(stmt); // .toLowerCase();

		newStmt = newStmt.replace("\t", " ");
		newStmt = newStmt.replace("\n", " ");
		newStmt = newStmt.replace("\r", " ");
		// newStmt = newStmt.replace(")",") ");
		newStmt = newStmt.replace(")", " ) ");
		newStmt = newStmt.replace("(", " ( ");

		newStmt = newStmt.replace(")and", ") and");
		newStmt = newStmt.replace(")or", ") or");
		newStmt = newStmt.replace("!=", " != ");
		newStmt = newStmt.replace(">=", " >= ");
		newStmt = newStmt.replace("<=", " <= ");
		newStmt = newStmt.replace("=", " = ");
		newStmt = newStmt.replace(">", " > ");
		newStmt = newStmt.replace("<", " < ");
		
		// math symbols
		newStmt = newStmt.replace("+", " + ");
		newStmt = newStmt.replace("-", " - ");
		newStmt = newStmt.replace("*", " * ");
		newStmt = newStmt.replace("/", " / ");

		
		// newStmt = newStmt.replace(")", ") ");

		while (newStmt.contains("  ")) {
			newStmt = newStmt.replace("  ", " ");
		}
		newStmt = newStmt.replace("! =", "!=");
		newStmt = newStmt.replace("> =", ">=");
		newStmt = newStmt.replace("< =", "<=");
		// newStmt = newStmt.replace("( ", "(");
		// newStmt = newStmt.replace(" (", "(");
		// newStmt = newStmt.replace(" )", ")");
		// newStmt = newStmt.replace("and(","and (");
		// newStmt = newStmt.replace("or(","or (");
		// newStmt = newStmt.replace(" ,", ",");
		newStmt = newStmt.replace(",", " , ");
		// newStmt = newStmt.replace("select(","select (");
		// newStmt = newStmt.replace("where(","where (");
		// newStmt = newStmt.replace("having(","having (");
		while (newStmt.contains("  ")) {
			newStmt = newStmt.replace("  ", " ");
		}
		newStmt = newStmt.trim();
		newStmt = newStmt.toLowerCase();

		if (newStmt.charAt(newStmt.length() - 1) != ';') {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(2, stmt));
		}
		if (newStmt.contains(" .")) {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(3, stmt));
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

		// System.out.println("formated: " + newStmt);

		return newStmt;
	}

	public static final String formatStripComments(String stmt) {
		String s = stmt;
		if (s != null) {
			boolean inComment = false;
			boolean inStatic = false;
			for (int i = 0; i < s.length(); i++) {
				if(s.charAt(i) == '\'' && !inComment) {
					if(inStatic)
						inStatic = false;
					else
						inStatic = true;
				} else if (s.charAt(i) == '#' && !inStatic) {
					inComment = true;
					s = s.substring(0, i) + " " + s.substring(i + 1);
				} else if (s.charAt(i) == '\n' || s.charAt(i) == '\r') {
					inComment = false;
				} else if (inComment) {
					// remove the current char
					s = s.substring(0, i) + " " + s.substring(i + 1);
				}
			}
			while (s.contains("  ")) {
				s = s.replace("  ", " ");
			}

		}
		return s;
	}

	public static final String getQueryFromStr(String stmt) throws ExceptionSQLStatement {
		String from = "-";
		int fromIndex = stmt.indexOf(" from ");
		if (fromIndex > 0) {
			fromIndex = fromIndex + 6;
			if (stmt.indexOf(" when ", fromIndex) > fromIndex) {
				from = stmt.substring(fromIndex, stmt.indexOf(" when ", fromIndex));
			} else if (stmt.indexOf(" when(", fromIndex) > fromIndex) {
				from = stmt.substring(fromIndex, stmt.indexOf(" when(", fromIndex));
			} else if (stmt.indexOf(" when ", fromIndex) > fromIndex) {
				from = stmt.substring(fromIndex, stmt.indexOf(" when ", fromIndex));
			} else if (stmt.indexOf(" output ", fromIndex) > fromIndex) {
				from = stmt.substring(fromIndex, stmt.indexOf(" output ", fromIndex));
			} else if (stmt.indexOf(" limit ", fromIndex) > fromIndex) {
				from = stmt.substring(fromIndex, stmt.indexOf(" limit ", fromIndex));
			} else if (stmt.indexOf(" timeout ", fromIndex) > fromIndex) {
				from = stmt.substring(fromIndex, stmt.indexOf(" timeout ", fromIndex));
			} else {
				from = stmt.substring(fromIndex, stmt.indexOf(";", fromIndex));
			}
		} else {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(4, stmt));
		}

		return from;
	}

	public static String getQueryLimitStr(String stmt) {
		
		String limit = null;

		if (stmt.contains(" limit ")) {
			if (stmt.contains(" timeout ")) {
				limit = stmt.substring(stmt.indexOf(" limit ") + 7, stmt.indexOf(" timeout "));
			} 
			else {
				limit = stmt.substring(stmt.indexOf(" limit ") + 7, stmt.indexOf(";"));
			}
			limit = limit.trim();
		}

		return limit;
	}
	
	/*	public static String getOperatorStr(String word) {
		if(word == null || word.length() == 0)
			return null;
		
		for(String operator : operatorWords) {
			if(word.contains(operator))
				return operator;
		}
		
		return null;
	}
*/	
	public static String getQueryOutputStr(String stmt) {
		
		String output = null;

		if (stmt.contains(" output ")) {
			if (stmt.contains(" limit ")) {
				output = stmt.substring(stmt.indexOf(" output ") + 8, stmt.indexOf(" limit "));
			}
			else if (stmt.contains(" timeout ")) {
				output = stmt.substring(stmt.indexOf(" output ") + 8, stmt.indexOf(" timeout "));
			} 
			else {
				output = stmt.substring(stmt.indexOf(" output ") + 8, stmt.indexOf(";"));
			}
		}

		return output;
	}

public static final String getQuerySelectList(String stmt) throws ExceptionSQLStatement {
		String select = "-";
		int selectIndex = stmt.indexOf("select ");
		int fromIndex = stmt.indexOf(" from ");

		if (selectIndex >= 0 && fromIndex > selectIndex + 1) {
			select = stmt.substring(stmt.indexOf("select ") + 7, stmt.indexOf(" from "));
		} else {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(4, stmt));
		}

		return select;
	}

	public static final String getQueryTimeoutStr(String stmt) throws ExceptionSQLStatement {

		if (stmt.contains(" timeout ")) 
			return stmt.substring(stmt.indexOf(" timeout ") + 9, stmt.indexOf(";")).trim();

		return null;
	}

	public static final String getQueryWhenStr(String stmt) throws ExceptionSQLStatement {
		String whenStr = "-";

		int fromIndex = stmt.indexOf(" from ");

		int whenIndex = stmt.indexOf(" when ", fromIndex);
		if (whenIndex == -1)
			whenIndex = stmt.indexOf(" when(", fromIndex);

		if (whenIndex > fromIndex) {
			whenIndex = whenIndex + 6;
			if (stmt.indexOf(" output ", whenIndex) > whenIndex) {
				whenStr = stmt.substring(whenIndex, stmt.indexOf(" output ", whenIndex));
			} else if (stmt.indexOf(" limit ", whenIndex) > whenIndex) {
				whenStr = stmt.substring(whenIndex, stmt.indexOf(" limit ", whenIndex));
			} else if (stmt.indexOf(" timeout ", whenIndex) > whenIndex) {
				whenStr = stmt.substring(whenIndex, stmt.indexOf(" timeout ", whenIndex));
			} else {
				whenStr = stmt.substring(whenIndex, stmt.indexOf(";", whenIndex));
			}
		}
		return whenStr;
	}

	// used for Create Window.
	public static final int getWindowFieldId(String stmt) throws ExceptionSQLStatement {

		int streamId = SQLParser.getWindowStreamId(stmt);

		if (streamId < 0) {
			throw new ExceptionSQLStatement("Invalid Stream name...");
		}
		String words[] = stmt.split(" ");
		if (words.length < 5) {
			throw new ExceptionSQLStatement("missing keyword");
		}
		if (words[0] == null || !words[0].equals("create")) {
			throw new ExceptionSQLStatement("missing keyword: *create* ... ");
		}
		if (words[1] == null || !words[1].equals("window")) {
			throw new ExceptionSQLStatement("missing keyword: create *window* ... ");
		}
		if (words[3] == null || !words[3].equals("running")) {
			throw new ExceptionSQLStatement("missing keyword: create window ... *running* ");
		}
		if (!stmt.contains("from")) {
			throw new ExceptionSQLStatement("missing keyword: create window ... running ... *from* ");
		}
		String fromStr = stmt.substring(stmt.indexOf("from") + 4);
		if (fromStr == null || !fromStr.contains(".")) {
			throw new ExceptionSQLStatement("Steam malformed. use \"from stream.field \" notation.");
		}

		String fieldName = fromStr.substring(fromStr.indexOf(".") + 1);
		fieldName = fieldName.substring(0, fieldName.indexOf(" "));
		fieldName = fieldName.trim();
		int fieldId = RioDB.rio.getEngine().getStream(streamId).getDef().getFieldId(fieldName);
		if (fieldId < 0) {
			throw new ExceptionSQLStatement("field not found. ");
		}
		if (!RioDB.rio.getEngine().getStream(streamId).getDef().isNumeric(fieldId)) {
			throw new ExceptionSQLStatement("field '" + fieldName + "' is not a numeric field.  ");

		}

		return fieldId;
	}
	
	public static final int getWindowPartitionExpiration(String stmt) throws ExceptionSQLStatement {
		
		int expirationSecs = 0;
		
		if (stmt.contains(" expire ")) {
			String expirationStr = stmt.substring(stmt.indexOf(" expire ") + 8);
			if (expirationStr.contains(" ")) {
				expirationStr = expirationStr.substring(0, expirationStr.indexOf(" "));
			} else {
				expirationStr = expirationStr.substring(0, expirationStr.indexOf(";"));
			}
			expirationStr.trim();


			if (expirationStr.charAt(expirationStr.length() - 1) == 's' || expirationStr.charAt(expirationStr.length() - 1) == 'm'
					|| expirationStr.charAt(expirationStr.length() - 1) == 'h' || expirationStr.charAt(expirationStr.length() - 1) == 'd') {
				
				String numberInRangeStr = expirationStr.substring(0, expirationStr.length() - 1);
				if(SQLParser.isNumber(numberInRangeStr)) {
					expirationSecs = Integer.valueOf(numberInRangeStr);
				}
				else {
					throw new ExceptionSQLStatement("could not find number in '"+ expirationStr +"'");
				}
				
				if(expirationStr.charAt(expirationStr.length() - 1) == 's') {
				}
				else if( expirationStr.charAt(expirationStr.length() - 1) == 'm') {
					expirationSecs = expirationSecs * 60;
				}
				else if( expirationStr.charAt(expirationStr.length() - 1) == 'h') {
					expirationSecs = expirationSecs * 60 * 60;
				}
				else if( expirationStr.charAt(expirationStr.length() - 1) == 'd') {
					expirationSecs = expirationSecs * 60 * 60 * 24;
				}
			}
			else {
				throw new ExceptionSQLStatement("Missing range-by-time unit  2s, 2m, 2h, 2d ...");
			}
		}
		return expirationSecs;

	}

	public static final int getWindowPartitionFieldId(String stmt) throws ExceptionSQLStatement {
		
		int partitionFieldId = -1;
		
		if (stmt.contains(" partition by ")) {
			String partition = stmt.substring(stmt.indexOf(" partition by ") + 14);
			if (partition.contains(" ")) {
				partition = partition.substring(0, partition.indexOf(" "));
			} else {
				partition = partition.substring(0, partition.indexOf(";"));
			}
			partition.trim();
			int streamId = getWindowStreamId(stmt);
			partitionFieldId = RioDB.rio.getEngine().getStream(streamId).getDef().getFieldId(partition);
			if (partitionFieldId < 0) {
				throw new ExceptionSQLStatement("partition field not found. ");
			}
		}

		return partitionFieldId;

	}

	public static final int getWindowRangeByFieldId(String stmt, int streamId) throws ExceptionSQLStatement {
		
		if (stmt != null && stmt.contains(" by ") && stmt.indexOf(";") > stmt.indexOf(" by ")) {
			String dateField = stmt.substring(stmt.indexOf(" by ") + 4, stmt.indexOf(";")).trim().toLowerCase();
			if(SQLParser.isStreamField(streamId, dateField)) { 
				int fieldId = RioDB.rio.getEngine().getStream(streamId).getDef().getFieldId(dateField);
				if(RioDB.rio.getEngine().getStream(streamId).getDef().isNumeric(fieldId)) {
					return fieldId;
				}
				else {
					throw new ExceptionSQLStatement("Field '"+ dateField +"' is not a 'date' field, and cannot be used for time range.");
				}
			} else {
				throw new ExceptionSQLStatement("Field '"+ dateField +"' not found. It and cannot be used for time range.");
			}
		}
		
		return -1;
	}
	
	public static final String getWindowRangeStr(String stmt) throws ExceptionSQLStatement {
		if (stmt != null && stmt.contains(" range ") && stmt.indexOf(";") > stmt.indexOf(" range ")) {
			
			String rangeStr = stmt.substring(stmt.indexOf(" range ") + 6, stmt.indexOf(";")).toLowerCase().trim();
			
			if(rangeStr == null || rangeStr.length() == 0) {
				throw new ExceptionSQLStatement("A window needs a RANGE. The range can be either a quantity (integer) or a time length (clock or timestamp)");
			}

			return rangeStr;
		}
		return null;
	}

	// used for Create Window.
	public static final String getWindowRunningFunctions(String stmt) throws ExceptionSQLStatement {

		String words[] = stmt.split(" ");
		if (words.length < 5) {
			throw new ExceptionSQLStatement("missing keyword");
		}
		if (words[0] == null || !words[0].equals("create")) {
			throw new ExceptionSQLStatement("missing keyword: *create* ... ");
		}
		if (words[1] == null || !words[1].equals("window")) {
			throw new ExceptionSQLStatement("missing keyword: create *window* ... ");
		}
		if (words[3] == null || !words[3].equals("running")) {
			throw new ExceptionSQLStatement("missing keyword: create window ... *running* ");
		}
		String functionStr = stmt.substring(stmt.indexOf("running") + 7);
		if (functionStr == null || !functionStr.contains("from")) {
			throw new ExceptionSQLStatement("missing keyword: create window ... running ... *from* ");
		}
		functionStr = functionStr.substring(0, functionStr.indexOf("from"));
		return functionStr;
	}

	// used for Create Window.
	public static final String getWindowStr(String stmt) throws ExceptionSQLStatement {

		String words[] = stmt.split(" ");
		if (words.length < 5) {
			throw new ExceptionSQLStatement("statement missing keywords");
		}
		if (words[0] == null || !words[0].equals("create")) {
			throw new ExceptionSQLStatement("not starting with 'create'.");
		}
		if (words[1] == null || !words[1].equals("window")) {
			throw new ExceptionSQLStatement("missing keyword: create window... ");
		}

		return words[2];
	}

	// used for Create Window.
	public static final int getWindowStreamId(String stmt) throws ExceptionSQLStatement {

		String words[] = stmt.split(" ");
		if (words.length < 5) {
			throw new ExceptionSQLStatement("missing keyword");
		}
		if (words[0] == null || !words[0].equals("create")) {
			throw new ExceptionSQLStatement("missing keyword: *create* ... ");
		}
		if (words[1] == null || !words[1].equals("window")) {
			throw new ExceptionSQLStatement("missing keyword: create *window* ... ");
		}
		if (words[3] == null || !words[3].equals("running")) {
			throw new ExceptionSQLStatement("missing keyword: create window ... *running* ");
		}
		if (!stmt.contains("from")) {
			throw new ExceptionSQLStatement("missing keyword: create window ... running ... *from* ");
		}
		String fromStr = stmt.substring(stmt.indexOf("from") + 4);
		if (fromStr == null || !fromStr.contains(".")) {
			throw new ExceptionSQLStatement("Steam malformed. use \"from stream.field \" notation.");
		}
		String streamName = fromStr.substring(0, fromStr.indexOf("."));
		streamName = streamName.trim();
		return RioDB.rio.getEngine().getStream(streamName);
	}

	public static final String getWindowWhereStr(String stmt) throws ExceptionSQLStatement {

		String where = null;

		int fromIndex = stmt.indexOf(" from ");

		int whereIndex = stmt.indexOf(" where ", fromIndex);
		if (whereIndex == -1)
			whereIndex = stmt.indexOf(" where(", fromIndex);

		if (whereIndex > fromIndex) {
			whereIndex = whereIndex + 6;
			if (stmt.indexOf(" partition ") > whereIndex) {
				where = stmt.substring(whereIndex, stmt.indexOf(" partition by ", whereIndex));
			} else if (stmt.indexOf(" range ") > whereIndex) {
				where = stmt.substring(whereIndex, stmt.indexOf(" range ", whereIndex));
			} else if (stmt.indexOf(" timeout ") > whereIndex) {
				where = stmt.substring(whereIndex, stmt.indexOf(" timeout ", whereIndex));
			} else {
				where = stmt.substring(whereIndex, stmt.indexOf(";", whereIndex));
			}
			where = where.trim();
		}

		return where;
	}

	public static boolean isDelimiter(String word) {
		if (word == null)
			return false;
		for (String s : delimiterWords) {
			if (s.equals(word))
				return true;
		}
		return false;
	}

	static boolean isMathFunction(String word) {
		if (word == null)
			return false;
		for (String s : mathWords) {
			if (s.toLowerCase().equals(word))
				return true;
		}
		return false;
	}

	public static boolean isMathOperator(String word) {
		if (word == null)
			return false;
		for (String s : mathOperatorWords) {
			if (s.equals(word))
				return true;
		}
		return false;
	}

	public static final boolean isNumber(String s) {
		if (s == null)
			return false;
		try {
			Float.valueOf(s);
			return true;
		} catch (NumberFormatException nfe) {
			return false;
		}
	}

	public static boolean isOperator(String word) {
		if (word == null)
			return false;
		for (String s : operatorWords) {
			if (s.equals(word))
				return true;
		}
		return false;
	}
	
	public static boolean isReservedWord(String word) {

		if (word == null)
			return false;

		if (isOperator(word))
			return true;
		if (isSQLFunction(word))
			return true;
		if (isMathFunction(word))
			return true;
		if (isDelimiter(word))
			return true;
		if (isOperator(word))
			return true;
		if (isMathOperator(word))
			return true;

		return false;
	}

	static boolean isSQLFunction(String word) {
		return SQLFunctionMap.isFunction(word);
	}
	
	public static final boolean isStreamField(int streamId, String word) {
		if (RioDB.rio.getEngine().getStream(streamId).getDef().getFieldId(word) >= 0) {
			return true;
		}
		return false;
	}
	
	public static final boolean isStreamName(String word) {
		if (RioDB.rio.getEngine().getStream(word) >= 0) {
			return true;
		}
		return false;
	}

	public static boolean isStringConstant(String word) {
		String s = word.trim();
		if (s != null && s.length() >= 2) {
			if (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
				return true;
			}
		}
		return false;
	}

	public static final boolean isWindowName(String word) {
		if (RioDB.rio.getEngine().getStreamIdOfWindow(word) >= 0) {
			return true;
		}
		return false;
	}

	public static final String mathFunctionRegCase(String word) {

		if (word == null)
			return null;

		if (word.equals("greatest"))
			return "max";
		if (word.equals("least"))
			return "min";

		for (String s : mathWords) {

			if (s.toLowerCase().equals(word) && !s.equals(word)) {
				return s;
			}
		}

		return word;
	}

	public static final String textDecode(String s) {
		if (s != null) {
			if (s.indexOf("$s~$") >= 0)
				s = s.replace("$s~$", " ");
			if (s.indexOf("$t~$") >= 0)
				s = s.replace("$t~$", "\t");
			if (s.indexOf("$n~$") >= 0)
				s = s.replace("$n~$", "\n");
			if (s.indexOf("$r~$") >= 0)
				s = s.replace("$r~$", "\r");
			if (s.indexOf("$op~$") >= 0)
				s = s.replace("$op~$", "(");
			if (s.indexOf("$cp~$") >= 0)
				s = s.replace("$cp~$", ")");

			if (s.indexOf("$nt~$") >= 0)
				s = s.replace("$nt~$", "!");
			if (s.indexOf("$eq~$") >= 0)
				s = s.replace("$eq~$", "=");
			if (s.indexOf("$gt~$") >= 0)
				s = s.replace("$gt~$", ">");
			if (s.indexOf("$lt~$") >= 0)
				s = s.replace("$lt~$", "<");
			if (s.indexOf("$sc~$") >= 0)
				s = s.replace("$sc~$", ";");
			if (s.indexOf("$pp~$") >= 0)
				s = s.replace("$pp~$", "|");
			if (s.indexOf("$es~$") >= 0)
				s = s.replace("$es~$", "&");
			if (s.indexOf("$cm~$") >= 0)
				s = s.replace("$cm~$", ",");
			if (s.indexOf("$pr~$") >= 0)
				s = s.replace("$pr~$", ".");
			if (s.indexOf("$ht~$") >= 0)
				s = s.replace("$ht~$", "#");
			if (s.indexOf("$qq~$") >= 0)
				s = s.replace("$qq~$", "''");

		}
		return s;
	}
	
	private static final String textEncode(String s) {
		if (s != null) {
			boolean inQuote = false;
			for (int i = 0; i < s.length(); i++) {
				if (s.charAt(i) == '\'') {
					if (inQuote) {
						if (i < s.length() - 1 && s.charAt(i + 1) == '\'') {
							i++;
						} else {
							inQuote = false;
						}
					} else {
						inQuote = true;
					}
				} else if (inQuote) {
					if (s.charAt(i) == ' ')
						s = s.substring(0, i) + "$s~$" + s.substring(i + 1);
					else if (s.charAt(i) == '\t')
						s = s.substring(0, i) + "$t~$" + s.substring(i + 1);
					else if (s.charAt(i) == '\n')
						s = s.substring(0, i) + "$n~$" + s.substring(i + 1);
					else if (s.charAt(i) == '\r')
						s = s.substring(0, i) + "$r~$" + s.substring(i + 1);
					else if (s.charAt(i) == '(')
						s = s.substring(0, i) + "$op~$" + s.substring(i + 1);
					else if (s.charAt(i) == ')')
						s = s.substring(0, i) + "$cp~$" + s.substring(i + 1);
					else if (s.charAt(i) == '!')
						s = s.substring(0, i) + "$nt~$" + s.substring(i + 1);
					else if (s.charAt(i) == '=')
						s = s.substring(0, i) + "$eq~$" + s.substring(i + 1);
					else if (s.charAt(i) == '>')
						s = s.substring(0, i) + "$gt~$" + s.substring(i + 1);
					else if (s.charAt(i) == '<')
						s = s.substring(0, i) + "$lt~$" + s.substring(i + 1);
					else if (s.charAt(i) == ';')
						s = s.substring(0, i) + "$sc~$" + s.substring(i + 1);
					else if (s.charAt(i) == '|')
						s = s.substring(0, i) + "$pp~$" + s.substring(i + 1);
					else if (s.charAt(i) == '&')
						s = s.substring(0, i) + "$ep~$" + s.substring(i + 1);
					else if (s.charAt(i) == ',')
						s = s.substring(0, i) + "$cm~$" + s.substring(i + 1);
					else if (s.charAt(i) == '.')
						s = s.substring(0, i) + "$pr~$" + s.substring(i + 1);
					else if (s.charAt(i) == '#')
						s = s.substring(0, i) + "$ht~$" + s.substring(i + 1);
				} else {
					String lowerChar = String.valueOf(s.charAt(i)).toLowerCase();
					s = s.substring(0, i) + lowerChar + s.substring(i + 1);

				}
			}
			s = s.replace("''", "$qq~$");
		}
		return s;
	}

}
