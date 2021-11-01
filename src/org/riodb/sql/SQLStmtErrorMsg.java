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

final public class SQLStmtErrorMsg {

	public final static String messageList[] = {
			
			"Syntax Problem: ", 								// 0
			"PARSING: Statement cannot be null", 				// 1
			"PARSING: Expected ';' marking end of statement.", 	// 2
			"PARSING: Identifier starting with a period.", 		// 3
			"PARSING: 'SELECT' and 'FROM are required.", 		// 4
			"PARSING: Missing close parenthesis.",				// 5
			"PARSING: Too many close parenthesis.",				// 6
			"", // 7
			"", // 8
			"", // 9
			"RESOURCE: Expected one stream, like 'SELECT bid FROM stocks", 	// 10
			"RESOURCE: Can declare multiple windows, but only one stream.", // 11
			"RESOURCE: Expected stream(window limit) notation, like 'stockbids(40s)'", // 12
			"RESOURCE: Alias cannot be a number number", 		// 13
			"RESOURCE: Expected to match a Stream name.", 		// 14
			"",	// 15
			"", // 16
			"", // 17
			"", // 18
			"", // 19
			"WHERE: Expected one or more conditions", 			// 20
			"WHERE: Needs a valid comparison operator", 		// 21
			"WHERE: Cannot reference windows in WHERE clause.", // 22
			"WHERE: Not a valid stream or stream alias.",  		// 23
			"WHERE: The named field does not exist in the stream. Make sure conditions are written with field first, then operator, then value. example: 'bid > 10'", // 24
			"WHERE: Value to be compared is not a valid number.", // 25
			"WHERE: String value should be enclosed in single quotes.", // 26
			"WHERE: Expected a string value for the condition.", // 27
			"WHERE: Expected stream column ( or alias.column ) before operator. ", // 28
			"WHERE: Expected a comparison operator", 			// 29
			"WHERE: Resource alias not found in FROM declaration.", // 30
			"WHERE: Stream or stream alias missing in WHERE clause?", // 31
			"WHERE: Operator not supported for numeric comparisons.", // 32
			"WHERE: Operator unknown.", 						// 33
			"WHERE: String operator unknown.", 					// 34
			"WHERE: unknown identifier.", 						// 35
			"WHERE: IN Condition: Expected items enclosed by parenthesis like IN('hello')", // 36
			"", // 37
			"", // 38
			"", // 39
			"", // 40
			"", // 41
			"", // 42
			"", // 43
			"", // 44
			"", // 45
			"", // 46
			"", // 47
			"", // 48
			"", // 49
			"SELECT: Expected a selected column or function.", 		// 50
			"SELECT: Expected 'window.column', or 'window.function(column).", // 51
			"SELECT: Expected a valid function.", 					// 52
			"SELECT: Resource not found in the FROM declaration.", 	// 53
			"SELECT: Column not found in the stream definition.", 	// 54
			"SELECT: Missing single quote enclosure.", // 55
			"", // 56
			"", // 57
			"", // 58
			"", // 59
			"HAVING: Expected one or more conditions", 			// 60
			"HAVING: Needs a valid comparison operator", 		// 61
			"HAVING: Cannot reference windows in WHERE clause.", // 62
			"HAVING: Not a valid stream or stream alias.",  		// 63
			"HAVING: The named field does not exist in the stream. Make sure conditions are written with field first, then operator, then value. example: 'bid > 10'", // 64
			"HAVING: Value to be compared is not a valid number.", // 65
			"HAVING: String value should be enclosed in single quotes.", // 66
			"HAVING: Expected a string value for the condition.", // 67
			"HAVING: Expected stream column ( or alias.column ) before operator. ", // 68
			"HAVING: Expected a comparison operator", 			// 69
			"HAVING: Resource alias not found in FROM declaration.", // 70
			"HAVING: Stream or stream alias missing in HAVING clause?", // 71
			"HAVING: Operator not supported for numeric comparisons.", // 72
			"HAVING: Operator unknown.", 						// 73
			"HAVING: String operator unknown.", 					// 74
			"HAVING: unknown identifier.", 						// 75
			"HAVING: IN Condition: Expected items enclosed by parenthesis like IN('hello')", // 76
			"HAVING: For now, a driving stream must be explicitly declared in FROM... " // 77
			
	};

	public final static String write(int err, String code) {
		
		String e;
		if(err > 0 && err < SQLStmtErrorMsg.messageList.length) {
			e = "SQL Error "+ err +": "+ SQLStmtErrorMsg.messageList[err] + ":\n\t"+SQLParser.decodeQuotedText(code);
		}
		else {
			e = "SQL Error 0: Unhandled:\n\t"+SQLParser.decodeQuotedText(code);
		}
		return e;
		
	}
}
