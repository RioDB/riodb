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
 *   Scalar functions that return NUMBER
 *   
 *   Instructions to add a new function:
 *   
 *   1- add the function name (in lower-case) to the array of functions
 *   2- write the function, with its name in lowercase always!
 *   
 */

package org.riodb.sql;

public final class SQLScalarFunctionsReturningNumber {

	// Decode function that evaluates numbers and return number.
	public static double decode_number(double... numbers)
			throws ExceptionSQLExecution {
		if (numbers.length > 2) {
			for (int i = 1; i < numbers.length; i += 2) {
				if (numbers[0] == numbers[i] && (i + 1) < numbers.length) {
					return numbers[i + 1];
				}
			}
			if (numbers.length % 2 == 0) {
				return numbers[numbers.length - 1];
			}
		}
		return Double.NaN;
	}

	// function that returns the length of a string
	public static int length(String s) throws ExceptionSQLExecution {
		if (s == null) {
			return 0;
		}
		return s.length();
	}

	// function that returns a string as number.
	public static double to_number(String s) throws ExceptionSQLExecution {
		if (s == null) {
			return Double.NaN;
		} else if (SQLParser.isNumber(s)) {
			return Double.valueOf(s);
		}
		throw new ExceptionSQLExecution(
				"TO_NUMBER: Failed attempt to convert an alpha-numeric string into a number.");
	}

	public static double floor(double d) throws ExceptionSQLExecution {
		throwExceptionIfNaN(d, "FLOOR");
		return Math.floor(d);
	}

	public static double ceil(double d) throws ExceptionSQLExecution {
		throwExceptionIfNaN(d, "CEIL");
		return Math.ceil(d);
	}

	/**
	 * Rounds number to nearest integer.
	 * 
	 * @param s
	 * @return
	 * @throws ExceptionSQLExecution
	 */
	public static double round(double d) throws ExceptionSQLExecution {
		throwExceptionIfNaN(d, "ROUND");
		return Math.round(d);
	}

	// Finds find occurrence of substring within string, returns -1 if not
	// found, otherwise returns position of first occurrence
	public static double instr(String string, String substring) {
		if (string == null || substring == null)
			return -1;
		int pos = string.indexOf(substring) + 1;
		return pos == 0 ? -1 : pos;
		// Since we need to add 1, if it's 0, then it must've been -1 before
	}

	private static void throwExceptionIfNaN(double d, String methodName)
			throws ExceptionSQLExecution {
		if (d == Double.NaN)
			throw new ExceptionSQLExecution(
					methodName + ": Number entered is NaN");
	}
}
