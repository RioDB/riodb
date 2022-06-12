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
	public static double decode_number(double... numbers) throws ExceptionSQLExecution {
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
		if(s == null) {
			return Double.NaN;
		}
		else if(SQLParser.isNumber(s)) {
			return Double.valueOf(s);
		}
		throw new ExceptionSQLExecution("TO_NUMBER: Failed attempt to convert an alpha-numeric string into a number.");
	}
	
	public static double floor(double d) {
		return Math.floor(d);
	}
	
	public static double ceil(double d) {
		return Math.ceil(d);
	}
	
	/**
	 * Rounds number to nearest integer.
	 * 
	 * @param s
	 * @return
	 */
	public static double round(double d) {
		return Math.round(d);
	}

}
