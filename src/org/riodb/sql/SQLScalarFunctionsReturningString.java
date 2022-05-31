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
 *   Scalar functions that return STRING
 *   
 *   Instructions to add a new function:
 *   
 *   1- add the function name (in lower-case) to the array of functions
 *   2- write the function, with its name in lowercase always!
 *   
 */
package org.riodb.sql;

public final class SQLScalarFunctionsReturningString {

	private static String[] stringFunctions = { "concat", "decode", "to_string" };

	// a function to concatenate any number of String arguments
	public static String concat(String... strings) {
		String s = "";
		if (strings.length > 0) {
			s = strings[0];
			for (int i = 1; i < strings.length; i++) {
				if (strings[i] != null) {
					// s = s +strings[i];
					s = s.concat(strings[i]);
				}
			}
		}
		return s;
	}

	// a string to decode and evaluate Numbers
	public static double decode(double... numbers) {
		if (numbers.length > 2) {
			for (int i = 1; i < numbers.length; i += 2) {
				if (numbers[0] == (numbers[i]) && (i + 1) < numbers.length) {
					return numbers[i + 1];
				}
			}
			if (numbers.length % 2 == 0) {
				return numbers[numbers.length - 1];
			}
		}
		return Double.NaN;
	}

	// a string to decode and evaluate strings
	public static String decode(String... strings) {
		if (strings.length > 2) {
			for (int i = 1; i < strings.length; i += 2) {
				if (strings[0].equals(strings[i]) && (i + 1) < strings.length) {
					return strings[i + 1];
				}
			}
			if (strings.length % 2 == 0) {
				return strings[strings.length - 1];
			}
		}
		return null;
	}

	// function to check if a word is the name of a String function
	public static boolean isStringFunction(String word) {
		if (word == null) {
			return false;
		}
		String w = word.toLowerCase();
		for (String f : stringFunctions) {
			if (f.equals(w)) {
				return true;
			}
		}
		return false;
	}

	// This is not a scalar function. Just a function to check if a string contains
	// the name of a scalar function that returns a String.
	public static boolean stringContainsStringFunction(String word) {
		if (word == null) {
			return false;
		}
		String s = word.toLowerCase();
		for (String f : stringFunctions) {
			if (s.contains(f)) {
				return true;
			}
		}
		return false;
	}

	public static String to_string(boolean b) {
		return String.valueOf(b);
	}

	// functions to convert anything into a String
	public static String to_string(double d) {
		return String.valueOf(d);
	}

	public static String to_string(float f) {
		return String.valueOf(f);
	}

	public static String to_string(int i) {
		return String.valueOf(i);
	}

	public static String to_string(Object o) {
		return o.toString();
	}

}
