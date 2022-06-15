
package org.riodb.sql;

public final class SQLScalarFunctions {

	// INDEX OF FUNCTIONS

	// the functions listed in the following arrays must be named in lower-case,
	// and their implementation must exist (with same exact name) in the respective
	// classes:

	// SQLScalarFunctionsReturningBoolean,
	private static String[] booleanFunctions = { "" };

	// SQLScalarFunctionsReturningNumber,
	private static String[] numericFunctions = { "to_number", "decode_number",
			"length", "floor", "ceil", "round", "instr" };

	// SQLScalarFunctionsReturningString
	private static String[] stringFunctions = { "decode", "concat", "to_string", "upper", "lower", "replace", "substr" };

	/*
	 * 
	 * 
	 * 
	 * The following are utility methods for the SQL expression compiler.
	 * 
	 */

	// function to check if a word is the name of a scalar function
	public static boolean isScalarFunction(String word) {
		if (word == null) {
			return false;
		} else if (isBooleanFunction(word) || isNumericFunction(word) || isStringFunction(word)) {
			return true;
		}
		return false;
	}

	// function to check if a word is the name of a scalar function that returns a
	// boolean
	public static boolean isBooleanFunction(String word) {
		if (word == null) {
			return false;
		}
		String w = word.toLowerCase();
		for (String f : booleanFunctions) {
			if (f.equals(w)) {
				return true;
			}
		}
		return false;
	}

	// checks if a word is the name of a scalar function
	public static boolean isNumericFunction(String word) {
		if (word == null) {
			return false;
		}
		String w = word.toLowerCase();
		for (String f : numericFunctions) {
			if (f.equals(w)) {
				return true;
			}
		}
		return false;
	}

	// function to check if a word is the name of a String function. NOT A SCALAR
	// FUNCTION
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
	// the name of a scalar function
	public static boolean stringContainsScalarFunction(String word) {
		if (word == null) {
			return false;
		}
		if (stringContainsBooleanFunction(word) || stringContainsNumericFunction(word)
				|| stringContainsStringFunction(word)) {
			return true;
		}
		return false;
	}

	// This is not a scalar function. Just a function to check if a string contains
	// the name of a scalar function that returns a String.
	public static boolean stringContainsBooleanFunction(String word) {
		if (word == null) {
			return false;
		}
		String s = word.toLowerCase();
		for (String f : booleanFunctions) {
			if (s.contains(f)) {
				return true;
			}
		}
		return false;
	}

	// This is not a scalar function. Just a function to check if a string contains
	// the name of a scalar function that returns a String.
	public static boolean stringContainsNumericFunction(String word) {
		if (word == null) {
			return false;
		}
		String s = word.toLowerCase();
		for (String f : numericFunctions) {
			if (s.contains(f)) {
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

}
