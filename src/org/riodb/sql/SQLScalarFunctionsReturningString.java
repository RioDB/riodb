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


	// a function to concatenate any number of String arguments
	public static String concat(String... strings) throws ExceptionSQLExecution {
		String s = "";
		if (strings != null && strings.length > 0) {
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

	// a string to decode and evaluate strings
	public static String decode(String... strings) throws ExceptionSQLExecution {

		if (strings != null && strings.length > 2) {
			for (int i = 1; i < strings.length; i += 2) {
				if (strings[0].equals(strings[i]) && (i + 1) < strings.length) {
					return strings[i + 1];
				}
			}
			if (strings.length % 2 == 0) {
				return strings[strings.length - 1];
			}
		} else if (strings == null || strings.length == 0) {
			throw new ExceptionSQLExecution("DECODE: decode() called with no arguments.");
		}
		return strings[0];
	}

	public static String to_string(boolean b) throws ExceptionSQLExecution {
		return String.valueOf(b);
	}

	// functions to convert anything into a String
	public static String to_string(double d) throws ExceptionSQLExecution {
		return String.valueOf(d);
	}

	public static String to_string(float f) throws ExceptionSQLExecution {
		return String.valueOf(f);
	}

	public static String to_string(int i) throws ExceptionSQLExecution {
		return String.valueOf(i);
	}

	public static String to_string(Object o) throws ExceptionSQLExecution {
		return o.toString();
	}

    public static String upper(String s) {
        return (s == null) ? null : s.toUpperCase();
    }

    public static String lower(String s) {
        return (s == null) ? null : s.toLowerCase();
    }

    public static String replace(String s, String old_s, String new_s) {
        return (s == null) ? null : s.replace(old_s, new_s);
    }

    public static String substr(String s, int from) throws ExceptionSQLExecution {
		return substr(s, from, s.length());
    }

    public static String substr(String s, int from, int to) throws ExceptionSQLExecution {
		try {
			return (s == null) ? null : s.substring(from, to);
		} catch (StringIndexOutOfBoundsException e) {
			throw new ExceptionSQLExecution("SUBSTR: Out of bounds");
		}
    }
}
