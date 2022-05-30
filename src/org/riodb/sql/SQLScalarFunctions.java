package org.riodb.sql;

public final class SQLScalarFunctions {

	private static String[] stringFunctions = { "decode", "concat", "length", "to_string" };
	private static String[] numericFunctions = { "to_number" };
	private static String[] dateFunctions = { "" };

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
		// System.out.println("Concatenated: "+ s + " items: "+ strings.length);
		return s;
	}

	public static double decode(double... numbers) {
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

	public static boolean isDateFunction(String word) {
		if (word == null) {
			return false;
		}
		String w = word.toLowerCase();
		for (String f : dateFunctions) {
			if (f.equals(w)) {
				return true;
			}
		}
		return false;
	}

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

	public static boolean isScalarFunction(String word) {
		if (word == null) {
			return false;
		}
		if (isStringFunction(word)) {
			return true;
		}
		if (isNumericFunction(word)) {
			return true;
		}
		if (isDateFunction(word)) {
			return true;
		}

		return false;
	}

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

	public static int length(String s) {
		if (s == null) {
			return 0;
		}
		return s.length();
	}

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
	
	public static String to_string(double d) {
		return String.valueOf(d);
	}
	public static String to_string(float f) {
		return String.valueOf(f);
	}
	public static String to_string(int i) {
		return String.valueOf(i);
	}
	public static String to_string(boolean b) {
		return String.valueOf(b);
	}
	public static String to_string(Object o) {
		return o.toString();
	}
	
	public static double to_number(String s) {
		return Double.valueOf(s);
	}

}
