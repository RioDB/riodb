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
 *   To encode and decode BASE64 strings... 
 */

package org.riodb.sql;

import java.util.ArrayList;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final public class BASE64Utils {

	// function to decode base64 text in quotes only.
	public static final String decodeQuotedText(String s) {

		// System.out.println("decodeQuotedText:"+s);

		if (s == null) {
			return null;
		}

		boolean inQuote = false;
		ArrayList<Integer> quoteBeginList = new ArrayList<Integer>();
		ArrayList<Integer> quoteEndList = new ArrayList<Integer>();
		for (int i = 0; i < s.length(); i++) {
			if (s.charAt(i) == '\'') {
				if (inQuote) {
					inQuote = false;
					quoteEndList.add(i);
				} else {
					inQuote = true;
					quoteBeginList.add(i);
				}
			}
		}

		String r = s;
		for (int i = 0; i < quoteEndList.size(); i++) {

			if (i < quoteEndList.size()) {
				String t = s.substring(quoteBeginList.get(i) + 1, quoteEndList.get(i));
				t = t.replace("$", "=");

				// confirm if the string is indeed base64 encoded:
				Pattern p = Pattern.compile("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$");
				Matcher m = p.matcher(t);
				if (m.find()) {
					// it is base64 encoded.
					byte[] decodedBytes = Base64.getDecoder().decode(t);
					String d = new String(decodedBytes);
					t = "'" + t.replace("=", "$") + "'";
					d = "'" + d + "'";
					r = r.replace(t, d);
				}
			}
		}

		// System.out.println("Decoded: "+ r);

		return r;

	}

	// function to decode base64 text in quotes only. Substituting single quotes for
	// double quotes.
	public static final String decodeQuotedTextToDoubleQuoted(String s) {

		// System.out.println("decodeQuotedTextToDoubleQuoted: "+ s);

		if (s == null) {
			return null;
		}

		boolean inQuote = false;
		ArrayList<Integer> quoteBeginList = new ArrayList<Integer>();
		ArrayList<Integer> quoteEndList = new ArrayList<Integer>();
		for (int i = 0; i < s.length(); i++) {
			if (s.charAt(i) == '\'') {
				if (inQuote) {
					inQuote = false;
					quoteEndList.add(i);
				} else {
					inQuote = true;
					quoteBeginList.add(i);
				}
			}
		}

		String r = s;
		for (int i = 0; i < quoteEndList.size(); i++) {

			if (i < quoteEndList.size()) {
				String t = s.substring(quoteBeginList.get(i) + 1, quoteEndList.get(i));
				t = t.replace("$", "=");

				// confirm if the string is indeed base64 encoded:
				Pattern p = Pattern.compile("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$");
				Matcher m = p.matcher(t);
				if (m.find()) {
					// it is base64 encoded.

					byte[] decodedBytes = Base64.getDecoder().decode(t);
					String d = new String(decodedBytes);
					d.replace("''", "'");
					t = "'" + t.replace("=", "$") + "'";
					d = "\"" + d + "\"";
					// System.out.println("replacing... "+ t + " -> " + d);
					r = r.replace(t, d);

				}
			}
		}

		// System.out.println("Decoded: "+ r);

		return r;

	}

	// function to decode base64
	public static final String decodeText(String s) {

		// System.out.println("decodeText: "+ s);
		try {
			byte[] decodedBytes = Base64.getDecoder().decode(s.replace("$", "="));
			return new String(decodedBytes);
		} catch (IllegalArgumentException iae) {
			// if s is invalid Base64, return original s
			return s;
		}
	}

	// Function to encode any quoted text into base64
	static final String encodeQuotedText(String s) {

		// System.out.println("Encoding: "+ s);
		if (s == null) {
			return null;
		}

		boolean inQuote = false;
		ArrayList<Integer> quoteBeginList = new ArrayList<Integer>();
		ArrayList<Integer> quoteEndList = new ArrayList<Integer>();
		for (int i = 0; i < s.length(); i++) {
			if (s.charAt(i) == '\'') {
				if (inQuote) {
					if (i < s.length() - 1 && s.charAt(i + 1) == '\'') {
						i++;
					} else {
						inQuote = false;
						quoteEndList.add(i);

					}
				} else {
					inQuote = true;
					quoteBeginList.add(i);
				}
			}
		}

		String r = s;
		for (int i = 0; i < quoteEndList.size(); i++) {
			if (i < quoteEndList.size()) {
				String t = s.substring(quoteBeginList.get(i) + 1, quoteEndList.get(i));
				String e = Base64.getEncoder().encodeToString(t.getBytes());
				e = e.replace("=", "$");
				t = "'" + t + "'";
				e = "'" + e + "'";
				r = r.replace(t, e);
				// System.out.println(t + " -> " + e);
			}
		}

		// System.out.println("Encoded: "+ r);

		return r;

	}

}
