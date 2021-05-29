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
 * 	Instead of a final class (without variables) that checks if a string in IN a list of comma-separated strings,
 *  this class is initialized with pre-determined variables to expedite comparisons, 
 *  which improves performance when the same pattern is being evaluated many, many times.  
 * 
 *  note: NULL and Empty String "" are treated the same. 
 */

package org.riodb.sql;

public class SQLStringIN {

	private String list[];

	SQLStringIN(String str) throws ExceptionSQLStatement {

		String originalStr = str;
//		System.out.println("{"+str+"}");
		str = str.trim();
		if (str != null && str.length() > 0 && str.charAt(0) == '(') {
			str = str.substring(1);
			if (str.length() > 1 && str.charAt(str.length() - 1) == ')') {
				str = str.substring(0, str.length() - 1);
			} else {
				throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(36,originalStr));
				
			}
		} else {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(36,originalStr));
		}

		str = str.trim();
		list = str.split(",");
		for (int i = 0; i < list.length; i++) {
			list[i] = list[i].trim();
			if (list[i] != null && list[i].length() > 0 && list[i].charAt(0) == '\'') {
				list[i] = list[i].substring(1);
				if (list[i].length() > 1 && list[i].charAt(list[i].length() - 1) == '\'') {
					list[i] = list[i].substring(0, list[i].length() - 1);
				} else {
					throw new ExceptionSQLStatement("IN condition requires non-empty strings enclosed by single quote, like  IN ('hello')  \n\t IN " + originalStr);
				}
			} else {
				throw new ExceptionSQLStatement("IN condition requires non-empty strings enclosed by single quote, like  IN ('hello')  \n\t IN " + originalStr);			}
		}
		removeNulls();
		removeDuplicates();
		
	}

	public boolean match(String s) {
		if (s == null || s.length() == 0) {
			return list.length == 0;
		}
		for (int i = 0; i < list.length; i++) {
			if (list[i].equals(s))
				return true;
		}
		return false;
	}

	private void removeNulls() {

		for (int i = list.length - 1; i >= 0; i--) {
			if (list[i] == null || list[i].length() == 0) {

				// remove item from list
				String[] newList = new String[list.length - 1];
				for (int j = 0; j < i; j++) {
					newList[j] = list[j];
				}
				for (int j = i; j < newList.length; j++) {
					newList[j] = list[j + 1];
				}
				list = newList;
			}
		}
	}

	private void removeDuplicates() {

		for (int i = list.length - 1; i >= 0; i--) {
			int dups = 0;
			for (int j = 0; j < list.length; j++) {
				if (j != i && list[j].equals(list[i])) {
					dups++;
				}
			}
			if (dups > 0) {

				// remove item from list
				String[] newList = new String[list.length - 1];
				for (int j = 0; j < i; j++) {
					newList[j] = list[j];
				}
				for (int j = i; j < newList.length; j++) {
					newList[j] = list[j + 1];
				}
				list = newList;
			}
		}
	}
}
