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

import java.util.HashSet;
import java.util.Iterator;

import org.riodb.engine.RioDB;

public class SQLStringIN {

	private HashSet<String> strings;

	SQLStringIN(String str) throws ExceptionSQLStatement {

		strings = new HashSet<String>();

		String originalStr = str;
//		System.out.println("{"+str+"}");
		str = str.trim();
		if (str != null && str.length() > 0 && str.charAt(0) == '(') {
			str = str.substring(1);
			if (str.length() > 1 && str.charAt(str.length() - 1) == ')') {
				str = str.substring(0, str.length() - 1);
			} else {
				throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(36, originalStr));

			}
		} else {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(36, originalStr));
		}

		str = str.trim();
		String list[] = str.split(",");
		for (int i = 0; i < list.length; i++) {
			list[i] = list[i].trim();
			if (list[i] != null && (list[i].equals("''") || list[i].toLowerCase().equals("null"))) {
				list[i] = null;
			} else if (list[i] != null && list[i].length() > 0 && list[i].charAt(0) == '\''
					&& list[i].charAt(list[i].length() - 1) == '\'') {
				list[i] = list[i].substring(1);
				list[i] = list[i].substring(0, list[i].length() - 1);
			} else {
				throw new ExceptionSQLStatement(
						"IN condition requires comma-separated strings enclosed by single quote, like  IN('word','more words',null,'Got it?')  \n\t IN "
								+ originalStr);
			}
			
			strings.add(list[i]);
		}
		
		
		RioDB.rio.getSystemSettings().getLogger().debug("SQLStringIN object created with words: "+ getElements());

	}

	public boolean match(String s) {
		return strings.contains(s);
	}
	
	public String getElements() {
		String elements = "";
		Iterator<String> itr = strings.iterator();
        while (itr.hasNext()) {
            elements = elements + "'"+ itr.next() + "'";
            if(itr.hasNext())
            	elements = elements + ",";
        }
        return elements;
	}

}
