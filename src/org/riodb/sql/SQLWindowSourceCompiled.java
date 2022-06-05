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
 *  Windows have a FROM clause, typically running a value from a Stream message.
 *  But sometimes windows can run a value from an expression, or formula. 
 *  For example, a formula to convert KM to Miles:
 *  
 *    FROM number(my_travel_stream.distanceKM * 1.60934) 
 * 
 * 	The SQLWindowSourceCompiled is used as a Class of such compiled formulas. 
 * 
 */

package org.riodb.sql;

import org.riodb.plugin.RioDBStreamMessage;

public interface SQLWindowSourceCompiled {
	public String getString(RioDBStreamMessage message, RioDBStreamMessage previousMessage)
			throws ExceptionSQLExecution;

	public double getNumber(RioDBStreamMessage message, RioDBStreamMessage previousMessage)
			throws ExceptionSQLExecution;

	public void loadIn(SQLStringIN inArr[]);

	public void loadLike(SQLStringLIKE likeArr[]);
}
