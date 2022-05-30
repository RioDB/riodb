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

package org.riodb.sql;

import java.util.ArrayList;
import java.util.TreeSet;

import org.riodb.engine.RioDB;

final public class SQLWindowSourceOperations {
	
 static final SQLWindowSourceExpression getWindowSource(String fromStr, int drivingStreamId, boolean whereClauseRequiresPrevious) throws ExceptionSQLStatement {
		
	 
	 	if (fromStr == null) {
	 		return null;
	 	}
		
		String sqlExpression = fromStr;
		RioDB.rio.getSystemSettings().getLogger().trace("\tFROM: " + fromStr);
		
		String stringOrNumber;
		if(sqlExpression.startsWith("number (")) {
			stringOrNumber = "number";
		} else if(sqlExpression.startsWith("string (")) {
			stringOrNumber = "string";
		} else {
			throw new ExceptionSQLStatement("FROM expression must be casted as 'number()' or 'string()'");
		}
		
		sqlExpression = sqlExpression.substring(8);
		sqlExpression = sqlExpression.substring(0, sqlExpression.lastIndexOf(")"));
		sqlExpression = sqlExpression.trim();
		
		//System.out.println("expression 1: "+ sourceExpression);
		
		boolean requiresPrevious = whereClauseRequiresPrevious;
		if (sqlExpression.contains(" previous ")) {
			requiresPrevious = true;
		}
		
		// ArrayLists of StringLike and StringIn objects if needed
		ArrayList<SQLStringLIKE> likeList = new ArrayList<SQLStringLIKE>();
		ArrayList<SQLStringIN> inList = new ArrayList<SQLStringIN>();

		// get the list of all required Windows for this query condition
		TreeSet<Integer> requiredWindows = new TreeSet<Integer>();

		// Not used, but we can share same procedure for JavaGenerator
		SQLQueryResources queryResources = null;
		
		String javaExpression = JavaGenerator.convertSqlToJava(sqlExpression, queryResources, drivingStreamId, likeList, inList, requiredWindows);
		
		
		RioDB.rio.getSystemSettings().getLogger().trace("\tcompiled: " + javaExpression);
		
		SQLStringLIKE[] likeArr = new SQLStringLIKE[likeList.size()];
		likeArr = likeList.toArray(likeArr);

		SQLStringIN[] inArr = new SQLStringIN[inList.size()];
		inArr = inList.toArray(inArr);

		return new SQLWindowSourceExpression(javaExpression, drivingStreamId, fromStr, stringOrNumber, requiresPrevious, likeArr, inArr);
	}	
}
