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
 * Windows don't have string values, only numeric. 
 * So for the HAVING condition on a string field, it must pertain to a field from the event. 
 * we just wrap this and pass to the SQLWhereConditionString (for event string conditions)
 * 
 */

package org.riodb.sql;
import org.riodb.windows.WindowSummary;

import org.riodb.plugin.RioDBStreamEvent;

public class SQLQueryConditionString implements SQLQueryCondition{

	private SQLWindowConditionString strCondition;
	
	SQLQueryConditionString(int streamId, int columnId, String operator, String patternStr, String expression)
	throws ExceptionSQLStatement {
		strCondition = new SQLWindowConditionString(streamId, columnId, operator, patternStr, expression);
	}

	@Override
	public boolean match(RioDBStreamEvent event, WindowSummary[] summaries) {
		
		return strCondition.match(event);
	}

	@Override
	public String getExpression() {
		return strCondition.getExpression();
	}
}
