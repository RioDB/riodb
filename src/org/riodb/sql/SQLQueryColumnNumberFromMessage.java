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
 *   user selects a Double value from the Stream  message:
 *   SELECT myStream.myStringField  from myStream... 
 */
package org.riodb.sql;

import org.riodb.windows.WindowSummary;
import org.riodb.windows.WindowSummary_String;
import org.riodb.plugin.RioDBStreamMessage;

public class SQLQueryColumnNumberFromMessage implements SQLQueryColumn {

	private int doubleColumnIndex;
	private String heading;

	SQLQueryColumnNumberFromMessage(int doubleColumnIndex, String heading) throws ExceptionSQLStatement {
		this.doubleColumnIndex = doubleColumnIndex;
		this.heading = heading;
	}

	@Override
	public String getValue(RioDBStreamMessage message, WindowSummary[] windowSummaries,
			WindowSummary_String[] windowSummaries_String) throws ExceptionSQLExecution {
		return String.valueOf(message.getDouble(doubleColumnIndex));
	}

	@Override
	public String getHeading() {
		return heading;
	}

}
