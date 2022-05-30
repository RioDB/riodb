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
 *  user selects a constant value, like :
 *  SELECT 'my_value' FROM ...
 * 
 */

package org.riodb.sql;

import org.riodb.windows.WindowSummary;
import org.riodb.windows.WindowSummary_String;
import org.riodb.plugin.RioDBStreamMessage;

public class SQLQueryColumnConstant implements SQLQueryColumn {

	private String constant;
	private String heading;

	SQLQueryColumnConstant(String constant, String heading) throws ExceptionSQLStatement {
		this.constant = BASE64Utils.decodeQuotedText(constant);
		this.heading = BASE64Utils.decodeQuotedText(heading);
		
		if(this.constant != null && this.constant.startsWith("'") && this.constant.endsWith("'") ) {
			this.constant = this.constant.substring(1, this.constant.length()-1);
		}
		//TODO: use decodeText()
	}

	@Override
	public String getValue(RioDBStreamMessage message, WindowSummary[] windowSummaries, WindowSummary_String[] windowSummaries_String) throws ExceptionSQLExecution {
		return constant;
	}

	@Override
	public String getHeading() {
		return heading;
	}

}
