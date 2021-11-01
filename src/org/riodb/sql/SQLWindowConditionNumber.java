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

import org.riodb.engine.RioDB;

import org.riodb.plugin.RioDBStreamMessage;

public class SQLWindowConditionNumber implements SQLWindowCondition{

	private int messageDoubleIndex;
	private int operator;
	// 0 >
	// 1 >=
	// 2 <
	// 3 <=
	// 4 =
	// 5 !=
	// is null
	// is not null
	private float threshhold;

	private String expression; 
	
	SQLWindowConditionNumber(int streamId, int columnId, String operatorIn, float threshhold, String expression)
			throws ExceptionSQLStatement{

		if(operatorIn!= null) {
			if(operatorIn.equals(">")) {
				this.operator = 0;
			}
			else if(operatorIn.equals(">=")) {
				this.operator = 1;
			}
			else if(operatorIn.equals("<")) {
				this.operator = 2;
			}
			else if(operatorIn.equals("<=")) {
				this.operator = 3;
			}
			else if(operatorIn.equals("=")) {
				this.operator = 4;
			}
			else if(operatorIn.equals("!=")) {
				this.operator = 5;
			} 
			else if(operatorIn.equals(" is null")) {
				this.operator = 6;
			} 
			else if(operatorIn.equals(" is not null")) {
				this.operator = 7;
			} 
			else {
				throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(32,operatorIn));
			}
			
		} else {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(33,operatorIn));
		}
		
		this.threshhold = threshhold;
		this.messageDoubleIndex = RioDB.rio.getEngine().getStream(streamId).getDef().getNumericFieldIndex(columnId); 
		this.expression = expression;

	}
	
	@Override
	public boolean match(RioDBStreamMessage message) throws ExceptionSQLExecution {
		if(operator == 0) {
			return message.getDouble(messageDoubleIndex) > threshhold;
		}
		else if(operator == 1) {
			return message.getDouble(messageDoubleIndex) >= threshhold;
		}
		else if(operator == 2) {
			return message.getDouble(messageDoubleIndex) < threshhold;
		}
		else if(operator == 3) {
			return message.getDouble(messageDoubleIndex) <= threshhold;
		}
		else if(operator == 4) {
			return message.getDouble(messageDoubleIndex) == threshhold;
		}
		else if(operator == 5) {
			return message.getDouble(messageDoubleIndex) != threshhold;
		}
		else if(operator == 6) {
			// NaN comparisons are inverted.. != means ==, and == means !=. 
			return message.getDouble(messageDoubleIndex) != Double.NaN;
		}
		else if(operator == 7) {
			// NaN comparisons are inverted.. != means ==, and == means !=. 
			return message.getDouble(messageDoubleIndex) == Float.NaN;
		}
		return false;
	}

	@Override
	public String getExpression() {
		return expression;
	}

}
