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

import org.riodb.windows.WindowSummary;

import org.riodb.plugin.RioDBStreamEvent;

public class SQLQueryConditionNumber implements SQLQueryCondition {

//	int streamId;
	private int windowId;
	private int functionId;
	private int operator;
	// 0 >
	// 1 >=
	// 2 <
	// 3 <=
	// 4 =
	// 5 !=
	// is null
	// is not null
	private double threshhold;
	private String expression;

	SQLQueryConditionNumber(int windowId, int functionId, String operatorIn, double threshhold, String expression)
			throws ExceptionSQLStatement {

		if (operatorIn != null) {
			if (operatorIn.equals(">")) {
				this.operator = 0;
			} else if (operatorIn.equals(">=")) {
				this.operator = 1;
			} else if (operatorIn.equals("<")) {
				this.operator = 2;
			} else if (operatorIn.equals("<=")) {
				this.operator = 3;
			} else if (operatorIn.equals("=")) {
				this.operator = 4;
			} else if (operatorIn.equals("!=")) {
				this.operator = 5;
			} else if (operatorIn.equals(" is null")) {
				this.operator = 6;
			} else if (operatorIn.equals(" is not null")) {
				this.operator = 7;
			} else {
				throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(72, operatorIn));
			}

		} else {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(73, operatorIn));
		}

		this.windowId = windowId;
		this.functionId = functionId;
		this.threshhold = threshhold;
		this.expression = expression;

	}

	@Override
	public boolean match(RioDBStreamEvent event, WindowSummary[] summaries) throws ExceptionSQLExecution {
		if (summaries[windowId] != null) {
			if (functionId == 0)
				return matchFunction(summaries[windowId].getAvg());
			if (functionId == 1)
				return matchFunction(summaries[windowId].getCount());
			if (functionId == 2)
				return matchFunction(summaries[windowId].getCountDistinct());
			// if(functionId == 3)
			// return matchFunction(summaries[windowId].getCountIf());
			if (functionId == 4)
				return matchFunction(summaries[windowId].getFirst());
			if (functionId == 5)
				return matchFunction(summaries[windowId].getLast());
			if (functionId == 6)
				return matchFunction(summaries[windowId].getMax());
			if (functionId == 7)
				return matchFunction(summaries[windowId].getMedian());
			if (functionId == 8)
				return matchFunction(summaries[windowId].getMin());
			if (functionId == 9)
				return matchFunction(summaries[windowId].getMode());
			if (functionId == 10)
				return matchFunction(summaries[windowId].getPopulationStdDev());
			if (functionId == 11)
				return matchFunction(summaries[windowId].getPopulationVariance());
			if (functionId == 12)
				return matchFunction(summaries[windowId].getPrevious());
			if (functionId == 13)
				return matchFunction(summaries[windowId].getSampleStdDev());
			if (functionId == 14)
				return matchFunction(summaries[windowId].getSampleVariance());
			if (functionId == 15)
				return matchFunction(summaries[windowId].getSlope());
			if (functionId == 16)
				return matchFunction(summaries[windowId].getSum());
			// if(functionId == 17)
			// return matchFunction(summaries[windowId].getSumIf());
		}
		return false;
	}

	/*
	 * private boolean matchFunction(float val) { if(operator == 0) { return val >
	 * threshhold; } else if(operator == 1) { return val >= threshhold; } else
	 * if(operator == 2) { return val < threshhold; } else if(operator == 3) {
	 * return val <= threshhold; } else if(operator == 4) { return val ==
	 * threshhold; } else if(operator == 5) { return val != threshhold; } else
	 * if(operator == 6) { // NaN comparisons are inverted.. != means ==, and ==
	 * means !=. return val != Float.NaN; } else if(operator == 7) { // NaN
	 * comparisons are inverted.. != means ==, and == means !=. return val ==
	 * Float.NaN; } return false; }
	 * 
	 * private boolean matchFunctions(int val) { if(operator == 0) { return val >
	 * threshhold; } else if(operator == 1) { return val >= threshhold; } else
	 * if(operator == 2) { return val < threshhold; } else if(operator == 3) {
	 * return val <= threshhold; } else if(operator == 4) { return val ==
	 * threshhold; } else if(operator == 5) { return val != threshhold; } else
	 * if(operator == 6) { // NaN comparisons are inverted.. != means ==, and ==
	 * means !=. return val != Float.NaN; } else if(operator == 7) { // NaN
	 * comparisons are inverted.. != means ==, and == means !=. return val ==
	 * Float.NaN; } return false; }
	 */
	private boolean matchFunction(double val) {
		if (operator == 0) {
			return val > threshhold;
		} else if (operator == 1) {
			return val >= threshhold;
		} else if (operator == 2) {
			return val < threshhold;
		} else if (operator == 3) {
			return val <= threshhold;
		} else if (operator == 4) {
			return val == threshhold;
		} else if (operator == 5) {
			return val != threshhold;
		} else if (operator == 6) {
			// NaN comparisons are inverted.. != means ==, and == means !=.
			return val != Float.NaN;
		} else if (operator == 7) {
			// NaN comparisons are inverted.. != means ==, and == means !=.
			return val == Float.NaN;
		}
		return false;
	}

	@Override
	public String getExpression() {
		return expression;
	}
}
