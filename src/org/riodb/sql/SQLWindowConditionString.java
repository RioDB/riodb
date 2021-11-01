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

public class SQLWindowConditionString implements SQLWindowCondition{

	
	//private int columnId;
	private int messageStringIndex;
	private int operator;
/*	
 * 0 =
 * 1 !=
 * 2 like
 * 3 not like
 * 4 is null
 * 5 is not null
 * 6 in
 * 7 not in
 * 8 >
 * 9 >=
 * 10 <
 * 11 <=
 */
	private boolean firstHalf;
	private String pattern;
	private SQLStringLIKE likeObj;
	private SQLStringIN inObj;
	private String expression;

	SQLWindowConditionString(int streamId, int columnId, String operator, String patternStr, String expression)
	throws ExceptionSQLStatement {
		
		//this.columnId = columnId;
		this.messageStringIndex = RioDB.rio.getEngine().getStream(streamId).getDef().getStringFieldIndex(columnId); 
		this.pattern = SQLParser.decodeQuotedText(patternStr);
		this.expression = expression;
		
		if(pattern!=null)
			pattern = pattern.replace("''", "'");
		
		pattern = pattern.trim();
		
		if(pattern.startsWith("'"))
			pattern = pattern.substring(1);

		if(pattern.endsWith("'"))
			pattern = pattern.substring(0, pattern.length()-1);
		
		if(operator!= null) {
			if(operator.equals("=")) {
				this.operator = 0;
			}
			else if(operator.equals("!=")) {
				this.operator = 1;
			}
			else if(operator.equals(" like ")) {
				
				this.operator = 2;
				likeObj = new SQLStringLIKE(pattern);
				
			}
			else if(operator.equals(" not like ")) {

				this.operator = 3;
				likeObj = new SQLStringLIKE(pattern);

			}
			else if(operator.equals(" is null")) {
				this.operator = 4;
			}
			else if(operator.equals(" is not null")) {
				this.operator = 5;
			}
			else if(operator.equals(" in ")) {
				this.operator = 6;
				inObj = new SQLStringIN(pattern);
			}
			else if(operator.equals(" not in ")) {
				this.operator = 7;
				inObj = new SQLStringIN(pattern);
			}
			else if(operator.equals(">")) {
				this.operator = 8;
			}
			else if(operator.equals(">=")) {
				this.operator = 9;
			}
			else if(operator.equals("<")) {
				this.operator = 10;
			}
			else if(operator.equals("<=")) {
				this.operator = 11;
			}
			else {
				throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(34,operator));
			}
			
			firstHalf = true;
			if(this.operator >= 6)
				firstHalf = false;
			
			//System.out.println("Stream "+streamId + " comlumn "+this.columnId + " stringIndex "+ this.messageStringIndex + " operator "+ this.operator + " pattern "+ this.pattern);
		}
	}

	@Override
	public boolean match(RioDBStreamMessage message) {
		
		if(message.getString(messageStringIndex) == null || message.getString(messageStringIndex).equals("")) {
			if(operator == 4) {
				return true;
			}
			else if(operator == 5) {
				return true;
			}
			else {
				return false;
			}
		}
		if(firstHalf) {
			if(operator == 0 ) {
				return message.getString(messageStringIndex).compareTo(pattern) == 0;
			}
			else if(operator == 1 ) {
				return message.getString(messageStringIndex).compareTo(pattern) != 0;
			}
			else if(operator == 2 ) {
				// like
				return likeObj.match(message.getString(messageStringIndex));
			}
			else if(operator == 3 ) {
				// not like
				return !likeObj.match(message.getString(messageStringIndex));
			}
			else {
				// operator can only be 6
				return inObj.match(message.getString(messageStringIndex));
			}
		}
		else {
			if(operator == 7 ) {
				return !inObj.match(message.getString(messageStringIndex));
			}
			else if(operator == 8 ) {
				return message.getString(messageStringIndex).compareTo(pattern) > 0;
			}
			else if(operator == 9 ) {
				return message.getString(messageStringIndex).compareTo(pattern) >= 0;
			}
			else if(operator == 10 ) {
				return message.getString(messageStringIndex).compareTo(pattern) < 0;
			}
			else {
				// operator can only be 11
				return message.getString(messageStringIndex).compareTo(pattern) <= 0;
			}
		
		}
		/*	
		 * 0 =
		 * 1 !=
		 * 2 like
		 * 3 not like
		 * 4 is null
		 * 5 is not null
		 * 6 in
		 * 7 not in
		 * 8 >
		 * 9 >=
		 * 10 <
		 * 11 <=
		 */
	}

	@Override
	public String getExpression() {
		return expression;
	}

}
