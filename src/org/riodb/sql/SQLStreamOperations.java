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

import java.util.LinkedHashMap;

import org.riodb.engine.RioDB;
import org.riodb.engine.Stream;

import org.riodb.plugin.RioDBPluginException;
import org.riodb.plugin.RioDBStreamEventDef;
import org.riodb.plugin.RioDBStreamEventField;

public final class SQLStreamOperations {
	
	
	
	public static final boolean createStream(String stmt) throws ExceptionSQLStatement, RioDBPluginException {

		boolean success = true;

		String newStmt = SQLStreamOperations.formatSQL(stmt);

		String streamName = SQLStreamOperations.getStreamName(newStmt);
		
		if (streamName == null || streamName.length() == 0) {
			RioDB.rio.getSystemSettings().getLogger().error("ERROR: Stream must have a name and at least one field.");
			return false;
		}

		LinkedHashMap<String, String> map = SQLStreamOperations.getFields(newStmt);
		
		
		if (map.size() > 0) {

			RioDBStreamEventDef def = new RioDBStreamEventDef();

			int timestampNumericFieldId = -1;
			
			int numericFieldCounter = 0;

			for (String key : map.keySet()) {
				boolean isNumber = false;
				if (map.get(key) != null && (map.get(key).equals("number") || map.get(key).equals("timestamp"))) {
					isNumber = true;
					if(map.get(key).equals("timestamp")) {
						if(timestampNumericFieldId >= 0) {
							throw new ExceptionSQLStatement("The stream can only have one 'timestamp' field. Others can be put in as 'number'.");
						}
						timestampNumericFieldId = numericFieldCounter;
					}
					numericFieldCounter++;
				}
				RioDBStreamEventField f = new RioDBStreamEventField(key, isNumber);
				def.addField(f);
			}
			
			def.setTimestampNumericFieldId(timestampNumericFieldId);

			String listenerType = SQLStreamOperations.getListenerType(newStmt);
			String listenerParams = SQLStreamOperations.getListenerParams(newStmt);
			
			Stream newStream = new Stream(RioDB.rio.getEngine().getStreamCount(), streamName, def, listenerType, listenerParams);

			RioDB.rio.getEngine().addStream(newStream);
			
			if(RioDB.rio.getEngine().isOnline()) {
				newStream.start();
			}

		} else {
			throw new ExceptionSQLStatement("SQL ERROR: CREATE STREAM . needs field declaration.");
		}

		return success;

	}

	public static final boolean dropStream(String stmt) throws ExceptionSQLStatement {

		String newStmt = SQLStreamOperations.formatSQL(stmt);
		
		String words[] = newStmt.split(" ");
		if (words.length >= 3 && words[0] != null && words[0].equals("drop") && words[1] != null
				&& words[1].equals("stream") && words[2] != null && words[2].length() > 0) {

			int streamId = RioDB.rio.getEngine().getStream(words[2]);

			if (streamId >= 0) {
				return RioDB.rio.getEngine().removeStream(streamId);
			}
		}
		return false;
	}

	
	/*
	 *    STRING FUNCTIONS
	 */
	
	public static final String formatSQL(String stmt)  throws ExceptionSQLStatement {
		
		if(stmt == null) {
			 throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(1,stmt));
		}
		String newStmt = stmt.trim(); //.toLowerCase();
		
		newStmt = newStmt.replace("\t"," ");
		newStmt = newStmt.replace("\n"," ");
		newStmt = newStmt.replace("\r"," ");
		newStmt = newStmt.replace(")"," ) ");
		newStmt = newStmt.replace("(", " ( ");
		while(newStmt.contains("  ")) {
			newStmt = newStmt.replace("  ", " ");
		}
		newStmt = newStmt.trim();
		
		if(newStmt.charAt(newStmt.length()-1) != ';') {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(2,stmt));
		}
		else {
			newStmt = newStmt.substring(0,newStmt.length()-1) + " ;";
		}
		
		// count parenthesis:
		int op = 0;
		int cp = 0;
		for(int i = 0; i < newStmt.length()-1; i++) {
			if(newStmt.charAt(i) == '(') {
				op++;
			}
			else if(newStmt.charAt(i) == ')') {
				cp++;
			}
		}
		if(op > cp) {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(5,stmt));
		}
		else if(op < cp) {
			throw new ExceptionSQLStatement(SQLStmtErrorMsg.write(6,stmt));
		}
		return newStmt;
	}

	public static final String getStreamName(String stmt) throws ExceptionSQLStatement {
		
		String newStmt = formatSQL(stmt);
		
		String words[] = newStmt.split(" ");
		
		if (words.length < 6) {
			throw new ExceptionSQLStatement("SQL ERROR: CREATE STREAM . needs more info.");
		}
		if(words[0]!= null && words[0].equals("create") &&
				words[1] != null && words[1].equals("stream")) {
			return words[2];
		}
		return null;
	}
	
	public static final LinkedHashMap<String,String> getFields(String stmt) throws ExceptionSQLStatement {
		LinkedHashMap<String,String> map = new LinkedHashMap<String,String>();
		String newStmt = formatSQL(stmt);
		int fromIndex = newStmt.indexOf("(")+1;
		if (fromIndex > 0) {
			int toIndex = newStmt.indexOf(")",fromIndex);
			if(toIndex > fromIndex) {
				String subs = newStmt.substring(fromIndex,toIndex);
				
				String fields[] = subs.split(",");
				for(int i = 0; i < fields.length; i++) {
					fields[i] = fields[i].trim();
					if(fields[i].contains(" ")) {
						String parts[] = fields[i].split(" ");
						map.put(parts[0],parts[1]);
					}
				}	
			}
			else {
				throw new ExceptionSQLStatement("SQL ERROR: expected close parenthesis.");
			}
		}
		else {
			throw new ExceptionSQLStatement("SQL ERROR: Create Stream missing field declaration.");
		}
		
		return map;
	}
	
	public static final String getListenerType(String stmt) throws ExceptionSQLStatement {
		String newStmt = formatSQL(stmt);
		
		if(newStmt == null || !newStmt.contains(" using ")) {
			throw new ExceptionSQLStatement("SQL ERROR: Create Stream missing 'using' keyword.");
		}
		
		String listenerType = newStmt.substring(newStmt.indexOf(" using ")+ 7).trim();
		listenerType = listenerType.substring(0, listenerType.indexOf(" ")).trim();
		
		return listenerType;
	}
	
	public static final String getListenerParams(String stmt) throws ExceptionSQLStatement {
		String newStmt = formatSQL(stmt);
		
		if(newStmt == null || !newStmt.contains(" using ")) {
			throw new ExceptionSQLStatement("SQL ERROR: Create Stream missing 'using' keyword.");
		}
		
		String listenerParams = newStmt.substring(newStmt.indexOf(" using ")+ 7).trim();
		listenerParams = listenerParams.substring(listenerParams.indexOf("(")+1).trim();
		listenerParams = listenerParams.substring(0, listenerParams.lastIndexOf(")")).trim();
		
		return listenerParams;
	}
	
	public static final String getWhere(String stmt) throws ExceptionSQLStatement {
		
		String where = "-";
		
		int fromIndex = stmt.indexOf(" from ");
		
		int whereIndex = stmt.indexOf(" where ", fromIndex);
		if(whereIndex == -1)
			whereIndex = stmt.indexOf(" where(", fromIndex);
		
		if (whereIndex > fromIndex) {
			whereIndex = whereIndex + 6;
			if(stmt.indexOf(" having",whereIndex)>whereIndex) {
				where = stmt.substring(whereIndex,stmt.indexOf(" having ",whereIndex));
			}
			else if(stmt.indexOf(" partition ")>whereIndex) {
				where = stmt.substring(whereIndex,stmt.indexOf(" partition by ",whereIndex));
			}
			else if(stmt.indexOf(" limit ")>whereIndex) {
				where = stmt.substring(whereIndex,stmt.indexOf(" limit ",whereIndex));
			}
			else if(stmt.indexOf(" timeout ")>whereIndex) {
				where = stmt.substring(whereIndex,stmt.indexOf(" timeout ",whereIndex));
			}
			else {
				where = stmt.substring(whereIndex,stmt.indexOf(";",whereIndex));
			}
		}
		
		return where.trim();
	}

}
