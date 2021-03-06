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

import org.mdkt.compiler.InMemoryJavaCompiler;
import org.riodb.engine.RioDB;
import org.riodb.windows.WindowSummary;
import org.riodb.windows.WindowSummary_String;

import org.riodb.plugin.RioDBStreamMessage;

import java.util.Iterator;
import java.util.TreeSet;

public class SQLQueryConditionExpression implements SQLQueryCondition {

	private String expression;
	private SQLQueryConditionCompiled compiledCondition;
	
	SQLQueryConditionExpression(String expression, SQLStringLIKE[] likeArr, SQLStringIN[] inArr, String originalExpression, TreeSet<Integer> requiredWindows) 
			throws ExceptionSQLStatement {

		this.expression = originalExpression;
		String className = "CompiledConditionClass"+ RioDB.rio.getEngine().counterNext();
		
		
		String source = "package org.riodb.sql;\r\n" + 
				"import org.riodb.plugin.RioDBStreamMessage;\r\n" +
				"import org.riodb.engine.RioDB;\r\n"+
				"import org.riodb.windows.WindowSummary;\r\n" + 
				"import org.riodb.windows.WindowSummary_String;\r\n"+
				"import org.riodb.sql.SQLStringIN;\r\n"+
				"import org.riodb.sql.SQLStringLIKE;\r\n";

		if (expression != null && expression.contains("SQLScalarFunctionsReturningNumber.")) {
			source = source + "import org.riodb.sql.SQLScalarFunctionsReturningNumber;\r\n";
		}
		if (expression != null && expression.contains("SQLScalarFunctionsReturningString.")) {
			source = source + "import org.riodb.sql.SQLScalarFunctionsReturningString;\r\n";
		}
		if (expression != null && expression.contains("SQLScalarFunctionsReturningBoolean.")) {
			source = source + "import org.riodb.sql.SQLScalarFunctionsReturningBoolean;\r\n";
		}
		if (expression != null && expression.contains("Math.")) {
			source = source + "import java.lang.Math;\r\n";
		}
		
		source = source + 
				"public class "+ className +" implements SQLQueryConditionCompiled {\r\n";
		
		if(likeArr.length > 0) {
			source += 	
					"	SQLStringLIKE likeList[];\r\n"+
					"	public void loadLike(SQLStringLIKE likeArr[]) {\r\n" + 
					"		this.likeList = likeArr;\r\n" + 
					"		String s = \"\";\r\n" + 
					"		for(int i = 0; i < likeArr.length; i++) {\r\n" + 
					"			s = s + likeArr[i].getElements();\r\n" + 
					"			if(i < likeArr.length-2)\r\n" + 
					"				s = s + \" | \";\r\n" + 
					"		}\r\n"+
					"		RioDB.rio.getSystemSettings().getLogger().debug(\"loadLike(): \"+ s);\r\n"+
					"	}\r\n";
		} else {
			source += 	
					"	public void loadLike(SQLStringLIKE likeArr[]) {}\r\n";
		}
		
		if(inArr.length > 0) {
			source = source + 
				"	SQLStringIN inList[];\r\n"+
				"	public void loadIn(SQLStringIN inArr[]) {\r\n" + 
				"		this.inList = inArr;\r\n" +
				"		String s = \"\";\r\n" + 
				"		for(int i = 0; i < inArr.length; i++) {\r\n" + 
				"			s = s + inArr[i].getElements();\r\n" + 
				"			if(i < inArr.length-2)\r\n" + 
				"				s = s + \" | \";\r\n" + 
				"		}\r\n"+
				"		RioDB.rio.getSystemSettings().getLogger().debug(\"loadIn(): \"+ s);\r\n"+
				"	}\r\n";
		} else {
			source = source + 
				"	public void loadIn(SQLStringIN inArr[]) {}\r\n"; 
		}
		
		source = source+
				"	@Override\r\n" + 
				"	public boolean match(RioDBStreamMessage message, WindowSummary[] windowSummaries, WindowSummary_String[] windowSummaries_String) throws ExceptionSQLExecution {\r\n";
				
				
				Iterator<Integer> iterator = requiredWindows.iterator(); 
		        while (iterator.hasNext()) {
		        	int windowId = Integer.valueOf(String.valueOf(iterator.next()));
		        	
		        	if(windowId >= 0) {
		        		source = source + 
		        		        "		if(  windowSummaries["+ String.valueOf(windowId) +"] == null)  \r\n"+
		        		        "			return false;\r\n";	
		        	} else {
		        		source = source + 
		        		        "		if(  windowSummaries_String["+ String.valueOf((windowId + 1) * -1) +"] == null)  \r\n"+
		        		        "			return false;\r\n";
		        	}
		        	
		        }

				source = source +
				"\r\n"
				+ "		try {\r\n"
				+ "			return "+ expression +";\r\n"
				+ "		} catch (java.lang.ArithmeticException e) { \r\n"
				+ "			throw new ExceptionSQLExecution(\"Arithmetic Exception. Devide by 0 or rouding overflow.\");\r\n" 
				+ "		}\r\n" 
				+ "	}\r\n"  
				+ "}\r\n" 
				+ "";
				
				
		try {
			
			RioDB.rio.getSystemSettings().getLogger().debug("Compiling dynamic class "+className);
			
			//System.out.println(source);
			
			@SuppressWarnings("unchecked")
			Class<SQLQueryConditionCompiled> newClass = (Class<SQLQueryConditionCompiled>) InMemoryJavaCompiler.newInstance().compile("org.riodb.sql."+ className , source.toString());

			compiledCondition = (SQLQueryConditionCompiled) newClass.getDeclaredConstructor().newInstance();
			
			if(inArr.length > 0) {
				compiledCondition.loadIn(inArr);
			}
			if(likeArr.length > 0) {
				compiledCondition.loadLike(likeArr);
			}
			
		} catch (Exception e) {
			throw new ExceptionSQLStatement("Error compiling dynamic class ["+ expression +"]:\n"+e.getMessage());
		}

	}

	@Override
	public boolean match(RioDBStreamMessage message, WindowSummary[] windowSummaries, WindowSummary_String[] windowSummaries_String) throws ExceptionSQLExecution {
		return compiledCondition.match(message, windowSummaries, windowSummaries_String);
	}

	@Override
	public String getExpression() {
		return expression;
	}
	
}
