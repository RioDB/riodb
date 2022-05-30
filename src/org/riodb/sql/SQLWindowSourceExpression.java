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

import org.riodb.plugin.RioDBStreamMessage;

public class SQLWindowSourceExpression {

	private String expression;
	private SQLWindowSourceCompiled compiledSource;
	private boolean requiresPrevious;
	
	SQLWindowSourceExpression(String expression, int streamId, String originalExpression, String stringOrNumber, boolean requiresPrevious,SQLStringLIKE[] likeArr, SQLStringIN[] inArr) 
			throws ExceptionSQLStatement {

		this.expression = originalExpression;
		this.requiresPrevious = requiresPrevious;
		
		String stringExpression = "null";
		String numberExpression = "Double.NaN";
		
		
		if(stringOrNumber != null && stringOrNumber.equals("string")) {
			stringExpression = expression;
		} else if(stringOrNumber != null && stringOrNumber.equals("number")) {
			numberExpression = expression;
		} else {
			throw new ExceptionSQLStatement("The compiler for window source expression could not determine if the expression results in a Number or in a String.");
		}
		
		
		String className = "CompiledWindowSource"+ RioDB.rio.getEngine().counterNext();
		
		String source = "package org.riodb.sql;\r\n" + 
				"import org.riodb.plugin.RioDBStreamMessage;\r\n"+
				"import org.riodb.sql.SQLStringIN;\r\n"+
				"import org.riodb.sql.SQLStringLIKE;\r\n";

		if (expression != null && expression.contains("SQLScalarFunctions.")) {
			source = source + "import org.riodb.sql.SQLScalarFunctions;\r\n";
		}
		if (expression != null && expression.contains("Math.")) {
			source = source + "import java.lang.Math;\r\n";
		}
			source = source + 
				"public class "+ className +" implements SQLWindowSourceCompiled {\r\n";
				
				
			if(likeArr.length > 0) {
				source += 	
						"	SQLStringLIKE likeList[];\r\n"+
						"	public void loadLike(SQLStringLIKE likeArr[]) {\r\n" + 
						"		this.likeList = likeArr;\r\n" + 
						//"		String s = \"\";\r\n" + 
						//"		for(int i = 0; i < likeArr.length; i++) {\r\n" + 
						//"			s = s + likeArr[i].getElements();\r\n" + 
						//"			if(i < likeArr.length-2)\r\n" + 
						//"				s = s + \" | \";\r\n" + 
						//"		}\r\n"+
						//"		RioDB.rio.getSystemSettings().getLogger().trace(\"loadLike(): \"+ s);\r\n"+
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
					//"		String s = \"\";\r\n" + 
					//"		for(int i = 0; i < inArr.length; i++) {\r\n" + 
					//"			s = s + inArr[i].getElements();\r\n" + 
					//"			if(i < inArr.length-2)\r\n" + 
					//"				s = s + \" | \";\r\n" + 
					//"		}\r\n"+
					//"		RioDB.rio.getSystemSettings().getLogger().trace(\"loadIn(): \"+ s);\r\n"+
					"	}\r\n";
			} else {
				source = source + 
					"	public void loadIn(SQLStringIN inArr[]) {}\r\n"; 
			}
				

				
				source = source+
				"	@Override\r\n" + 
				"	public String getString(RioDBStreamMessage message,RioDBStreamMessage previousMessage) {\r\n" + 
				"		return "+ stringExpression +";\r\n" + 
				"	}\r\n" + 

				"	@Override\r\n" + 
				"	public double getNumber(RioDBStreamMessage message,RioDBStreamMessage previousMessage) {\r\n" + 
				"		return "+ numberExpression +";\r\n" + 
				"	}\r\n" + 

				"}\r\n" + 
				"";
		
				//System.out.println(source);
				
		try {
			
			RioDB.rio.getSystemSettings().getLogger().trace("    compiled class "+className);
			
			@SuppressWarnings("unchecked")
			Class<SQLWindowSourceCompiled> newClass = (Class<SQLWindowSourceCompiled>) InMemoryJavaCompiler.newInstance().compile("org.riodb.sql."+ className , source.toString());

			compiledSource = (SQLWindowSourceCompiled) newClass.getDeclaredConstructor().newInstance();
			
			
		} catch (Exception e) {
			RioDB.rio.getSystemSettings().getLogger().debug("Error compiling dynamic class for expression: ["+expression + "] " + e.getMessage().replace("\n", "\\n"));
			throw new ExceptionSQLStatement("Error evaluating window source (FROM...)");
		}

	}

	public String getString(RioDBStreamMessage message, RioDBStreamMessage previousMessage) {
		return compiledSource.getString(message, previousMessage);
	}
	
	public double getNumber(RioDBStreamMessage message, RioDBStreamMessage previousMessage) {
		return compiledSource.getNumber(message, previousMessage);
	}

	public String getExpression() {
		return expression;
	}
	
	public boolean requiresPrevious() {
		return requiresPrevious;
	}
	
}
