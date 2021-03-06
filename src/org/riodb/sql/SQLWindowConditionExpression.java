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

public class SQLWindowConditionExpression implements SQLWindowCondition {

	private String expression;
	private SQLWindowConditionCompiled compiledCondition;

	SQLWindowConditionExpression(String expression, int streamId, SQLStringLIKE[] likeArr, SQLStringIN[] inArr,
			String originalExpression) throws ExceptionSQLStatement {

		this.expression = originalExpression;
		String className = "CompiledWindowCondition" + RioDB.rio.getEngine().counterNext();

		String source = "package org.riodb.sql;\r\n" 
				+ "import org.riodb.plugin.RioDBStreamMessage;\r\n"
				+ "import org.riodb.sql.SQLStringIN;\r\n" 
				+ "import org.riodb.sql.SQLStringLIKE;\r\n";

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
		source = source + "public class " + className 
				+ " implements SQLWindowConditionCompiled {\r\n";

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
					//"		RioDB.rio.getSystemSettings().getLogger().trace(\"\\tloadLike(): \"+ s);\r\n"+
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
				//"		RioDB.rio.getSystemSettings().getLogger().trace(\"\\tloadIn(): \"+ s);\r\n"+
				"	}\r\n";
		} else {
			source = source + 
				"	public void loadIn(SQLStringIN inArr[]) {}\r\n"; 
		}

		source = source + "	@Override\r\n"
				+ "	public boolean match(RioDBStreamMessage message,RioDBStreamMessage previousMessage) throws ExceptionSQLExecution {\r\n"
				
				// use TRY to catch exceptions like divide by 0
				+ "		try {\r\n"
				+ "			return "+ expression +";\r\n"
				+ "		} catch (java.lang.ArithmeticException e) { \r\n"
				+ "			throw new ExceptionSQLExecution(\"Arithmetic Exception. Devide by 0 or rouding overflow.\");\r\n" 
				+ "		}\r\n" 
			
				+ "	}\r\n" 
				+ "}\r\n";
		
		//System.out.println(source);

		try {

			@SuppressWarnings("unchecked")
			Class<SQLWindowConditionCompiled> newClass = (Class<SQLWindowConditionCompiled>) InMemoryJavaCompiler
					.newInstance().compile("org.riodb.sql." + className, source.toString());

			compiledCondition = (SQLWindowConditionCompiled) newClass.getDeclaredConstructor().newInstance();

			if (inArr.length > 0) {
				compiledCondition.loadIn(inArr);
			}
			if (likeArr.length > 0) {
				compiledCondition.loadLike(likeArr);
			}

			RioDB.rio.getSystemSettings().getLogger().trace("    compiled " + className);

		} catch (Exception e) {
			RioDB.rio.getSystemSettings().getLogger().debug(
					"Error compiling dynamic class: [" + expression + "] " + e.getMessage().replace("\n", "\\n"));
			throw new ExceptionSQLStatement("Error evaluating query conditions.");
		}

	}

	@Override
	public boolean match(RioDBStreamMessage message, RioDBStreamMessage previousMessage) throws ExceptionSQLExecution  {
		return compiledCondition.match(message, previousMessage);
	}

	@Override
	public String getExpression() {
		return expression;
	}

}
