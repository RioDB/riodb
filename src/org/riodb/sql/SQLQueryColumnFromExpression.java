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

public class SQLQueryColumnFromExpression implements SQLQueryColumn {

	private SQLQueryColumnCompiled compiledItem;
	private String heading;

	SQLQueryColumnFromExpression(String expression, String heading, SQLStringLIKE[] likeArr, SQLStringIN[] inArr) throws ExceptionSQLStatement {

		this.heading = heading;
		
		String className = "CompiledSelectClass" + RioDB.rio.getEngine().counterNext();

		String source = "package org.riodb.sql;\r\n" + 
		          "import org.riodb.plugin.RioDBStreamMessage;\r\n"	+
		          "import org.riodb.windows.WindowSummary;\r\n" +
				  "import org.riodb.windows.WindowSummary_String;\r\n"+
				  "import org.riodb.sql.SQLStringIN;\r\n"+
				  "import org.riodb.sql.SQLStringLIKE;\r\n";
		
		if (expression != null && expression.contains("SQLScalarFunctions.")) {
			source = source + "import org.riodb.sql.SQLScalarFunctions;\r\n";
		}
		if (expression != null && expression.contains("Math.")) {
			source = source + "import java.lang.Math;\r\n";
		}
		source = source + "public class " + className + " implements SQLQueryColumnCompiled {\r\n";
		
		
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

		source = source + "	@Override\r\n" + 
		"	public String getValue(RioDBStreamMessage message, WindowSummary[] windowSummaries, WindowSummary_String[] windowSummaries_String) throws ExceptionSQLExecution {\r\n	"+
		"	return String.valueOf("	+ expression + ");\r\n" + 
		"	}\r\n" + "}";
		
		try {

			RioDB.rio.getSystemSettings().getLogger().debug("Compiling dynamic class " + className);

			//System.out.println("\n\n\n"+ source + "\n\n\n");
			@SuppressWarnings("unchecked")
			Class<SQLQueryColumnCompiled> newClass = (Class<SQLQueryColumnCompiled>) InMemoryJavaCompiler.newInstance()
					.compile("org.riodb.sql." + className, source.toString());

			compiledItem = (SQLQueryColumnCompiled) newClass.getDeclaredConstructor().newInstance();

		} catch (Exception e) {
			RioDB.rio.getSystemSettings().getLogger().debug("Error compiling dynamic class: ["+expression + "] " + e.getMessage().replace("\n", "\\n"));
			throw new ExceptionSQLStatement("Error evaluating column expression.");
		}

	}

	@Override
	public String getValue(RioDBStreamMessage message, WindowSummary[] windowSummaries, WindowSummary_String[] windowSummaries_String) throws ExceptionSQLExecution {
		return compiledItem.getValue(message, windowSummaries, windowSummaries_String);
	}

	@Override
	public String getHeading() {
		return heading;
	}
}