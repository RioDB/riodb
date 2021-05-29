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

import org.riodb.plugin.RioDBStreamEvent;

public class SQLQueryColumnFromExpression implements SQLQueryColumn {

	private SQLQueryColumnCompiled compiledItem;
	private String heading;

	SQLQueryColumnFromExpression(String expression, String heading) throws ExceptionSQLStatement {

		this.heading = heading;
		
		String className = "CompiledSelectClass" + RioDB.rio.getEngine().counterNext();

		String source = "package org.riodb.sql;\r\n" + 
		          "import org.riodb.plugin.RioDBStreamEvent;\r\n"	+
				  "import org.riodb.windows.WindowSummary;\r\n";

		if (expression != null && expression.contains("Math.")) {
			source = source + "import java.lang.Math;\r\n";
		}
		source = source + "public class " + className + " implements SQLQueryColumnCompiled {\r\n";

		source = source + "	@Override\r\n" + 
		"	public String getValue(RioDBStreamEvent event, WindowSummary[] windowSummaries) throws ExceptionSQLExecution {\r\n	"+
		"	return String.valueOf("	+ expression + ");\r\n" + 
		"	}\r\n" + "}";
		
		try {

			RioDB.rio.getLogger().debug("Compiling dynamic class " + className);

			@SuppressWarnings("unchecked")
			Class<SQLQueryColumnCompiled> newClass = (Class<SQLQueryColumnCompiled>) InMemoryJavaCompiler.newInstance()
					.compile("org.riodb.sql." + className, source.toString());

			compiledItem = (SQLQueryColumnCompiled) newClass.getDeclaredConstructor().newInstance();

		} catch (Exception e) {
			System.out.println(e.getMessage());
			RioDB.rio.getLogger().debug("Error compiling dynamic class: ["+expression + "] " + e.getMessage().replace("\n", "\\n"));
			throw new ExceptionSQLStatement("Error evaluating query conditions.");
		}

	}

	@Override
	public String getValue(RioDBStreamEvent event, WindowSummary[] windowSummaries) throws ExceptionSQLExecution {
		return compiledItem.getValue(event, windowSummaries);
	}

	@Override
	public String getHeading() {
		return heading;
	}
}