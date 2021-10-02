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
	
	SQLWindowConditionExpression(String expression, int streamId, SQLStringLIKE[] likeArr, SQLStringIN[] inArr, String originalExpression) 
			throws ExceptionSQLStatement {

		this.expression = originalExpression;
		String className = "CompiledWindowCondition"+ RioDB.rio.getEngine().counterNext();
		
		String source = "package org.riodb.sql;\r\n" + 
				"import org.riodb.plugin.RioDBStreamMessage;\r\n" + 
				"import org.riodb.sql.SQLStringIN;\r\n" + 
				"import org.riodb.sql.SQLStringLIKE;\r\n";
		
				if(expression!=null && expression.contains("Math.")) {
					source = source + 
					"import java.lang.Math;\r\n";
				}
				source = source + 
				"public class "+ className +" implements SQLWindowConditionCompiled {\r\n" + 
				"	SQLStringLIKE likeList[];\r\n" + 
				"	SQLStringIN inList[];\r\n";
		
				source = source +
				"	public void loadLike(SQLStringLIKE likeArr[]) {\r\n" + 
				"		this.likeList = likeArr;\r\n" + 
				"	}\r\n";
				
				source = source + 
				"	public void loadIn(SQLStringIN inArr[]) {\r\n" + 
				"		this.inList = inArr;\r\n" + 
				"	}\r\n";
				
				source = source+
				"	@Override\r\n" + 
				"	public boolean match(RioDBStreamMessage message) {\r\n" + 
				"		return "+ expression +";\r\n" + 
				"	}\r\n" + 
				"}\r\n" + 
				"";
		
		try {
			
			RioDB.rio.getSystemSettings().getLogger().debug("Compiling dynamic class "+className);
			
			@SuppressWarnings("unchecked")
			Class<SQLWindowConditionCompiled> newClass = (Class<SQLWindowConditionCompiled>) InMemoryJavaCompiler.newInstance().compile("org.riodb.sql."+ className , source.toString());

			compiledCondition = (SQLWindowConditionCompiled) newClass.getDeclaredConstructor().newInstance();
			
			if(inArr.length > 0) {
				compiledCondition.loadIn(inArr);
			}
			if(likeArr.length > 0) {
				compiledCondition.loadLike(likeArr);
			}
			
		} catch (Exception e) {
			RioDB.rio.getSystemSettings().getLogger().debug("Error compiling dynamic class: ["+expression + "] " + e.getMessage().replace("\n", "\\n"));
			throw new ExceptionSQLStatement("Error evaluating query conditions.");
		}

	}

	@Override
	public boolean match(RioDBStreamMessage message) {
		return compiledCondition.match(message);
	}

	@Override
	public String getExpression() {
		return expression;
	}
	
}
