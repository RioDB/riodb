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

import org.riodb.classloaders.OutputClassLoader;
import org.riodb.engine.RioDB;
import org.riodb.plugin.RioDBPlugin;
import org.riodb.plugin.RioDBPluginException;

public final class SQLQueryOutputOperations {

	static final RioDBPlugin getOutput(String outputStr, String[] columnHeaders) throws ExceptionSQLStatement {
		
		String pluginName = "";
		String outputParams = "";
		if(outputStr == null || outputStr.length() == 0) {
			throw new ExceptionSQLStatement("Received empty Query Output string.");
		}
		else if(outputStr.indexOf('(') > 0) {
			pluginName   = outputStr.substring(0,outputStr.indexOf('(')).replace(" ","");
			outputParams = outputStr.substring(outputStr.indexOf('(')+1, outputStr.lastIndexOf(')'));
			outputParams = outputParams.trim();
			outputParams = BASE64Utils.decodeQuotedText(outputParams);
			//System.out.println("pluginName:"+ pluginName);
			//System.out.println("outputParams:"+ outputParams);
		}
		else {
			pluginName = outputStr;
		}
		
		RioDBPlugin newOutput = OutputClassLoader.getOutputPlugin(pluginName);
		
		try {
			newOutput.initOutput(outputParams, columnHeaders);
		} catch (RioDBPluginException e) {
			RioDB.rio.getSystemSettings().getLogger().error(e.getMessage());
			ExceptionSQLStatement s = new ExceptionSQLStatement("Error loading output plugin");
			s.setStackTrace(e.getStackTrace());
			throw s;
		}
		
		return newOutput;
	}
}
