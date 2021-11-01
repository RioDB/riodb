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

/*
	Output plugin class STDOUT is built-in in RioDB.
	It's used to output selected (matched) data to the console where the
	RioDB process is running. 
	
	When users submit query to output STDOUT, RioDB does not load
	and external jar file, since STDOUT is already part of RioDB
*/

package org.riodb.classloaders;

import org.riodb.engine.RioDB;
import org.riodb.plugin.*;

final public class STDOUT implements RioDBPlugin {

	// the delimiter between fields:
	String delimiter = "\t";
	
	@Override
	public String getType() {
		return "STDOUT";
	}

	@Override
	public void initOutput(String outputParams, String[] columnHeaders) throws RioDBPluginException {
		String params[] = outputParams.split(" ");
		if(params.length == 2 && params[0].equals("delimiter")) {
			delimiter = params[1].trim().replace("'", "");
		}
	}

	@Override
	final public void sendOutput(String[] columns) {

		// basically, just print each of the columns selected onto console. 
		String output = "";
		for (String c : columns) {
			output = output + c + delimiter;
		}
		System.out.println(output.substring(0, output.length() - delimiter.length())); // remove last tab
	}

	@Override
	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(1);
	}

	@Override
	public RioDBStreamMessage getNextInputMessage() throws RioDBPluginException {
		return null;
	}

	@Override
	public int getQueueSize() {
		return 0;
	}

	@Override
	public void initInput(String inputParams, RioDBStreamMessageDef def) throws RioDBPluginException {
		throw new RioDBPluginException("STDOUT cannot be used as INPUT.");
	}

	@Override
	public void start() throws RioDBPluginException {
	}

	@Override
	public void stop() throws RioDBPluginException {
	}

	@Override
	public String version() {
		return RioDB.VERSION;
	}

}
