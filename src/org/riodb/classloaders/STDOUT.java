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

package org.riodb.classloaders;

import org.riodb.plugin.*;

final public class STDOUT implements RioDBOutput {

	public String getType() {
		return "STDOUT";
	}

	public void init(String outputParams, String[] columnHeaders) throws RioDBPluginException {
		// no init required for STDOUT
	}

	final public void send(String[] columns) {

		String output = "";
		for (String c : columns) {
			output = output + c + "\t";
		}
		System.out.println(output.substring(0, output.length() - 1)); // remove last tab
	}

	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(1);
	}

}
