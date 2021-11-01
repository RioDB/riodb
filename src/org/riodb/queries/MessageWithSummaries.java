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

	Wrapper for containing array of windowSummaries and message together.
	
	Message data and window summary data get wrapped up together into one
	object and sent to the queries. Then, queries can reference values
	from the message and values from the window. 
	
	The reason they have to be bundled together is because they go into
	a blocking queue of objects, and the queue of objects accept only one
	single object. 

 */

package org.riodb.queries;

import org.riodb.windows.WindowSummary;

import org.riodb.plugin.RioDBStreamMessage;
import org.riodb.sql.SQLParser;

public class MessageWithSummaries {
	// The message object
	private RioDBStreamMessage message;
	// the array of window summaries
	private WindowSummary windowSummaries[];

	// constructor
	public MessageWithSummaries(RioDBStreamMessage message, WindowSummary[] windowSummaries){
		this.message = message;
		this.windowSummaries = windowSummaries;
	}

	// get a reference of the message
	public RioDBStreamMessage getMessageRef() {
		return message;
	}

	// get a reference of the windowSummary array
	public WindowSummary[] getWindowSummariesRef() {
		return windowSummaries;
	}
	
	// a print function for troubleshooting
	public void printMessage() {
		
		String s = "--- message:";
		for(int i = 0; i < message.getDoubleFieldsCount(); i++) {
			s = s + "  "+ message.getDouble(i) + ",";
		}
		for(int i = 0; i < message.getStringFieldsCount(); i++) {
			s = s + "  "+ SQLParser.decodeText(message.getString(i)) + ",";
		}
		System.out.println(s);
	}
}
