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

	Wrapper for containing array of windowSummaries and event together.
	
	Event data and window summary data get wrapped up together into one
	object and sent to the queries. Then, queries can reference values
	from the event and values from the window. 
	
	The reason they have to be bundled together is because they go into
	a blocking queue of objects, and the queue of objects accept only one
	single object. 

 */

package org.riodb.queries;

import org.riodb.windows.WindowSummary;

import org.riodb.plugin.RioDBStreamEvent;

public class EventWithSummaries {
	// The event object
	private RioDBStreamEvent event;
	// the array of window summaries
	private WindowSummary windowSummaries[];

	// constructor
	public EventWithSummaries(RioDBStreamEvent event, WindowSummary[] windowSummaries){
		this.event = event;
		this.windowSummaries = windowSummaries;
	}

	// get a reference of the event
	public RioDBStreamEvent getEventRef() {
		return event;
	}

	// get a reference of the windowSummary array
	public WindowSummary[] getWindowSummariesRef() {
		return windowSummaries;
	}
}
