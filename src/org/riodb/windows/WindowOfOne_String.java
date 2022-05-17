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
 *   Window of ONE single element
 *   
 *   When when an element is inserted, it replaces the previous element. 
 *   
 */

package org.riodb.windows;

import org.riodb.engine.RioDB;
import org.riodb.sql.SQLFunctionMap;

public class WindowOfOne_String implements Window_String {

	// Window Summary is WindowSummaryOfOne
	private final WindowSummaryOfOne_String windowSummary = new WindowSummaryOfOne_String();
	
	// Needed if wrapper uses partitioned windows 
	private int partitionExpiration;
	
	// Last timestamp
	private int lastEntryTime;
	
	// previous flag
	private boolean requiresPrevious;

	// Constructor
	public WindowOfOne_String(boolean requiresPrevious, int partitionExpiration) {

		this.requiresPrevious = requiresPrevious;
		this.partitionExpiration = partitionExpiration;
		
		RioDB.rio.getSystemSettings().getLogger().debug("\tconstructing Window (String) of one slot");
		
	}
	
	// Public procedure to add Element to Window
	private void add(String elementInserted) {

		if(requiresPrevious) {
			windowSummary.setPrevious(windowSummary.getLast());
		}
		if(windowSummary.isEmpty())
			windowSummary.setFull(true);
		windowSummary.setLast(elementInserted);

	}
	
	
	@Override
	public String getAggregations() {
		// TODO: Loop through SQLFunctionMap ?
		String s = "[\"average\",\"count\",\"count_distinct\",\"first\",\"last\",\"max\",\"median\",\"min\",\"mode\"";
		
		if(requiresPrevious)
			s = s + ",\"previous\"";
	
		s = s + "\"slope\",\"sum\",\"variance\"]";
		
		return s;
	}

	// range is always 1
	@Override
	public String getRange() {
		return "1";
	}

	// get count
	@Override
	public int getWindowCount() {
		return windowSummary.getCount();
	}

	// get last
	@Override
	public String getWindowLast() {
		return windowSummary.getLast();
	}

	// get a deep copy
	@Override
	public WindowSummaryOfOne_String getWindowSummaryCopy() {
		return new WindowSummaryOfOne_String(windowSummary);
	}
	
	// is empty?
	@Override
	public boolean isEmpty() {
		return windowSummary.isEmpty();
	}

	// is full??
	@Override
	public boolean isFull() {
		return windowSummary.isFull();
	}

	// clone a copy in reset state
	@Override
	public Window_String makeEmptyClone() {
		return new WindowOfOne_String(requiresPrevious, partitionExpiration);
	}

	// for debugging. prints to screen.
	@Override 
	public void printElements() {
		System.out.print("element " + windowSummary.getLast());
		
	}

	// requires any aggregate function?
	@Override
	public boolean requiresFunction(int functionId) {
		if(functionId == SQLFunctionMap.getFunctionId("avg") ||
				functionId == SQLFunctionMap.getFunctionId("avg") ||
				functionId == SQLFunctionMap.getFunctionId("count") ||
				functionId == SQLFunctionMap.getFunctionId("count_distinct") ||
				functionId == SQLFunctionMap.getFunctionId("first") ||
				functionId == SQLFunctionMap.getFunctionId("last") ||
				functionId == SQLFunctionMap.getFunctionId("max") ||
				functionId == SQLFunctionMap.getFunctionId("median") ||
				functionId == SQLFunctionMap.getFunctionId("min") ||
				functionId == SQLFunctionMap.getFunctionId("mode") ||
				functionId == SQLFunctionMap.getFunctionId("slope") ||
				functionId == SQLFunctionMap.getFunctionId("sum") ||
				functionId == SQLFunctionMap.getFunctionId("variance")) {
			return true;
		}
		if(functionId == SQLFunctionMap.getFunctionId("previous")) {
			return requiresPrevious;
		}
		return false;		
	}

	// a wrapper function that adds an element and returns the windowSummary
	@Override
	public WindowSummaryInterface_String trimAddAndGetWindowSummaryCopy(String element, int currentSecond) {
		// currentSecond is not applicable for windowOfOne. Only here to satisfy interface. 		
		add(element);
		return getWindowSummaryCopy();
	}
	
	// When a window does NOT match its condition, but should still expire entries
	@Override
	public WindowSummaryOfOne_String trimAndGetWindowSummaryCopy(int currentSecond) {
		if(partitionExpiration > 0) {
			lastEntryTime = currentSecond;
		}
		return new WindowSummaryOfOne_String(windowSummary);
	}
	
	// window of fixed length 1 does not evict elements based on time. 
	// method is only here to satisfy interface
	@Override
	public void trimExpiredWindowElements(int currentSecond) {
		//return 0;
	}

	// check if window is due for expiration
	@Override
	public boolean isDueForExpiration(int currentSecond) {
		if(currentSecond - lastEntryTime > Window.GRACE_PERIOD) {
			return true;
		}
		return false;
	}
	
}
