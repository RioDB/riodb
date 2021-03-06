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
import org.riodb.sql.SQLAggregateFunctions;

public class WindowOfOne implements Window {

	// Window Summary is WindowSummaryOfOne
	private final WindowSummaryOfOne windowSummary = new WindowSummaryOfOne();
	
	// Needed if wrapper uses partitioned windows 
	private int partitionExpiration;
	
	// Last timestamp
	private int lastEntryTime;
	
	// previous flag
	private boolean requiresPrevious;

	// Constructor
	public WindowOfOne(boolean requiresPrevious, int partitionExpiration) {

		this.requiresPrevious = requiresPrevious;
		this.partitionExpiration = partitionExpiration;
		
		RioDB.rio.getSystemSettings().getLogger().debug("\tconstructing Window of one slot");
		
	}
	
	// Public procedure to add Element to Window
	private void add(double elementInserted) {

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
	public double getWindowLast() {
		return windowSummary.getLast();
	}

	// get a deep copy
	@Override
	public WindowSummaryOfOne getWindowSummaryCopy() {
		return new WindowSummaryOfOne(windowSummary);
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
	public Window makeEmptyClone() {
		return new WindowOfOne(requiresPrevious, partitionExpiration);
	}

	// for debugging. prints to screen.
	@Override 
	public void printElements() {
		System.out.print("element " + windowSummary.getLast());
		
	}

	// requires any aggregate function?
	@Override
	public boolean requiresFunction(int functionId) {
		if(functionId == SQLAggregateFunctions.getFunctionId("avg") ||
				functionId == SQLAggregateFunctions.getFunctionId("avg") ||
				functionId == SQLAggregateFunctions.getFunctionId("count") ||
				functionId == SQLAggregateFunctions.getFunctionId("count_distinct") ||
				functionId == SQLAggregateFunctions.getFunctionId("first") ||
				functionId == SQLAggregateFunctions.getFunctionId("last") ||
				functionId == SQLAggregateFunctions.getFunctionId("max") ||
				functionId == SQLAggregateFunctions.getFunctionId("median") ||
				functionId == SQLAggregateFunctions.getFunctionId("min") ||
				functionId == SQLAggregateFunctions.getFunctionId("mode") ||
				functionId == SQLAggregateFunctions.getFunctionId("slope") ||
				functionId == SQLAggregateFunctions.getFunctionId("sum") ||
				functionId == SQLAggregateFunctions.getFunctionId("variance")) {
			return true;
		}
		if(functionId == SQLAggregateFunctions.getFunctionId("previous")) {
			return requiresPrevious;
		}
		return false;		
	}

	// a wrapper function that adds an element and returns the windowSummary
	@Override
	public WindowSummaryInterface trimAddAndGetWindowSummaryCopy(double element, int currentSecond) {
		// currentSecond is not applicable for windowOfOne. Only here to satisfy interface. 		
		add(element);
		return getWindowSummaryCopy();
	}
	
	// When a window does NOT match its condition, but should still expire entries
	@Override
	public WindowSummaryOfOne trimAndGetWindowSummaryCopy(int currentSecond) {
		if(partitionExpiration > 0) {
			lastEntryTime = currentSecond;
		}
		return new WindowSummaryOfOne(windowSummary);
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
