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

public class WindowOfOne implements Window {

	private final WindowSummaryOfOne windowSummary = new WindowSummaryOfOne();
	
	// partitionExpiration - to expire stale partitions. 
	private int partitionExpiration;
	private int lastEntryTime;
	
	private boolean requiresPrevious;

	// Constructor
	public WindowOfOne(boolean requiresPrevious, int partitionExpiration) {

		this.requiresPrevious = requiresPrevious;
		this.partitionExpiration = partitionExpiration;
		
		RioDB.rio.getSystemSettings().getLogger().debug("constructing Window of one slot");
		
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
		String s = "[\"average\",\"count\",\"count_distinct\",\"first\",\"last\",\"max\",\"median\",\"min\",\"mode\"";
		
		if(requiresPrevious)
			s = s + ",\"previous\"";
	
		s = s + "\"slope\",\"sum\",\"variance\"]";
		
		return s;
	}

	@Override
	public int getRange() {
		return 1;
	}

	@Override
	public int getWindowCount() {
		return windowSummary.getCount();
	}

	@Override
	public double getWindowLast() {
		return windowSummary.getLast();
	}

	@Override
	public WindowSummaryOfOne getWindowSummaryCopy() {
		return new WindowSummaryOfOne(windowSummary);
	}
	
	@Override
	public boolean isEmpty() {
		return windowSummary.isEmpty();
	}

	@Override
	public boolean isFull() {
		return windowSummary.isFull();
	}

	@Override
	public Window makeFreshClone() {
		return new WindowOfOne(requiresPrevious, partitionExpiration);
	}

	@Override // for debugging. prints to screen. 
	public void printElements() {
		System.out.print("element " + windowSummary.getLast());
		
	}

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
	// this saves a subsequent call to hashmap.get(window).get(windowsummary)
	@Override
	public WindowSummaryInterface trimAddAndGetWindowSummaryCopy(double element, int currentSecond) {
		// currentSecond is not applicable for windowOfOne. Only here to satisfy interface. 		
		add(element);
		return getWindowSummaryCopy();
	}
	
	@Override
	public WindowSummaryOfOne trimAndGetWindowSummaryCopy(int currentSecond) {
		if(partitionExpiration > 0) {
			lastEntryTime = currentSecond;
		}
		return new WindowSummaryOfOne(windowSummary);
	}
	
	@Override
	public int trimExpiredWindowElements(int currentSecond) {
		return 0;
	}

	@Override
	public boolean isDueForExpiration(int currentSecond) {
		if(currentSecond - lastEntryTime > Window.GRACE_PERIOD) {
			return true;
		}
		return false;
	}
	
}
