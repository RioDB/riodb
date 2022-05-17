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

	An interface to describe what windows should be made like

*/
package org.riodb.windows;

public interface Window_String {
	// minimum time to wait before dropping expired partitions. 
	public static final int GRACE_PERIOD = 60;
	// get list of aggregations
	public String getAggregations();
	// gets the window Range. Example: 1000s-100s
	public String getRange();
	// get count of elements
	public int getWindowCount();
	// get last element inserted
	public String getWindowLast();
	// get a clone of the WindowSummary
	public WindowSummaryInterface_String getWindowSummaryCopy();
	// check if window is a partition due for expiration;
	public boolean isDueForExpiration(int currentSecond);
	// if window has no elements
	public boolean isEmpty();
	// if window of quantity is full, or window of time has reached eviction age
	public boolean isFull();
	// Makes a copy of the window, but fresh from start. 
	// used to start new empty windows when partitioning windows by a key. 
	public Window_String makeEmptyClone();
	// print function for debugging
	public void printElements();
	// checks if this window requires a function:
	public boolean requiresFunction(int functionId);
	// trim expired, add element with timestamp,and get a clone of the WindowSummary
	public WindowSummaryInterface_String trimAddAndGetWindowSummaryCopy(String element, int currentSecond);
	// trim expired and get clone of the WindowSummary
	// used when a window does NOT match its query condition, but should still trim expired entries
	public WindowSummaryInterface_String trimAndGetWindowSummaryCopy(int currentSecond);
	// function to evict expired elements (effective on windowOfTime* only)
	public void trimExpiredWindowElements(int currentSecond);
}
