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

package org.riodb.windows;

public interface Window {
	// minimum time to wait before dropping expired partitions. 
	public static final int GRACE_PERIOD = 60;
	// get list of aggregations
	public String getAggregations();
	// gets the window size of the window (which is not the element count() )
	public int getRange();
	// get count of elements
	public int getWindowCount();
	// get last element inserted
	public double getWindowLast();
	// get a clone of the WindowSummary
	public WindowSummaryInterface getWindowSummaryCopy();
	// check if window is a partition due for expiration;
	public boolean isDueForExpiration(int currentSecond);
	// if window has no elements
	public boolean isEmpty();
	// if window of quantity is full, or window of time has reached eviction age
	public boolean isFull();
	// Makes a copy of the window, but fresh from start. 
	// used to start new empty windows when partitioning windows by a key. 
	public Window makeFreshClone();
	// print function for debugging
	public void printElements();
	// checks if this window requires a function:
	public boolean requiresFunction(int functionId);
	// add element with timestamp, trim expired, and get a clone of the WindowSummary
	public WindowSummaryInterface trimAddAndGetWindowSummaryCopy(double element, int currentSecond);
	// trim expired and get clone of the WindowSummary
	public WindowSummaryInterface trimAndGetWindowSummaryCopy(int currentSecond);
	// function to evict expired elements (effective on windowOfTime* only)
	public int trimExpiredWindowElements(int currentSecond);
}
