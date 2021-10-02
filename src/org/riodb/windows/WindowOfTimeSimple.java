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
 *    The WindowOfTimeComplex is a window that expires elements based on age. 
 *    But it must keep every individual element in order to calculate complex 
 *    stats such as slope, variance, etc. 
 *    When only simple stats are required (like Count, Sum, etc), and we don't
 *    need to maintain every individual element, then we summarize data into seconds
 *    
 *    In WindowOfTimeSimple, data is summarized into Seconds. So if 500,000 elements
 *    arrive within 1 second, it's summarized into one single mini batch. 
 *    This is a LOT faster, but only if user is willing to accept the rounding to the
 *    nearest second.  
 *    
 *    The class SecondNode is used to store summarized data about that 1 second. 
 *    It's similar to WindowSummary but with fewer bells and whistles. 
 *    
 *    WIndowOfTimeSimple is used when fewer functions are required:
 *       Avg, Count, First, Last, Max, Min, Previous, Sum, Empty, Full
 *    
 *    If other stats like Median, Slope, Distinct are needed, the use WindowOfTimeComplex
 * 
 */

package org.riodb.windows;

import java.util.ArrayDeque;

import org.riodb.engine.RioDB;
import org.riodb.sql.SQLFunctionMap;

public class WindowOfTimeSimple implements Window {

	/// SecondNode summarized stats for everything arrived within a single second
	private class SecondNode {
		private int nodeSecond; // The second that this node captures
		private double nodeMax; // max val received in this second
		private double nodeMin; // min val received in this second
		private double nodeFirst; // first item received in this batch
		private double nodeSum; // sum of all vals in this second
		private int nodeCount; // count of all vals in this second.

		// constructor: the second that the node represents, and the first element
		// inserted
		SecondNode(int second, double elementInserted) {
			this.nodeMax = elementInserted;
			this.nodeMin = elementInserted;
			this.nodeSum = elementInserted;
			this.nodeFirst = elementInserted;
			this.nodeCount = 1;
			this.nodeSecond = second;
		}

		// adding item to second node
		protected void add(double element) {
			if (requiresMax && element > nodeMax) {
				nodeMax = element;
			}
			if (requiresMin && element < nodeMin) {
				nodeMin = element;
			}
			if (requiresSum) {
				nodeSum += element;
			}
			if (requiresCount) {
				nodeCount++;
			}
		}

		protected int getSecond() {
			return nodeSecond;
		}

		protected double getMax() {
			return nodeMax;
		}

		protected double getMin() {
			return nodeMin;
		}

		protected double getFirst() {
			return nodeFirst;
		}

		protected double getSum() {
			return nodeSum;
		}

		protected int getCount() {
			return nodeCount;
		}
	}

	// A list of all SecondNodes
	private ArrayDeque<SecondNode> secondSummaryList;

	// a WindowSummary object to track the current state of this window.
	private WindowSummary windowSummary;
	
	// partitionExpiration - to expire stale partitions. 
	private int partitionExpiration;
	private int lastEntryTime;

	// the last second to receive data, use to rotate to new node when the most
	// recent node becomes old.
	private int mostRecentSecond;

	// window range
	private int range;

	private boolean functionsRequired[];
	// required aggregations:
	private boolean requiresCount;
	private boolean requiresFirst;
	private boolean requiresMax;
	private boolean requiresMin;
	private boolean requiresPrevious;
	private boolean requiresSum;
	
		// constructor
	public WindowOfTimeSimple(int range, boolean[] functionsRequired, int partitionExpiration) {

		this.range = range;
		this.partitionExpiration = partitionExpiration;
		this.functionsRequired = functionsRequired;
		this.requiresCount = functionsRequired[SQLFunctionMap.getFunctionId("count")];
		this.requiresFirst = functionsRequired[SQLFunctionMap.getFunctionId("first")];
		this.requiresMax = functionsRequired[SQLFunctionMap.getFunctionId("max")];
		this.requiresMin = functionsRequired[SQLFunctionMap.getFunctionId("min")];
		this.requiresPrevious = functionsRequired[SQLFunctionMap.getFunctionId("previous")];
		this.requiresSum = functionsRequired[SQLFunctionMap.getFunctionId("sum")];
		
		RioDB.rio.getSystemSettings().getLogger().debug("constructing Window of time, simple");
		// list = new LinkedList<SecondNode>();
		secondSummaryList = new ArrayDeque<SecondNode>();
		windowSummary = new WindowSummary();
		mostRecentSecond = 0;
	}
	
	@Override
	public Window makeFreshClone() {
		return new WindowOfTimeSimple(range, functionsRequired, partitionExpiration);
	}

	@Override
	public WindowSummary trimAddAndGetWindowSummaryCopy(double element, int currentSecond) {
		trimExpiredWindowElements(currentSecond);
		add(element, currentSecond);
		return getWindowSummaryCopy();
	}

	private void add(double elementInserted, int currentSecond) {

		// check if the latest "Second" is the current second.
		if (mostRecentSecond == currentSecond) {
			secondSummaryList.getLast().add(elementInserted);
		}
		// else, time has moved on to a new second.
		else {
			// new second on the clock
			// timestamp
			if(partitionExpiration > 0) {
				lastEntryTime = currentSecond;
			}
			// make new Node
			SecondNode s = new SecondNode(currentSecond, elementInserted);
			secondSummaryList.add(s);
		}

		// we only bother with updating max and min if a rule needs them.
		if (requiresMax && elementInserted > windowSummary.getMax()) {
			windowSummary.setMax(elementInserted);
		}
		if (requiresMin && elementInserted < windowSummary.getMin()) {
			windowSummary.setMin(elementInserted);
		}
		if (requiresCount) {
			windowSummary.incrementCount();
		}
		if (requiresSum) {
			windowSummary.sumAdd(elementInserted);
		}
		if (requiresPrevious) {
			windowSummary.setPrevious(windowSummary.getLast());
		}
		if (requiresFirst && windowSummary.getCount() == 1) {
			windowSummary.setFirst(elementInserted);
		}

		windowSummary.setLast(elementInserted);

	}
	
	
	// TODO need to synchronize so that QUERY calls don't clash with CLOCK calls. 
	// If sync performance is bad, then maybe have CLOCK pass request as a special message through message process. 
	@Override
	public int trimExpiredWindowElements(int currentSecond) {

		// if the oldest record is current, then there's nothing to remove. 
		if(mostRecentSecond == currentSecond) {
			return 0;
		}
		
		int expirationTime = currentSecond - range;
		
		// if the newest record is old enough to be evicted, then we might as well 
		// evict everything by resetting the window. 
		if(secondSummaryList.peekLast().getSecond() < expirationTime ) {
			int totalRemoved = windowSummary.getCount();
			secondSummaryList = new ArrayDeque<SecondNode>();
			WindowSummary newEmptyWindow = new WindowSummary();
			if(requiresPrevious) {
				newEmptyWindow.setPrevious(windowSummary.getPrevious());
			}
			windowSummary = newEmptyWindow;
			return totalRemoved;
		}

		mostRecentSecond = currentSecond;
		int count = 0;
		boolean maxRemoved = false;
		boolean minRemoved = false;
		
		
		// evict expired entries in secondList, starting from oldest to newest. 
		for (SecondNode sn : secondSummaryList) {
			if (sn.getSecond() < expirationTime) {
				if (requiresSum) {
					windowSummary.sumSubtract((double) sn.getSum());
				}
				if (requiresMax && sn.getMax() == windowSummary.getMax()) {
					maxRemoved = true;
				}
				if (requiresMin && sn.getMin() == windowSummary.getMin()) {
					minRemoved = true;
				}
				if (requiresCount) {
					windowSummary.setCount(windowSummary.getCount() - sn.getCount());
				}
				secondSummaryList.poll();
				count++;
			} else { // end loop
				break;
			}
		}
		// if everything was removed entirely, windowSummary can be reset to brand new WindowSummary:
		if (secondSummaryList == null || secondSummaryList.size() == 0) {
			windowSummary = new WindowSummary();
		} else if (count > 0) {
			if (maxRemoved || minRemoved) {

				double max = secondSummaryList.peek().getMax();
				double min = secondSummaryList.peek().getMin();
				for (SecondNode entry : secondSummaryList) {
					if (requiresMax && entry.getMax() > max) {
						max = entry.getMax();
					}
					if (requiresMin && entry.getMin() < min) {
						min = entry.getMin();
					}
				}
				if (requiresMax) {
					windowSummary.setMax(max);
				}
				if (requiresMin) {
					windowSummary.setMin(min);
				}
			}

			if (requiresFirst) {
				windowSummary.setFirst(secondSummaryList.peek().getFirst());
			}

			if (!windowSummary.isFull()) {
				windowSummary.setFull(true);
			}

		}
		return count;
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
	public WindowSummary trimAndGetWindowSummaryCopy(int currentSecond) {
		trimExpiredWindowElements(currentSecond);
		return getWindowSummaryCopy();
	}
	
	@Override
	public WindowSummary getWindowSummaryCopy() {
		return new WindowSummary(windowSummary);
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
	public void printElements() {
		String s = "nodes getCount(): ";
		for (SecondNode entry : secondSummaryList) {
			s = s + ", " + entry.getCount();
		}

	}

	@Override
	public String getAggregations() {
		return SQLFunctionMap.getFunctionsAvailable(functionsRequired);
	}
	
	@Override
	public boolean requiresFunction(int functionId) {
		return functionsRequired[functionId];
	}
	
	@Override
	public int getRange() {
		return range;
	}
	
	@Override
	public boolean isDueForExpiration(int currentSecond) {
		if(currentSecond - lastEntryTime > Window.GRACE_PERIOD) {
			return true;
		}
		return false;
	}


}
