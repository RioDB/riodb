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
import org.riodb.sql.SQLAggregateFunctions;

public class WindowOfTimeSimple_String implements Window_String {

	/// SecondNode summarized stats for everything arrived within a single second
	private class SecondNode {
		private int nodeSecond; // The second that this node captures
		private String nodeMax; // max val received in this second
		private String nodeMin; // min val received in this second
		private String nodeFirst; // first item received in this batch
		private String nodeLast; // first item received in this batch
		private String nodePrevious; // element before Last

		private int nodeCount; // count of all vals in this second.

		// constructor: the second that the node represents, and the first element
		// inserted
		SecondNode(int second, String elementInserted) {
			this.nodeMax = elementInserted;
			this.nodeMin = elementInserted;
			this.nodeFirst = elementInserted;
			this.nodeLast = elementInserted;
			this.nodeCount = 1;
			this.nodeSecond = second;
			this.nodePrevious = null;
			// should never hit NaN because getPrevious is only called
			// when nodeCount > 1
		}

		// adding item to second node
		protected void add(String element) {

			if (requiresMax && element.compareTo(nodeMax) > 0) {
				nodeMax = element;
			}
			if (requiresMin && element.compareTo(nodeMin) < 0) {
				nodeMin = element;
			}
			if (requiresCount) {
				nodeCount++;
			}
			if (requiresPrevious) {
				nodePrevious = nodeLast;
			}
			this.nodeLast = element;

		}

		protected int getSecond() {
			return nodeSecond;
		}

		protected String getMax() {
			return nodeMax;
		}

		protected String getMin() {
			return nodeMin;
		}

		protected String getFirst() {
			return nodeFirst;
		}

		protected String getLast() {
			return nodeLast;
		}

		protected String getPrevious() {
			return nodePrevious;
		}

		protected int getCount() {
			return nodeCount;
		}
	}

	// A list of all SecondNodes in window
	private ArrayDeque<SecondNode> batchedWindowQueue;

	// A list of all SecondNodes waiting in queue
	// used when range has endtime.
	// exmaple: range 100s-10s has to wait for 10s
	private ArrayDeque<SecondNode> batchedWaitingQueue;

	// a WindowSummary object to track the current state of this window.
	private WindowSummary_String windowSummary;

	// partitionExpiration - to expire stale partitions.
	private int partitionExpiration;
	private int lastEntryTime;

	// the last second to receive data, use to rotate to new node when the most
	// recent node becomes old.
	private int mostRecentSecond;

	// window range
	private int rangeStart;
	private int rangeEnd;
	private boolean hasRangeEnd;

	private boolean functionsRequired[];
	// required aggregations:
	private boolean requiresCount;
	private boolean requiresFirst;
	private boolean requiresMax;
	private boolean requiresMin;
	private boolean requiresPrevious;
	
	// constructor
	public WindowOfTimeSimple_String(int rangeStart, int rangeEnd, boolean[] functionsRequired, int partitionExpiration) {

		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;
		this.hasRangeEnd = false;
		if (rangeEnd > 0) {
			this.hasRangeEnd = true;
			// initialize waiting Queue.
			batchedWaitingQueue = new ArrayDeque<SecondNode>();
		}

		this.partitionExpiration = partitionExpiration;
		this.functionsRequired = functionsRequired;
		this.requiresCount = functionsRequired[SQLAggregateFunctions.getFunctionId("count")];
		this.requiresFirst = functionsRequired[SQLAggregateFunctions.getFunctionId("first")];
		this.requiresMax = functionsRequired[SQLAggregateFunctions.getFunctionId("max")];
		this.requiresMin = functionsRequired[SQLAggregateFunctions.getFunctionId("min")];
		this.requiresPrevious = functionsRequired[SQLAggregateFunctions.getFunctionId("previous")];

		RioDB.rio.getSystemSettings().getLogger().debug("\tconstructing Window (String) of time, simple, for Strings");
		// list = new LinkedList<SecondNode>();
		batchedWindowQueue = new ArrayDeque<SecondNode>();
		windowSummary = new WindowSummary_String();
		mostRecentSecond = 0;
	}

	@Override
	public Window_String makeEmptyClone() {
		return new WindowOfTimeSimple_String(rangeStart, rangeEnd, functionsRequired, partitionExpiration);
	}

	@Override
	public WindowSummary_String trimAddAndGetWindowSummaryCopy(String element, int currentSecond) {

		trimExpiredWindowElements(currentSecond);

		// if waiting queue is not being used
		if (!hasRangeEnd) {
			// add new element to window queue
			add(element, currentSecond);
		}
		// else (waiting queue is being used
		else {
			// add new item to tail of waiting queue
			// check if the latest "Second" is the current second.
			if (mostRecentSecond == currentSecond && !batchedWaitingQueue.isEmpty()) {
				batchedWaitingQueue.getLast().add(element);
			}
			// else, time has moved on to a new second.
			else {
				// new second on the clock
				// mark lastEntryTime
				if (partitionExpiration > 0) {
					lastEntryTime = currentSecond;
				}
				// make new Node
				SecondNode s = new SecondNode(currentSecond, element);
				batchedWaitingQueue.add(s);
				mostRecentSecond = currentSecond;
			}
		}

		return getWindowSummaryCopy();
	}

	private void add(String elementInserted, int currentSecond) {

		// if NOT empty...
		if (!batchedWindowQueue.isEmpty()) {
			// check if the latest "Second" is the current second.
			if (mostRecentSecond == currentSecond) {
				batchedWindowQueue.getLast().add(elementInserted);
			}
			// else, time has moved on to a new second.
			else {
				// new second on the clock
				// mark lastEntryTime
				if (partitionExpiration > 0) {
					lastEntryTime = currentSecond;
				}
				// make new Node
				SecondNode s = new SecondNode(currentSecond, elementInserted);
				batchedWindowQueue.add(s);
				mostRecentSecond = currentSecond;
			}

			// we only bother with updating max and min if a rule needs them.
			if (requiresMax && elementInserted.compareTo(windowSummary.getMax()) > 0) {
				windowSummary.setMax(elementInserted);
			}
			if (requiresMin && elementInserted.compareTo(windowSummary.getMin()) < 0) {
				windowSummary.setMin(elementInserted);
			}
			if (requiresCount) {
				windowSummary.incrementCount();
			}
			if (requiresPrevious) {
				windowSummary.setPrevious(windowSummary.getLast());
			}
			if (requiresFirst && windowSummary.getCount() == 1) {
				windowSummary.setFirst(elementInserted);
			}
			windowSummary.setLast(elementInserted);

		} else {
			// window is empty. Insert very first
			// IF statements are different because some variables have NaN assigned.
			// new second on the clock
			// mark lastEntryTime
			if (partitionExpiration > 0) {
				lastEntryTime = currentSecond;
			}
			// make new Node
			SecondNode s = new SecondNode(currentSecond, elementInserted);
			batchedWindowQueue.add(s);
			mostRecentSecond = currentSecond;

			// we only bother with updating max and min if a rule needs them.
			if (requiresMax) {
				windowSummary.setMax(elementInserted);
			}
			if (requiresMin) {
				windowSummary.setMin(elementInserted);
			}
			if (requiresCount) {
				windowSummary.incrementCount();
			}
			if (requiresFirst) {
				windowSummary.setFirst(elementInserted);
			}
			windowSummary.setLast(elementInserted);

		}

	}

	// procedure to add a SecondNode to window.
	// This is used one waiting queue needs to bring
	// nodes that are done waiting into the window
	private void add(SecondNode newNode) {

		if (!batchedWindowQueue.isEmpty()) {
			batchedWindowQueue.add(newNode);
		
			// we only bother with updating max and min if a rule needs them.
			if (requiresMax && newNode.getMax().compareTo(windowSummary.getMax()) > 0) {
				windowSummary.setMax(newNode.getMax());
			}
			if (requiresMin && newNode.getMin().compareTo(windowSummary.getMin()) < 0) {
				windowSummary.setMin(newNode.getMin());
			}
			if (requiresCount) {
				windowSummary.incrementCount(newNode.getCount());
			}


			// if new node has more than 1, use newNode previous
			if (requiresPrevious && newNode.getCount() > 1) {
				windowSummary.setPrevious(newNode.getPrevious());
			}
			// if new node only has 1, then previous will be the last from
			// the previous node.
			else {
				windowSummary.setPrevious(windowSummary.getLast());
			}

			windowSummary.setLast(newNode.getLast());
		}
		// Window is empty. 
		// Commands are different because window variables are currently set to NaN
		else {
			batchedWindowQueue.add(newNode);
		
			// we only bother with updating max and min if a rule needs them.
			if (requiresMax) {
				windowSummary.setMax(newNode.getMax());
			}
			if (requiresMin) {
				windowSummary.setMin(newNode.getMin());
			}
			if (requiresCount) {
				windowSummary.incrementCount(newNode.getCount());
			}
			if (requiresFirst) {
				windowSummary.setFirst(newNode.getFirst());
			}
			if (requiresPrevious && newNode.getCount()>1) {
				windowSummary.setPrevious(newNode.getPrevious());
			}
			windowSummary.setLast(newNode.getLast());
		}

	}

	// TODO need to synchronize so that QUERY calls don't clash with CLOCK calls.
	// If sync performance is bad, then maybe have CLOCK pass request as a special
	// message through message process.
	@Override
	public void trimExpiredWindowElements(int currentSecond) {

		int expirationTime = currentSecond - rangeStart;

		// we remove expired entries.

		// for efficiency, we may skip the trouble of looking for expired entries:
		// If the window is empty, there's nothing to remove.
		// if the most recent record was added at the current second, there's nothing to
		// remove.
		// because it would have already been removed.
		if (mostRecentSecond != currentSecond && !batchedWindowQueue.isEmpty()) {

			// if the newest record is old enough to be evicted, then everything is old
			// enough to be evicted. We reset the window to a new empty window
			if (batchedWindowQueue.peekLast().getSecond() <= expirationTime) {
				// int totalRemoved = windowSummary.getCount();
				batchedWindowQueue = new ArrayDeque<SecondNode>();
				WindowSummary_String newEmptyWindow = new WindowSummary_String();
				if (requiresPrevious) {
					newEmptyWindow.setPrevious(windowSummary.getPrevious());
				}
				windowSummary = newEmptyWindow;

			} else {
				// we are only evicting expired elements, and preserving non-expired elements.

				int count = 0;
				boolean maxRemoved = false;
				boolean minRemoved = false;

				// loop to evict expired entries, starting from oldest to newest.
				for (SecondNode sn : batchedWindowQueue) {
					if (sn.getSecond() <= expirationTime) {
						if (requiresMax && sn.getMax().equals(windowSummary.getMax())) {
							maxRemoved = true;
						}
						if (requiresMin && sn.getMin().equals(windowSummary.getMin())) {
							minRemoved = true;
						}
						if (requiresCount) {
							windowSummary.setCount(windowSummary.getCount() - sn.getCount());
						}
						batchedWindowQueue.poll();
						count++;
					} else { // end loop
						break;
					}
				}

				// if everything was removed entirely, windowSummary can be reset to brand new
				// WindowSummary:
				if (batchedWindowQueue == null || batchedWindowQueue.size() == 0) {
					windowSummary = new WindowSummary_String();
				} else if (count > 0) {
					if (maxRemoved || minRemoved) {

						String max = batchedWindowQueue.peek().getMax();
						String min = batchedWindowQueue.peek().getMin();
						for (SecondNode entry : batchedWindowQueue) {
							if (requiresMax && entry.getMax().compareTo(max) > 0) {
								max = entry.getMax();
							}
							if (requiresMin && entry.getMin().compareTo(min) < 0) {
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
						windowSummary.setFirst(batchedWindowQueue.peek().getFirst());
					}

					if (!windowSummary.isFull()) {
						windowSummary.setFull(true);
					}

				}

			}

		}

		// done evicting stuff.
		// IF using waiting queue....
		// move elements from waitingQueue into windowQueue:
		// loop through head of waiting queue dequeuing elements done waiting.
		if (hasRangeEnd && !batchedWaitingQueue.isEmpty()) {

			for (SecondNode waitingSecondNode : batchedWaitingQueue) {

				// element is done waiting when element second + waiting period < current second
				if (waitingSecondNode.getSecond() + rangeEnd <= currentSecond) {
					// remove pair from waiting queue and add to window queue.
					add(batchedWaitingQueue.poll());

				} else {
					// done with items that are done waiting...
					break;
				}
			}
		}

	}

	@Override
	public int getWindowCount() {
		return windowSummary.getCount();
	}

	@Override
	public String getWindowLast() {
		return windowSummary.getLast();
	}

	@Override
	public WindowSummary_String trimAndGetWindowSummaryCopy(int currentSecond) {
		trimExpiredWindowElements(currentSecond);
		return getWindowSummaryCopy();
	}

	@Override
	public WindowSummary_String getWindowSummaryCopy() {
		return new WindowSummary_String(windowSummary);
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
		for (SecondNode entry : batchedWindowQueue) {
			s = s + ", " + entry.getCount();
		}

	}

	@Override
	public String getAggregations() {
		return SQLAggregateFunctions.getFunctionsAvailable(functionsRequired);
	}

	@Override
	public boolean requiresFunction(int functionId) {
		return functionsRequired[functionId];
	}

	@Override
	public String getRange() {
		if (hasRangeEnd) {
			return rangeStart + "-" + rangeEnd;
		}
		return String.valueOf(rangeStart);
	}

	@Override
	public boolean isDueForExpiration(int currentSecond) {
		if (currentSecond - lastEntryTime > Window.GRACE_PERIOD) {
			return true;
		}
		return false;
	}

}
