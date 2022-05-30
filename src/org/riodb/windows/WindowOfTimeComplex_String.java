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
 *   Window of Time Complex
 *   
 *   This is same as WindowOfQuantity, but instead of tracking n elements, 
 *   it tracks elements under a certain age. 
 *   
 *   As elements are inserted and evicted, the window statistics (stored in a WindowSummary object) are updated. 
 *   Each stat is only updated if required by query. Otherwise, their computation is skipped to save time. 
 *   
 *   Eviction happens when elements hit a certain age. 
 *   
 *   Elements are stored in an ArrayDeque of a ValueWithTimestamp. 
 *   
 *   Additional collections like TreeMap and ArrayDeque are used if the query requires Median or CountDistinct.
 *   If the query does not require median, mode or count distinct, then the program should select WintoOfTimeSimple.  
 *   
 */

package org.riodb.windows;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

import org.riodb.engine.RioDB;
import org.riodb.sql.SQLAggregateFunctions;

import java.util.TreeMap;

public class WindowOfTimeComplex_String implements Window_String {

	// Window stats are stored in a summary object for easier passing
	// there's no performance loss versus local variables.
	private WindowSummary_String windowSummary;

	// partitionExpiration - to expire stale partitions.
	private int partitionExpiration;
	private int lastEntryTime;

	// A FIFO queue to hold elements in the window
	// Elements MUST be inserted in chronological order
	private ArrayDeque<ValueWithTimestamp_String> windowQueue;

	// A FIFO queue to hold elements waiting
	// Used only when range has end time, for example:
	// 100s-10s -> Elements must wait 10s before entering
	private ArrayDeque<ValueWithTimestamp_String> waitingQueue;

	// Stores elements ordered for calculating MEDIAN and/or MODE
	// Double is an element value.
	// Integer is the count of how many elements in the window have that value
	private TreeMap<String, Counter> sortedElements;
	private boolean usingSorted;

	// Collection of distinct elements for COUNT_DISTINCT function.
	// is NOT used if sortedElements is already used
	// Double is an element value.
	// Integer is the count of how many elements in the window have that value
	private HashMap<String, Counter> uniqueElements;

	// Count of how many elements equal windowMax
	// private int maxSiblings; // no longer needed

	// Count of how many elements equal windowMin
	// private int minSiblings; // no longer needed

	// Quantity of the mode element
	private int modeQuantity;


	// for debug...
	private int silencedUntil; // gets updated with a future timestamp when a rule violation occurs.

	// last time expired items were evicted
	// private int lastTrim;

	// window range
	private int rangeStart;
	private int rangeEnd;
	private boolean hasRangeEnd;

	private boolean functionsRequired[];
	// required aggregations:
	private boolean requiresCount;
	private boolean requiresCountDistinct;
	private boolean requiresFirst;
	private boolean requiresLast;
	private boolean requiresMax;
	private boolean requiresMin;
	private boolean requiresMode;
	private boolean requiresPrevious;

	// Constructor
	public WindowOfTimeComplex_String(int rangeStart, int rangeEnd, boolean[] functionsRequired, int partitionExpiration) {

		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;
		this.hasRangeEnd = false;
		if (rangeEnd > 0) {
			this.hasRangeEnd = true;
			// initialize waiting Queue.
			waitingQueue = new ArrayDeque<ValueWithTimestamp_String>();
		}

		this.partitionExpiration = partitionExpiration;
		this.functionsRequired = functionsRequired;
		this.requiresCount = functionsRequired[SQLAggregateFunctions.getFunctionId("count")];
		this.requiresCountDistinct = functionsRequired[SQLAggregateFunctions.getFunctionId("count_distinct")];
		this.requiresFirst = functionsRequired[SQLAggregateFunctions.getFunctionId("first")];
		this.requiresLast = functionsRequired[SQLAggregateFunctions.getFunctionId("last")];
		this.requiresMax = functionsRequired[SQLAggregateFunctions.getFunctionId("max")];
		this.requiresMin = functionsRequired[SQLAggregateFunctions.getFunctionId("min")];
		this.requiresMode = functionsRequired[SQLAggregateFunctions.getFunctionId("mode")];
		this.requiresPrevious = functionsRequired[SQLAggregateFunctions.getFunctionId("previous")];

		RioDB.rio.getSystemSettings().getLogger().debug("\tconstructing Window (String) of time, complex");

		windowSummary = new WindowSummary_String();

		// start empty initial stack
		windowQueue = new ArrayDeque<ValueWithTimestamp_String>();

		usingSorted = false;
		// additional collections are initialized as needed
		if (requiresMode) {
			sortedElements = new TreeMap<String, Counter>();
			usingSorted = true;
			uniqueElements = null;
		} else if (requiresCountDistinct) {
			uniqueElements = new HashMap<String, Counter>();
			sortedElements = null;
		} else {
			uniqueElements = null;
			sortedElements = null;
		}

		
		// some variables get default assignment.
		silencedUntil = 0;

		// lastTrim = 0;

	}

	@Override
	public Window_String makeEmptyClone() {
		return new WindowOfTimeComplex_String(rangeStart, rangeEnd, functionsRequired, partitionExpiration);
	}

	// a wrapper function that adds an element and returns the windowSummary
	@Override
	public WindowSummary_String trimAddAndGetWindowSummaryCopy(String element, int currentSecond) {
		// timestamp
		if (partitionExpiration > 0) {
			lastEntryTime = currentSecond;
		}
		trimExpiredWindowElements(currentSecond);

		// Double elementAsDouble = element;
		ValueWithTimestamp_String elementWithTimestamp = new ValueWithTimestamp_String(element, currentSecond);

		// if waiting queue is not being used
		if (!hasRangeEnd) {
			// add new element to window queue
			add(elementWithTimestamp);
		}
		// else (waiting queue is being used
		else {
			// add new item to tail of waiting queue
			waitingQueue.add(elementWithTimestamp);
		}

		return getWindowSummaryCopy();
	}

	// Public procedure to add Element to Window
	// it looks redundant to have the primitive parameters AND the combined object.
	// But since the caller already has both addresses available, might as well use
	// the primitives to reduce unboxing time.
	// the ValueWithTimestamp object will be used for the arrayDequeue.
	private void add(ValueWithTimestamp_String pairInserted) {

		String elementAsString = pairInserted.stringValue();

		/*
		 * functions like Previous, Last, and Variance are pre-calculated before we
		 * start making changes to Sum, Count, etc
		 * 
		 */

		// if Previous is required...
		if (requiresPrevious) {
			windowSummary.setPrevious(windowSummary.getLast());
		}
		// if Last is required...
		if (requiresLast) {
			windowSummary.setLast(pairInserted.stringValue());
		}

		// If queue already has elements
		if (windowQueue.size() >= 1) {

			windowQueue.add(pairInserted);

			// if Count is required...
			if (requiresCount) {
				windowSummary.incrementCount();
			}

			// if MAX is required
			if (requiresMax && pairInserted.stringValue().compareTo(windowSummary.getMax()) > 0) {
				windowSummary.setMax(pairInserted.stringValue());
			}
			// if MIN is required...
			if (requiresMin && pairInserted.stringValue().compareTo(windowSummary.getMin()) < 0) {
				windowSummary.setMin(pairInserted.stringValue());
			}

			// If either Median or Mode is required...
			if (usingSorted) {
				// Very similar to what we did for a full window,
				// except there's no evicted element to remove.
				// If elementInserted is already in the tree, increment it's counter Integer
				Counter c = sortedElements.get(elementAsString);
				if (c != null) {
					c.increment();
					if (requiresMode && c.isGT(modeQuantity)) {
						windowSummary.setMode(pairInserted.stringValue());
						modeQuantity = c.intValue();
					}
				} else {
					sortedElements.put(pairInserted.stringValue(), new Counter());
					if (requiresCountDistinct) {
						windowSummary.setCountDistinct(sortedElements.size());
					}
				}

			}
			// If countDistinct is relying on uniqueElements hashmap:
			// countDistinct use hashmap
			else if (requiresCountDistinct) {
				Counter c = uniqueElements.get(pairInserted.stringValue());
				if (c != null) {
					c.increment();
				} else {
					uniqueElements.put(pairInserted.stringValue(), new Counter());
					windowSummary.setCountDistinct(uniqueElements.size());
				}
			}

		}
		/*
		 * 
		 * We already handled adding new element to window that has elements.
		 * 
		 * This next is for adding the very first element to an empty window
		 */
		else {
			// add first element to arrayDeque
			windowQueue.add(pairInserted);
			// if Count is required...
			if (requiresCount) {
				windowSummary.setCount(1);
			}
			// if First is required...
			if (requiresFirst) {
				windowSummary.setFirst(pairInserted.stringValue());
			}

			// if Max is required...
			if (requiresMax) {
				windowSummary.setMax(pairInserted.stringValue());
				// if (!usingSorted)
				// maxSiblings = 1;
			}
			// if Min is required
			if (requiresMin) {
				windowSummary.setMin(pairInserted.stringValue());
				// if (!usingSorted)
				// minSiblings = 1;
			}
			// if SUM is required
			// if Median is required...
			if (usingSorted) {
				sortedElements.put(pairInserted.stringValue(), new Counter());
				if (requiresMode) {
					windowSummary.setMode(pairInserted.stringValue());
					modeQuantity = 1;
				}
				if (requiresCountDistinct) {
					windowSummary.setCountDistinct(1);
				}
//				printSorted(in);
			} else if (requiresCountDistinct) {
				uniqueElements.put(pairInserted.stringValue(), new Counter());
				windowSummary.setCountDistinct(1);
			}
		
		}
		

//		printSorted();
//		printElements();

	}

	private void resetWindow() {
		// init windowSummary
		windowSummary = new WindowSummary_String();

		// start empty initial stack
		windowQueue = new ArrayDeque<ValueWithTimestamp_String>();

		// additional collections are initialized as needed
		if (usingSorted) {
			sortedElements = new TreeMap<String, Counter>();
		} else if (requiresCountDistinct) {
			uniqueElements = new HashMap<String, Counter>();
		}

	}

	// TODO need to synchronize so that QUERY calls don't clash with CLOCK calls.
	// If sync performance is bad, then maybe have CLOCK pass request as a special
	// message through message process.
	@Override
	public void trimExpiredWindowElements(int currentSecond) {

		// counting how many elements get evicted, to return
		// int count = 0;
		boolean itemsRemoved = false;

		// lastTrim = currentSecond;
		// int expirationTime = lastTrim - range;
		int expirationTime = currentSecond - rangeStart;

		// Window has to be not empty. Else there's nothing to evict
		if (windowQueue.size() > 0) {

			// if the newest element is due for expiration, then everything can go
			if (windowQueue.peekLast().getSecond() <= expirationTime) {
				resetWindow();
			}
			// otherwise, we check from the oldest
			else if (windowQueue.peekFirst().getSecond() <= expirationTime) {

				// in case we are expiring many entries,
				// there's no point in searching for a new max and min in each iteration
				// compute those once at the end once the loop is done. If needed
				boolean maxEvicted = false;
				boolean minEvicted = false;
				boolean modeEvicted = false;

				for (ValueWithTimestamp_String evictingElement : windowQueue) {

					if (evictingElement.getSecond() <= expirationTime) {

						windowQueue.poll().stringValue();

						if (requiresCount) {
							windowSummary.decrementCount();
						}
						if (requiresMax && evictingElement.stringValue().equals(windowSummary.getMax())) {
							maxEvicted = true;
						}
						if (requiresMin && evictingElement.stringValue().equals(windowSummary.getMin())) {
							minEvicted = true;
						}

						if (usingSorted) {
							// evict element from Sorted map if counter reaches zero
							if (sortedElements.get(evictingElement.stringValue()).decrementReachZero()) {
								sortedElements.remove(evictingElement.stringValue());
							}

							// if mode was evicted, we need a new mode
							if (requiresMode && windowSummary.getMode().equals(evictingElement.stringValue())) {
								modeEvicted = true;
							}

					
						}
						// not using sorted tree. Count distinct uses a hashmap
						else if (requiresCountDistinct
								&& uniqueElements.get(evictingElement.stringValue()).decrementReachZero()) {
							uniqueElements.remove(evictingElement.stringValue());
						}

					
					

						// count++;
						itemsRemoved = true;
					} else {
						// done with expired elements
						break;
					}

				}

				// Eviction complete. Now compute post-eviction stats

				if (itemsRemoved) {

					if (maxEvicted) {
						windowSummary.setMax(computeWindowMax());
					}
					if (minEvicted) {
						windowSummary.setMin(computeWindowMin());
					}
					if (modeEvicted) {
						windowSummary.setMode(computeWindowMode());
					}

					if (requiresFirst) {
						windowSummary.setFirst(windowQueue.peekFirst().stringValue());
					}

					if (requiresCount) {
						windowSummary.setCount(windowQueue.size());
					}

					if (requiresCountDistinct) {

						if (usingSorted) {
							windowSummary.setCountDistinct(sortedElements.size());
						} else {
							windowSummary.setCountDistinct(uniqueElements.size());
						}

					}
				
					// System.out.println("ExpirationTime " + expirationTime + " Removed " + count +
					// "\t size: "
					// + arrayDeque.size() + " sortedElements " + sortedElements.size());

				}
			}
		}
		// if(count > 0)
		// System.out.println("trimmed "+ count);

		// done evicting stuff.
		// IF using waiting queue....
		// move elements from waitingQueue into windowQueue:
		// loop through head of waiting queue dequeuing elements done waiting.
		if (hasRangeEnd) {
			for (ValueWithTimestamp_String waitingElement : waitingQueue) {

				// element is done waiting when element second + waiting period < current second
				if (waitingElement.getSecond() + rangeEnd <= currentSecond) {
					// remove pair from waiting queue and add to window queue.
					add(waitingQueue.poll());

				} else {
					// done with items that are done waiting...
					break;
				}
			}
		}

		// return count;
	}

	// Procedure called when we need to traverse all the data looking for a new MAX
	// it updates windowSummar.max
	private String computeWindowMax() {
		if (usingSorted) {
			return sortedElements.lastKey();
		} else {
			String tempMax = null;
			tempMax = windowQueue.peekFirst().stringValue();
			// maxSiblings = 0;
			// loop through dequeue looking for a smaller value
			for (ValueWithTimestamp_String entry : windowQueue) {
				if (entry.stringValue().compareTo(tempMax) > 0) {
					tempMax = entry.stringValue();
					// maxSiblings = 1;
				}
				// else if (entry.stringValue() == tempMax) {
				// maxSiblings++;
				// }
			}
			return tempMax;
		}
	}

	// Procedure called when we need to traverse all the data looking for a new Min
	// it updates windowSummar.min
	private String computeWindowMin() {
		if (usingSorted) {
			return sortedElements.firstKey();
		} else {
			String tempMin = null;
			// minSiblings = 0;
			// loop through deque looking for lower Min
			for (ValueWithTimestamp_String entry : windowQueue) {
				if (entry.stringValue().compareTo(tempMin) < 0) {
					tempMin = entry.stringValue();
					// minSiblings = 1;
				}
				// else if (entry.stringValue() == tempMin) {
				// minSiblings++;
				// }
			}
			return tempMin;
		}
	}

	// compute the Mode of this window
	private String computeWindowMode() {

		int highestCounter = 0;
		String newMode = null;
		// loop through sorted treemap looking for highest integer value.
		// remember, treemap<Double/Integer> is an element and how many repeats it has
		for (Entry<String, Counter> entry : sortedElements.entrySet()) {
			if (entry.getValue().intValue() > highestCounter) {
				highestCounter = entry.getValue().intValue();
				newMode = entry.getKey();
			}
		}
		modeQuantity = highestCounter;
		return newMode;
	}


	// get silenced at
	public int getSilencedUntil() {
		return silencedUntil;
	}

	@Override
	public boolean isEmpty() {
		return windowQueue.size() == 0;
	}

	@Override
	public boolean isFull() {
		return windowSummary.isFull();
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
	public int getWindowCount() {
		return windowSummary.getCount();
	}

	@Override
	public String getWindowLast() {
		return windowSummary.getLast();
	}

	// for debugging. prints to screen.
	public void printElements() {
		String string = "";

		String[] fa = new String[windowQueue.size()];
		int i = 0;
		for (ValueWithTimestamp_String f : windowQueue) {
			string = string + f.stringValue() + "\t";
			fa[i++] = f.stringValue();
		}
		System.out.println(string);
		string = "";
		Arrays.sort(fa);
		for (String f : fa) {
			string = string + f + "\t";
		}
		System.out.println(string);

	}

	// for debugging. prints to screen.
	public void printSorted() {
		String string = "Sorted:";
		for (Entry<String, Counter> entry : sortedElements.entrySet()) {
			string = string + "\t" + entry.getKey() + "[" + entry.getValue().intValue() + "]";
		}
		System.out.println(string);
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
