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
 *   Window of Quantity
 *   
 *   Controls statistics for a window of n elements. 
 *   As elements are inserted and evicted, the window statistics (stored in a WindowSummary object) are updated. 
 *   Each stat is only updated if required by query. Otherwise, their computation is skipped to save time. 
 *   
 *   It starts by stacking elements into an ArrayDeque collection until it reaches the window size limit. 
 *   once full, it transfers the data into a circular array for better performance. 
 *   
 *   Additional collections like TreeMap and ArrayDeque are optionally used, if the query requires Median or CountDistinct. 
 *   
 */

package org.riodb.windows;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map.Entry;

import org.riodb.engine.RioDB;
import org.riodb.sql.SQLFunctionMap;

import java.util.TreeMap;

public class WindowOfQuantity_String implements Window_String {

	// summary of window aggregated stats
	private final WindowSummary_String windowSummary = new WindowSummary_String();

	// partitionExpiration - to expire stale partitions.
	private int partitionExpiration;
	private int lastEntryTime;

	// A collection to stack initial elements, before window is full
	private ArrayDeque<String> initialWindow;

	// First-in-First-out circular array to store elements after window is full
	private String[] windowElements;
	
	// window size
	private int rangeSize;

	// marks the position of the oldest element in the circular array
	private int windowArrayMarker;

	// First-in-First-out circular array to store elements waiting to be in range
	/*
	 * Buffer for range... If a range is from 1000-100, then a message has to wait
	 * in a queue for 99 other messages. For this, a ring buffer is used as a
	 * waiting queue.
	 */
	private CircularArray_String waitingQueue;
	
	// window range start - only stored for cloning window
	private int rangeStart;
	
	// flag that rangeEnd is in use
	private boolean hasRangeEnd;

	// window range end
	private int rangeEnd;

	// If the queue window full (using circularArray instead of initialQueue
	// boolean windowIsFull;

	// Stores elements ordered for calculating MEDIAN and/or MODE
	// Double is an element value.
	// Counter is the count of how many elements in the window have that value
	private TreeMap<String, Counter> sortedElements;
	private boolean sortedElementsRequired;

	// Collection of distinct elements for COUNT_DISTINCT function.
	// is NOT used if sortedElements is already used
	// Double is an element value.
	// Counter is the count of how many elements in the window have that value
	private HashMap<String, Counter> uniqueElements;

	// For windows larger than 500 elements, MAX data will get summarized into
	// buckets of 100 for faster processing
	// We will refer to these as pages, tracked in an array of paginated MAX (or
	// the max value in each bucket of 100)
	private String maxPaginated[];
	private String minPaginated[];
	// page size
	private final int threshholdForUsingBuckets = 500;
	private final int pageSizeDefault = 100;
	private int pageSize; // will start as 0. Will also serve the purpose of a flag which indicates that
	// we're using pagination when pageSize > 0

	// Quantity of the mode element
	private int modeQuantity;

	private boolean required_Functions[];
	// required functions loaded out of the boolean array into descriptive variables
	// for readability.
	private boolean requiresCount;
	private boolean requiresCountDistinct;
	private boolean requiresFirst;
	private boolean requiresLast;
	private boolean requiresMax;
	private boolean requiresMin;
	private boolean requiresMode;
	private boolean requiresPrevious;


	// Constructor
	public WindowOfQuantity_String(int rangeStart, int rangeEnd, boolean[] functionsRequired, int partitionExpiration) {

		this.hasRangeEnd = false;
		this.rangeSize = rangeStart;
		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;

		if (rangeEnd > 0) {
			this.hasRangeEnd = true;
			this.rangeSize = rangeStart - rangeEnd;
			this.waitingQueue = new CircularArray_String(rangeEnd);
		}

		this.partitionExpiration = partitionExpiration;

		this.required_Functions = functionsRequired;
		this.requiresCount = functionsRequired[SQLFunctionMap.getFunctionId("count")];
		this.requiresCountDistinct = functionsRequired[SQLFunctionMap.getFunctionId("count_distinct")];
		this.requiresFirst = functionsRequired[SQLFunctionMap.getFunctionId("first")];
		this.requiresLast = functionsRequired[SQLFunctionMap.getFunctionId("last")];
		this.requiresMax = functionsRequired[SQLFunctionMap.getFunctionId("max")];
		this.requiresMin = functionsRequired[SQLFunctionMap.getFunctionId("min")];
		this.requiresMode = functionsRequired[SQLFunctionMap.getFunctionId("mode")];
		this.requiresPrevious = functionsRequired[SQLFunctionMap.getFunctionId("previous")];


		RioDB.rio.getSystemSettings().getLogger().debug("\tconstructing Window (String) of Quantity for Strings");

		// start empty initial stack
		initialWindow = new ArrayDeque<String>();
		windowElements = null;

		sortedElementsRequired = false;
		// additional collections are initialized as needed
		if (requiresMode
		// || requiresMax
		// || requiresMin
		) {
			sortedElements = new TreeMap<String, Counter>();
			sortedElementsRequired = true;
			uniqueElements = null;
		} else if (requiresCountDistinct) {
			uniqueElements = new HashMap<String, Counter>();
			sortedElements = null;
		} else {
			uniqueElements = null;
			sortedElements = null;
		}


		pageSize = 0;
		maxPaginated = null;
		minPaginated = null;

		// some variables get default assignment.
		windowArrayMarker = 0;

	}

	@Override
	public Window_String makeEmptyClone() {
		return new WindowOfQuantity_String(rangeStart, rangeEnd, required_Functions, partitionExpiration);
	}

	// a wrapper function that adds an element and returns the windowSummary
	@Override
	public WindowSummary_String trimAddAndGetWindowSummaryCopy(String element, int currentSecond) {
		
		// window of quantity does not trim by time. ignore currentSecond
		// timestamp
		if (partitionExpiration > 0) {
			lastEntryTime = currentSecond;
		}
		add(element);

		//if(hasRangeEnd)
		//System.out.println("Waiting queue: "+ waitingQueue.size());
		//printElements();
		
		
		return getWindowSummaryCopy();
	}

	// Public procedure to add Element to Window
	private void add(String elementInserted) {

		// if waiting queue is being used but not yet full...
		if (hasRangeEnd && !waitingQueue.isFull()) {
			// put in the waiting queue only 
			waitingQueue.put(elementInserted);
			// nothing else to do. Actual window is still empty. 

		} else {

			// if using rangeEnd and the waitingQueue is full:
			if (hasRangeEnd) {
				
				elementInserted = waitingQueue.putAndPop(elementInserted);

			}

			// if Previous is required...
			if (requiresPrevious) {
				windowSummary.setPrevious(windowSummary.getLast());
			}
			// if Last is required...
			if (requiresLast) {
				windowSummary.setLast(elementInserted);
			}
			// If window is full, we are using Circular Array.
			// Computations are different due to evicting oldest element
			if (windowSummary.isFull()) {

				// after inserting element, marker is incremented. But we still need original marker. 
				int markerOfElementInserted = windowArrayMarker;
				// add new element to circular array and retrieve evicted element.
				String elementEvicted = putAndPopFromWindow(elementInserted);

				// if First is required...
				if (requiresFirst) {
					windowSummary.setFirst(getFirst());
				}
				// if the new arriving is same as oldest leaving, we need to do nothing.
				if (!elementInserted.equals(elementEvicted)) {


					// if Median or Mode are required, then we must deal with the sorted TreeMap
					if (sortedElementsRequired) {

						/// part 1 - add new to sorted tree
						// If sorted tree already contains elementInserted, we increment its counter
						// value.
						Counter c = sortedElements.get(elementInserted);
						if (c != null) {
							c.increment();
							// update windowMode if the new element quantity is greater than windowMode
							if (requiresMode && c.isGT(modeQuantity)) {
								windowSummary.setMode(elementInserted);
								System.out.println("newMode: "+ windowSummary.getMode());
								modeQuantity = c.intValue();
							}
						} else {
							sortedElements.put(elementInserted, new Counter());
						}

						// Decrement the count of the element evicted.
						// If the count reaches zero, remove from sortedElements
						if (sortedElements.get(elementEvicted).decrementReachZero()) {
							sortedElements.remove(elementEvicted);
						}

						// If the elementEvicted was Mode, check if there's a new higher Mode.
						if (requiresMode && windowSummary.getMode().equals(elementEvicted)) {
							windowSummary.setMode(computeWindowMode());
						}



						// if CountDistinct is required, and we have a TreeMap, might as well get it
						// from the TreeMap
						if (requiresCountDistinct) {
							windowSummary.setCountDistinct(sortedElements.size());
						}

						// If Max or Min are required and we have a TreeMap, might as well get it from
						// the TreeMap
						if (requiresMax) {
							if (elementInserted.compareTo(windowSummary.getMax()) > 0) {
								windowSummary.setMax(elementInserted);
							} else if (elementEvicted.equals(windowSummary.getMax())) {
								windowSummary.setMax(sortedElements.lastKey());
							}
						}
						if (requiresMin) {
							if (elementInserted.compareTo( windowSummary.getMin()) < 0) {
								windowSummary.setMin(elementInserted);
							} else if (elementEvicted.equals(windowSummary.getMin())) {
								windowSummary.setMin(sortedElements.firstKey());
							}
						}

					}
					// NOT using sorted: CountDistinct, Min and Max have to be calculated some other
					// way
					else {

						// countDistinct will rely on the HashMap uniqueElements
						if (requiresCountDistinct) {
							Counter c = uniqueElements.get(elementInserted);
							if (c != null) {
								c.increment();
							} else {
								uniqueElements.put(elementInserted, new Counter());
							}

							// decrement and remove the evicted value if necessary
							if (uniqueElements.get(elementEvicted).decrementReachZero()) {
								uniqueElements.remove(elementEvicted);
							}

							windowSummary.setCountDistinct(uniqueElements.size());
						}

						if (requiresMax) {
							/// Set new max if elementInserted is bigger than max
							if (elementInserted.compareTo(windowSummary.getMax()) > 0) {
								windowSummary.setMax(elementInserted);
								if (pageSize > 0) {
									maxPaginated[markerOfElementInserted / pageSize] = elementInserted;
								}
							} else {
								// if inserted is not overall MAX but max within page
								if (pageSize > 0 && elementInserted.compareTo(maxPaginated[markerOfElementInserted / pageSize]) > 0) {
									maxPaginated[markerOfElementInserted / pageSize] = elementInserted;
								}

								// if evicted was overall max
								if (elementEvicted.equals(windowSummary.getMax())) {
									if (pageSize > 0) {
										computePageMax(markerOfElementInserted);
									}
									windowSummary.setMax(computeWindowMax());
								}
								// if evicted was not overall max but max within page
								else if (pageSize > 0 && elementEvicted.equals(maxPaginated[markerOfElementInserted / pageSize])) {
									computePageMax(markerOfElementInserted);
								}
							}

						}

						if (requiresMin) {
							/// Set new min if elementInserted is bigger than min
							if (elementInserted.compareTo(windowSummary.getMin()) < 0) {
								windowSummary.setMin(elementInserted);
								if (pageSize > 0) {
									minPaginated[markerOfElementInserted / pageSize] = elementInserted;
								}
							} else {
								// if inserted is not overall min but min within page
								if (pageSize > 0 && elementInserted.compareTo(minPaginated[markerOfElementInserted / pageSize]) < 0) {
									minPaginated[markerOfElementInserted / pageSize] = elementInserted;
								}

								// if evicted was overall min
								if (elementEvicted.equals(windowSummary.getMin())) {
									if (pageSize > 0) {
										computePageMin(markerOfElementInserted);
									}
									windowSummary.setMin(computeWindowMin());
								}
								// if evicted was not overall min but min within page
								else if (pageSize > 0 && elementEvicted.equals(minPaginated[markerOfElementInserted / pageSize])) {
									computePageMin(markerOfElementInserted);
									
									// for use with RingBuffer class. 
									// minPaginated[thisArrayMarker / pageSize] = buffer.getPageMin(markerOfElementInserted, pageSize); 
								}
							}
						}

					}


				}


			}
			/*
			 * 
			 * So far we handled adding a new element to a window that is already full
			 *
			 * Now we handle adding element to a window that is not yet full
			 * 
			 * it is still using the ArrayDeque instead of circular array.
			 * 
			 * And there's no element being Evicted. So calculations are different.
			 * 
			 */
			else if (initialWindow.size() >= 1) {

				// create Double object for arrayDeque.
				String elementAsString = elementInserted;
				initialWindow.add(elementAsString);

				// if Count is required...
				if (requiresCount) {
					windowSummary.incrementCount();
				}

				// if MAX is required
				if (requiresMax && elementInserted.compareTo(windowSummary.getMax()) > 0) {
					windowSummary.setMax(elementInserted);
				}
				// if MIN is required...
				if (requiresMin && elementInserted.compareTo(windowSummary.getMin()) < 0) {
					windowSummary.setMin(elementInserted);
				}

				// If either Median or Mode is required...
				if (sortedElementsRequired) {
					// Very similar to what we did for a full window,
					// except there's no evicted element to remove.
					// If elementInserted is already in the tree, increment it's counter Integer
					Counter c = sortedElements.get(elementAsString);
					if (c != null) {
						c.increment();
						if (requiresMode && c.isGT(modeQuantity)) {
							windowSummary.setMode(elementInserted);
							modeQuantity = c.intValue();
						}
					} else {
						sortedElements.put(elementInserted, new Counter());
						// since we already have a treemap, we can get count distinct:
						if (requiresCountDistinct) {
							windowSummary.setCountDistinct(sortedElements.size());
						}
					}

			
				}
				// If countDistinct is relying on uniqueElements hashmap:
				else if (requiresCountDistinct) {

					Counter c = uniqueElements.get(elementInserted);
					if (c != null) {
						c.increment();
					} else {
						uniqueElements.put(elementInserted, new Counter());
						windowSummary.setCountDistinct(uniqueElements.size());
					}

				}

			}
			/*
			 * 
			 * We already handled adding new element to window that is full, and window that
			 * is not yet full.
			 * 
			 * This next is for adding the very first element to a window, and this happens
			 * only once.
			 */
			else {
				// add first element to initialQueue
				initialWindow.add(elementInserted);
				// if Count is required...
				if (requiresCount) {
					windowSummary.setCount(1);
				}
				// if First is required...
				if (requiresFirst) {
					windowSummary.setFirst(elementInserted);
				}

				// if Max is required...
				if (requiresMax) {
					windowSummary.setMax(elementInserted);
				}
				// if Min is required
				if (requiresMin) {
					windowSummary.setMin(elementInserted);
				}
				// if a sortedTree is being used..
				if (sortedElementsRequired) {
					sortedElements.put(elementInserted, new Counter());
					if (requiresMode) {
						windowSummary.setMode(elementInserted);
						modeQuantity = 1;
					}
					if (requiresCountDistinct) {
						windowSummary.setCountDistinct(1);
					}
//				printSorted(in);
				} else if (requiresCountDistinct) {
					uniqueElements.put(elementInserted, new Counter());
					windowSummary.setCountDistinct(1);
				}
			}
			// If the queue just got full, time to switch from ArrayDeque to circular array
			if (!windowSummary.isFull() && initialWindow.size() == rangeSize) {
				convertToFullWindow();
				windowSummary.setFull(true);
				windowSummary.setCount(windowElements.length); // redundant
			}

		} // end if(hasRangeEnd && !waitingQueue.isFull())

	
	}


	// Procedure to convert from arrayDeque to fixed recycling array:
	private void convertToFullWindow() {
		windowElements = new String[initialWindow.size()];
		// fill array with contents of arrayDeque

		if (requiresMax && rangeSize >= threshholdForUsingBuckets) {
			pageSize = pageSizeDefault;
			maxPaginated = new String[(rangeSize / pageSize) + 1];
		}
		if (requiresMin && rangeSize >= threshholdForUsingBuckets) {
			pageSize = pageSizeDefault;
			minPaginated = new String[(rangeSize / pageSize) + 1];
		}

		int pageCount = 0;
		int pageIndex = 0;
		String pageMax = null;
		String pageMin = null;

		for (int i = 0; i < windowElements.length; i++) {
			windowElements[i] = initialWindow.poll();

			if (pageSize > 0) {
				if (requiresMax && windowElements[i].compareTo(pageMax) > 0) {
					pageMax = windowElements[i];
				}
				if (requiresMin && windowElements[i].compareTo(pageMin) < 0) {
					pageMin = windowElements[i];
				}
				pageCount++;
				if (pageCount == pageSize) {
					pageCount = 0;
					if (requiresMax) {
						maxPaginated[pageIndex] = pageMax;
						pageMax = null;
					}
					if (requiresMin) {
						minPaginated[pageIndex] = pageMin;
						pageMin = null;
					}
					pageIndex++;
				}
			}
		}

//		String s = "";
//		for (int i = 0; i < maxPaginated.length; i++) {
//			s = s + "  " + maxPaginated[i];
//		}
//		System.out.println(s);

		// empty arrayDeque to save memory.
		initialWindow = null;

		// clean up memory...
		System.gc();
	}

	// procedure to find a new max within a page
	// called when the max of a page is evicted
	// should never be called when not using paginated max
	private void computePageMax(int thisArrayMarker) {
		int start = (thisArrayMarker) / pageSize;
		// devide by 100 and multiply by 100 in order to round it to hundreds
		start = start * pageSize;
		int end = start + pageSize;
		if (end >= windowElements.length) {
			end = windowElements.length;
		}
		String max = windowElements[start];
		for (int i = start; i < end; i++) {
			if (windowElements[i].compareTo(max) > 0) {
				max = windowElements[i];
			}
		}
		maxPaginated[thisArrayMarker / pageSize] = max;
	}

	// procedure to find a new min within a page
	// called when the min of a page is evicted
	// should never be called when not using paginated Min
	private void computePageMin(int thisArrayMarker) {
		int start = (thisArrayMarker) / pageSize;
		// devide by 100 and multiply by 100 in order to round it to hundreds
		start = start * pageSize;
		int end = start + pageSize;
		if (end >= windowElements.length) {
			end = windowElements.length;
		}
		String min = windowElements[start];
		for (int i = start; i < end; i++) {
			if (windowElements[i].compareTo(min) < 0) {
				min = windowElements[i];
			}
		}
		minPaginated[thisArrayMarker / pageSize] = min;
	}

	// Procedure called when we need to traverse all the pages looking for a new
	// overall MAX
	// it updates windowSummar.max
	private String computeWindowMax() {
		// If the window size is greater than threshholdForUsingBuckets, we're using
		// pagination
		if (pageSize > 0) {
			String tempMax = maxPaginated[0];
			// loop through circular array looking for a greater value
			for (int i = 1; i < maxPaginated.length; i++) {
				if (maxPaginated[i].compareTo(tempMax) > 0) {
					tempMax = maxPaginated[i];
				}
			}
			return tempMax;
		}
		// if the window size is smaller than threshholdForUsingBuckets, we're not using
		// pagination.
		else {
			String tempMax = windowElements[0];
			// loop through circular array looking for a greater value
			for (int i = 1; i < windowElements.length; i++) {
				if (windowElements[i].compareTo(tempMax) > 0) {
					tempMax = windowElements[i];
				}
			}
			return tempMax;
		}
	}

	// Procedure called when we need to traverse all the pages looking for a new
	// overall MIN
	private String computeWindowMin() {
		if (pageSize > 0) {
			String tempMin = minPaginated[0];
			// loop through circular array looking for a greater value
			for (int i = 1; i < minPaginated.length; i++) {
				if (minPaginated[i].compareTo(tempMin) < 0) {
					tempMin = minPaginated[i];
				}
			}
			return tempMin;
		} else {
			String tempMin = windowElements[0];
			// loop through circular array looking for a greater value
			for (int i = 1; i < windowElements.length; i++) {
				if (windowElements[i].compareTo(tempMin) < 0) {
					tempMin = windowElements[i];
				}
			}
			return tempMin;
		}
	}

	// compute the Mode of this window
	private String computeWindowMode() {// (double elementInserted) {
		int highestCounter = 0;
		String newMode = null;
		// loop through sorted treemap looking for the key with highest Counter.
		for (Entry<String, Counter> entry : sortedElements.entrySet()) {
			if (entry.getValue().intValue() > highestCounter) {
				highestCounter = entry.getValue().intValue();
				newMode = entry.getKey();
			}
		}
		modeQuantity = highestCounter;
		return newMode;
	}


	// get size
	private int getCount() {
		if (windowSummary.isFull()) {
			return windowElements.length;
		}
		return initialWindow.size();
	}

	private String getFirst() {
		if (windowElements != null) {
			return windowElements[windowArrayMarker];
		}
		return initialWindow.peekFirst();
	}


	@Override
	public boolean isEmpty() {
		return getCount() == 0;
	}

	@Override
	public boolean isFull() {
		return windowSummary.isFull();
	}

	@Override
	public WindowSummary_String trimAndGetWindowSummaryCopy(int currentSecond) {
		// window of quantity does NOT trim by time... ignore trim...
		return new WindowSummary_String(windowSummary);
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

	// for debugging. Prints to screen.
	@Override
	public void printElements() {
		String string = "";
		if (windowSummary.isFull()) {
			string = "Array\t";
			for (int i = windowArrayMarker; i < windowElements.length; i++) {
				string = string + windowElements[i] + "\t";
			}
			for (int i = 0; i < windowArrayMarker; i++) {
				string = string + windowElements[i] + "\t";
			}
		} else {
			string = "Queue\t";
			for (String f : initialWindow) {
				string = string + f + "\t";
			}
		}
		System.out.println(string);
		if (sortedElements != null) {
			string = "sortedElements:";
			for (Entry<String, Counter> entry : sortedElements.entrySet()) {
				string = string + "\t" + entry.getKey() + "[" + entry.getValue().intValue() + "]";
			}
	//		System.out.println(string);
		}

	}

	@Override
	public void trimExpiredWindowElements(int currentSecond) {
		// not applicable. Just here to satisfy interface.
		//return 0;
	}

	@Override
	public String getAggregations() {
		return SQLFunctionMap.getFunctionsAvailable(required_Functions);
	}

	@Override
	public boolean requiresFunction(int functionId) {
		return required_Functions[functionId];
	}

	@Override
	public String getRange() {
		if (hasRangeEnd) {
			return String.valueOf(rangeStart + "-" + rangeEnd);
		}
		return String.valueOf(rangeSize);
	}

	@Override
	public boolean isDueForExpiration(int currentSecond) {
		if (currentSecond - lastEntryTime > Window.GRACE_PERIOD) {
			return true;
		}
		return false;
	}

	
	// procedure to add element to array and remove oldest lement in one shot
	private String putAndPopFromWindow(String in) {
		// swaps array entry old for new, at marker
		String ret = windowElements[windowArrayMarker];
		windowElements[windowArrayMarker] = in;
		windowArrayMarker++;
		// if marker exceeds array, return to 0
		if (windowArrayMarker == windowElements.length) {
			windowArrayMarker = 0;
		}
		return ret;
	}
	

}
