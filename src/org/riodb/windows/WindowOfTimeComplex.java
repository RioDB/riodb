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

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.riodb.engine.RioDB;
import org.riodb.sql.SQLFunctionMap;

import java.util.TreeMap;

public class WindowOfTimeComplex implements Window {

	// Window stats are stored in a summary object for easier passing
	// there's no performance loss versus local variables.
	private WindowSummary windowSummary;

	// partitionExpiration - to expire stale partitions.
	private int partitionExpiration;
	private int lastEntryTime;

	// A FIFO queue to hold elements in the window
	// Elements MUST be inserted in chronological order
	private ArrayDeque<ValueWithTimestamp> windowQueue;

	// A FIFO queue to hold elements waiting
	// Used only when range has end time, for example:
	// 100s-10s -> Elements must wait 10s before entering
	private ArrayDeque<ValueWithTimestamp> waitingQueue;

	// Stores elements ordered for calculating MEDIAN and/or MODE
	// Double is an element value.
	// Integer is the count of how many elements in the window have that value
	private TreeMap<Double, Counter> sortedElements;
	private boolean usingSorted;

	// Collection of distinct elements for COUNT_DISTINCT function.
	// is NOT used if sortedElements is already used
	// Double is an element value.
	// Integer is the count of how many elements in the window have that value
	private HashMap<Double, Counter> uniqueElements;

	// Count of how many elements equal windowMax
	// private int maxSiblings; // no longer needed

	// Count of how many elements equal windowMin
	// private int minSiblings; // no longer needed

	// median requires a local copy
	// This is the actual element known as the median element.
	// whereas in windowSummary, the median sometimes is a point in between the
	// two median elements when the window has an even number of elements.
	private double medianLocalVar;

	// Count of how many OTHER elements equal windowMedian
	private int medianDuplicates; //

	// When medianSibings > 1, we need to know which element is the exact median.
	private int medianMarker;

	// Quantity of the mode element
	private int modeQuantity;

	// counts how many inserts since the SLOPE variables were reset
	private long insertsSinceSlopeReset;

	// In regression formula, Ex is the SUM of all positions 1+2+3...+n
	private double slopeEx;

	// In regresion formula, Exy is the SUM of the product of xy. x1*y1 + x2*y2 ...
	// xn*yn
	private BigDecimal slopeExy;

	// In regresion formula, Ex2 is the SUM of all positions squared.
	private BigDecimal slopeEx2;

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
	private boolean requiresMedian;
	private boolean requiresMin;
	private boolean requiresMode;
	private boolean requiresPrevious;
	private boolean requiresSlope;
	private boolean requiresSum;
	private boolean requiresVariance;

	// Constructor
	public WindowOfTimeComplex(int rangeStart, int rangeEnd, boolean[] functionsRequired, int partitionExpiration) {

		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;
		this.hasRangeEnd = false;
		if (rangeEnd > 0) {
			this.hasRangeEnd = true;
			// initialize waiting Queue.
			waitingQueue = new ArrayDeque<ValueWithTimestamp>();
		}

		this.partitionExpiration = partitionExpiration;
		this.functionsRequired = functionsRequired;
		this.requiresCount = functionsRequired[SQLFunctionMap.getFunctionId("count")];
		this.requiresCountDistinct = functionsRequired[SQLFunctionMap.getFunctionId("count_distinct")];
		this.requiresFirst = functionsRequired[SQLFunctionMap.getFunctionId("first")];
		this.requiresLast = functionsRequired[SQLFunctionMap.getFunctionId("last")];
		this.requiresMax = functionsRequired[SQLFunctionMap.getFunctionId("max")];
		this.requiresMedian = functionsRequired[SQLFunctionMap.getFunctionId("median")];
		this.requiresMin = functionsRequired[SQLFunctionMap.getFunctionId("min")];
		this.requiresMode = functionsRequired[SQLFunctionMap.getFunctionId("mode")];
		this.requiresPrevious = functionsRequired[SQLFunctionMap.getFunctionId("previous")];
		this.requiresSlope = functionsRequired[SQLFunctionMap.getFunctionId("slope")];
		this.requiresSum = functionsRequired[SQLFunctionMap.getFunctionId("sum")];
		this.requiresVariance = functionsRequired[SQLFunctionMap.getFunctionId("variance")];

		RioDB.rio.getSystemSettings().getLogger().debug("\tconstructing Window of time, complex");

		windowSummary = new WindowSummary();

		// start empty initial stack
		windowQueue = new ArrayDeque<ValueWithTimestamp>();

		usingSorted = false;
		// additional collections are initialized as needed
		if (requiresMedian || requiresMode) {
			sortedElements = new TreeMap<Double, Counter>();
			usingSorted = true;
			uniqueElements = null;
		} else if (requiresCountDistinct) {
			uniqueElements = new HashMap<Double, Counter>();
			sortedElements = null;
		} else {
			uniqueElements = null;
			sortedElements = null;
		}

		// if variance is required
		if (requiresVariance) {
			// computeWindowVariance();
			windowSummary.resetVarRunningSum();
		}

		// some variables get default assignment.
		silencedUntil = 0;

		// lastTrim = 0;

	}

	@Override
	public Window makeEmptyClone() {
		return new WindowOfTimeComplex(rangeStart, rangeEnd, functionsRequired, partitionExpiration);
	}

	// a wrapper function that adds an element and returns the windowSummary
	@Override
	public WindowSummary trimAddAndGetWindowSummaryCopy(double element, int currentSecond) {
		// timestamp
		if (partitionExpiration > 0) {
			lastEntryTime = currentSecond;
		}
		trimExpiredWindowElements(currentSecond);

		// Double elementAsDouble = element;
		ValueWithTimestamp elementWithTimestamp = new ValueWithTimestamp(element, currentSecond);

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
	private void add(ValueWithTimestamp pairInserted) {

		Double elementAsDouble = pairInserted.doubleValue();

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
			windowSummary.setLast(pairInserted.doubleValue());
		}

		// If queue already has elements
		if (windowQueue.size() >= 1) {

			windowQueue.add(pairInserted);

			// if Count is required...
			if (requiresCount) {
				windowSummary.incrementCount();
			}

			// if SUM is required...
			if (requiresSum) {
				windowSummary.sumAdd(pairInserted.doubleValue());
			}
			// if MAX is required
			if (requiresMax && pairInserted.doubleValue() > windowSummary.getMax()) {
				windowSummary.setMax(pairInserted.doubleValue());
			}
			// if MIN is required...
			if (requiresMin && pairInserted.doubleValue() < windowSummary.getMin()) {
				windowSummary.setMin(pairInserted.doubleValue());
			}

			// If either Median or Mode is required...
			if (usingSorted) {
				// Very similar to what we did for a full window,
				// except there's no evicted element to remove.
				// If elementInserted is already in the tree, increment it's counter Integer
				Counter c = sortedElements.get(elementAsDouble);
				if (c != null) {
					c.increment();
					if (requiresMode && c.isGT(modeQuantity)) {
						windowSummary.setMode(pairInserted.doubleValue());
						modeQuantity = c.intValue();
					}
				} else {
					sortedElements.put(pairInserted.doubleValue(), new Counter());
					if (requiresCountDistinct) {
						windowSummary.setCountDistinct(sortedElements.size());
					}
				}

				// If MEDIAN is required...
				if (requiresMedian) {
					// now determine if the median changed.
					// the logic:
					// if inserted == median, and the count is now odd, median slides up
					if (pairInserted.doubleValue() == medianLocalVar) {
						medianDuplicates++;
						if (windowQueue.size() % 2 != 0) {
							medianMarker++;
						}
					}
					// If inserted > median, and the count is now now odd: median slides up
					else if (pairInserted.doubleValue() > medianLocalVar && windowQueue.size() % 2 != 0) { //
						if (medianSlideUp()) {
							windowSummary.setMedian(computeWindowMedian());
						}
					}
					// If inserted < median, count is now even: median slides down
					else if (pairInserted.doubleValue() < medianLocalVar && windowQueue.size() % 2 == 0) {
						if (medianSlideDown()) {
							windowSummary.setMedian(computeWindowMedian());
						}
					}
					// windowSummary.setMedian(computeWindowMedian());
					// TODO: Not needed???
				}

			}
			// If countDistinct is relying on uniqueElements hashmap:
			// countDistinct use hashmap
			else if (requiresCountDistinct) {
				Counter c = uniqueElements.get(pairInserted.doubleValue());
				if (c != null) {
					c.increment();
				} else {
					uniqueElements.put(pairInserted.doubleValue(), new Counter());
					windowSummary.setCountDistinct(uniqueElements.size());
				}
			}

			// if SLOPE is required...
			if (requiresSlope) {
				// Restart fresh is sums get TOO large.
				if (slopeExy.precision() + slopeExy.scale() > 20 || slopeEx2.precision() + slopeEx2.scale() > 20) {
					resetSlopeVars();
				} else {
					// count up the inserts since last slope reset.
					insertsSinceSlopeReset++;
					// sum of all X positions...
					slopeEx = slopeEx + insertsSinceSlopeReset;
					// sum of x*y
					BigDecimal xy = new BigDecimal((insertsSinceSlopeReset * pairInserted.doubleValue()));
					slopeExy = slopeExy.add(xy, Constants.MATH_CONTEXT);
					// sum of x square
					BigDecimal x2 = new BigDecimal((insertsSinceSlopeReset * insertsSinceSlopeReset));
					slopeEx2 = slopeEx2.add(x2, Constants.MATH_CONTEXT);

				}

				// call reusable function to finish computing windowSlope variable.
				windowSummary.setSlope(computeWindowSlope());
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
				windowSummary.setFirst(pairInserted.doubleValue());
			}

			// if Max is required...
			if (requiresMax) {
				windowSummary.setMax(pairInserted.doubleValue());
				// if (!usingSorted)
				// maxSiblings = 1;
			}
			// if Min is required
			if (requiresMin) {
				windowSummary.setMin(pairInserted.doubleValue());
				// if (!usingSorted)
				// minSiblings = 1;
			}
			// if SUM is required
			if (requiresSum) {
				windowSummary.setSum(pairInserted.doubleValue());
			}
			// if Median is required...
			if (usingSorted) {
				sortedElements.put(pairInserted.doubleValue(), new Counter());
				if (requiresMedian) {
					medianLocalVar = pairInserted.doubleValue();
					windowSummary.setMedian(pairInserted.doubleValue());
					medianDuplicates = 0;
					medianMarker = 0;
				}
				if (requiresMode) {
					windowSummary.setMode(pairInserted.doubleValue());
					modeQuantity = 1;
				}
				if (requiresCountDistinct) {
					windowSummary.setCountDistinct(1);
				}
//				printSorted(in);
			} else if (requiresCountDistinct) {
				uniqueElements.put(pairInserted.doubleValue(), new Counter());
				windowSummary.setCountDistinct(1);
			}
			// if Slope is required
			if (requiresSlope) {
				insertsSinceSlopeReset = 1; // starting...
				slopeEx = 1; // starting
				slopeExy = new BigDecimal(pairInserted.doubleValue()); // x is 1. x*y = y
				slopeEx2 = Constants.ONE; // starting
				windowSummary.setSlope(computeWindowSlope()); // set windowSummary.slope value.
			}
		}
		// if variance is required
		if (requiresVariance) {
			// computeWindowVariance();
			// windowSummary.varRunningSumAdd(elementInserted);
			windowSummary.varRunningSumAdd(
					computeVarianceAddend(pairInserted.doubleValue(), windowSummary.getAvg(), windowSummary.getSum()));
		}

//		printSorted();
//		printElements();
//		System.out.println("localMedian: " + localMedian + " medianMarker: " + medianMarker + " actualMedian: "+ windowSummary.getMedian()+"\n");
		/*
		 * if(windowSummary.getCount() > 1 && windowSummary.getSlope() != 1) {
		 * System.out.println("				insertsSinceSlopeReset = " +
		 * insertsSinceSlopeReset + "\r\n				min = " + windowSummary.getMin()
		 * + "\r\n				max = " + windowSummary.getMax() +
		 * 
		 * "\r\n				slopeEx = " + slopeEx + "\r\n				slopeExy = "
		 * + slopeExy + "\r\n				slopeEx2 = " + slopeEx2 +
		 * "\r\n				slope = " + windowSummary.getSlope()); System.exit(0); }
		 */
	}

	private void resetWindow() {
		// init windowSummary
		windowSummary = new WindowSummary();

		// start empty initial stack
		windowQueue = new ArrayDeque<ValueWithTimestamp>();

		// additional collections are initialized as needed
		if (usingSorted) {
			sortedElements = new TreeMap<Double, Counter>();
		} else if (requiresCountDistinct) {
			uniqueElements = new HashMap<Double, Counter>();
		}
		// if variance is required
		if (requiresVariance) {
			// computeWindowVariance();
			windowSummary.resetVarRunningSum();
		}
		// if Slope is required
		if (requiresSlope) {
			insertsSinceSlopeReset = 0; // starting...
			slopeEx = 0; // starting
			slopeExy = new BigDecimal(0); // x is 1. x*y = y
			slopeEx2 = new BigDecimal(0); // starting
			windowSummary.setSlope(0);
		}

		// System.out.println("reset - sortedElements size " + sortedElements.size());
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
				boolean medianChanged = false;
				boolean modeEvicted = false;

				for (ValueWithTimestamp evictingElement : windowQueue) {

					if (evictingElement.getSecond() <= expirationTime) {

						// double evicted = arrayDeque.poll().doubleValue();
						windowQueue.poll().doubleValue();

						if (requiresSum) {
							windowSummary.sumSubtract(evictingElement.doubleValue());
						}
						if (requiresCount) {
							windowSummary.decrementCount();
						}
						if (requiresMax && evictingElement.doubleValue() == windowSummary.getMax()) {
							maxEvicted = true;
						}
						if (requiresMin && evictingElement.doubleValue() == windowSummary.getMin()) {
							minEvicted = true;
						}

						if (usingSorted) {
							// evict element from Sorted map if counter reaches zero
							if (sortedElements.get(evictingElement.doubleValue()).decrementReachZero()) {
								sortedElements.remove(evictingElement.doubleValue());
							}

							// if mode was evicted, we need a new mode
							if (requiresMode && windowSummary.getMode() == evictingElement.doubleValue()) {
								modeEvicted = true;
							}

							// If MEDIAN is required...
							if (requiresMedian) {

								// evicted lower, now odd: slides up
								// evicted higher, now even: slides down
								// evicted median, now even: slides down
								// evicted median, now odd: slides up
								if (evictingElement.doubleValue() < medianLocalVar && windowQueue.size() % 2 != 0) { //
									if (medianSlideUp()) {
										medianChanged = true;
									}
								} else if (evictingElement.doubleValue() > medianLocalVar
										&& windowQueue.size() % 2 == 0) { //
									if (medianSlideDown()) {
										medianChanged = true;
									}
								} else if (evictingElement.doubleValue() == medianLocalVar) {
									if (windowQueue.size() % 2 == 0) { // slides down
										if (medianSlideDown()) {
											medianChanged = true;
										}
									} else { // slides up
										if (medianSlideUp()) {
											medianChanged = true;
										}
									}
								}
//								System.out.println("    ### Out: " + evictingElement.doubleValue() + " median: " + localMedian + " marker: " + medianMarker);

							}

						}
						// not using sorted tree. Count distinct uses a hashmap
						else if (requiresCountDistinct
								&& uniqueElements.get(evictingElement.doubleValue()).decrementReachZero()) {
							uniqueElements.remove(evictingElement.doubleValue());
						}

						// removed since it runs at the end of the loop
						// if (requiresFirst) {
						// windowSummary.setFirst(arrayDeque.peekFirst().doubleValue());
						// }

						// if variance is required
						if (requiresVariance) {
							// count check to prmessage division by zero.
							if (windowSummary.getCount() > 0) {
								windowSummary.varRunningSumRemove(
										computeVarianceSubtrahand(evictingElement.doubleValue(), windowSummary.getAvg(),
												windowSummary.getSum(), windowSummary.getCount()));
							} else {
								windowSummary.setVarRunningSum(0.0);
							}
						}

						if (requiresSlope) {
							// count how many inserts since resetSlopeVars()
							// insertsSinceSlopeReset++;
							// as we evict the oldest element, we also evict it's position x from the sum of
							// x
							long removedIndex = insertsSinceSlopeReset - windowQueue.size();
							// as we insert new element, we add new x to ex
							slopeEx = slopeEx - removedIndex;
							// Update the sum of x*y
							BigDecimal xy = new BigDecimal((removedIndex * evictingElement.doubleValue()));
							slopeExy = slopeExy.subtract(xy, Constants.MATH_CONTEXT);
							// Update the sum of x square
							BigDecimal x2 = new BigDecimal((removedIndex * removedIndex));
							slopeEx2 = slopeEx2.subtract(x2, Constants.MATH_CONTEXT);
							// As these Sums start to get too big, we need to reset them back to a fresh
							// start.
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
					if (medianChanged) {
						windowSummary.setMedian(computeWindowMedian());
					}
					if (modeEvicted) {
						windowSummary.setMode(computeWindowMode());
					}

					if (requiresFirst) {
						windowSummary.setFirst(windowQueue.peekFirst().doubleValue());
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
					if (requiresSlope) {
						// set the Regression Slope for this window.
						windowSummary.setSlope(computeWindowSlope());
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
			for (ValueWithTimestamp waitingElement : waitingQueue) {

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
	private double computeWindowMax() {
		if (usingSorted) {
			return sortedElements.lastKey().doubleValue();
		} else {
			double tempMax = Double.MIN_VALUE;
			tempMax = windowQueue.peekFirst().doubleValue();
			// maxSiblings = 0;
			// loop through dequeue looking for a smaller value
			for (ValueWithTimestamp entry : windowQueue) {
				if (entry.doubleValue() > tempMax) {
					tempMax = entry.doubleValue();
					// maxSiblings = 1;
				}
				// else if (entry.doubleValue() == tempMax) {
				// maxSiblings++;
				// }
			}
			return tempMax;
		}
	}

	// Procedure called when we need to traverse all the data looking for a new Min
	// it updates windowSummar.min
	private double computeWindowMin() {
		if (usingSorted) {
			return sortedElements.firstKey().doubleValue();
		} else {
			double tempMin = Double.MAX_VALUE;
			// minSiblings = 0;
			// loop through deque looking for lower Min
			for (ValueWithTimestamp entry : windowQueue) {
				if (entry.doubleValue() < tempMin) {
					tempMin = entry.doubleValue();
					// minSiblings = 1;
				}
				// else if (entry.doubleValue() == tempMin) {
				// minSiblings++;
				// }
			}
			return tempMin;
		}
	}

	// compute the Mode of this window
	private double computeWindowMode() {

		int highestCounter = 0;
		double newMode = 0;
		// loop through sorted treemap looking for highest integer value.
		// remember, treemap<Double/Integer> is an element and how many repeats it has
		for (Entry<Double, Counter> entry : sortedElements.entrySet()) {
			if (entry.getValue().intValue() > highestCounter) {
				highestCounter = entry.getValue().intValue();
				newMode = entry.getKey().doubleValue();
			}
		}
		modeQuantity = highestCounter;
		return newMode;
	}

	// calculate the regression line slope of elements in this window
	@SuppressWarnings("deprecation")
	private float computeWindowSlope() {
		// float d = (float) ((getCount() * slopeExy - slopeEx * windowSummary.getSum())
		// / (getCount() * slopeEx2 - slopeEx * slopeEx));
		/// or (part a - part b) / (part c - part d )

		// the formula above executed with BigDecimal operations:

		BigDecimal count = new BigDecimal(windowSummary.getCount());

		// getCount() * slopeExy
		BigDecimal a = count.multiply(slopeExy, Constants.MATH_CONTEXT);
		// slopeEx * windowSummary.getSum()
		BigDecimal b = new BigDecimal(slopeEx * windowSummary.getSum());
		// a - b
		a = a.subtract(b, Constants.MATH_CONTEXT);

		// getCount() * slopeEx2
		BigDecimal c = count.multiply(slopeEx2, Constants.MATH_CONTEXT);
		// slopeEx * slopeEx
		BigDecimal d = new BigDecimal((slopeEx * slopeEx));
		// c - d
		c = c.subtract(d, Constants.MATH_CONTEXT);

		if (windowSummary.getCount() > 1 && slopeEx > 0)
			a = a.divide(c, Constants.MATH_CONTEXT);

		return a.setScale(3, BigDecimal.ROUND_HALF_DOWN).floatValue();

	}

	// Handle median slide up.
	// true if median becomes the next greatest element
	// false if median is the same element but moves up a sibling.
	private boolean medianSlideUp() {
		if (medianDuplicates == medianMarker) { // if rightmost, like 6 out of 6, or 0 out of 0.
			// median value becomes the key of the next entry to the right
			Map.Entry<Double, Counter> t = sortedElements.higherEntry((Double) medianLocalVar);
			if (t != null) {
				medianLocalVar = t.getKey();
				medianDuplicates = t.getValue().intValue() - 1; // if Counter = 5, then 4 duplicates.
				medianMarker = 0;
				return true;
			}
		} else {
			// increment marker
			// same median value, just moving up the index.
			medianMarker++;
		}
		return false;
	}

	// Handle median slide down
	// true if median becomes the next greatest element
	// false if median is the same element but moves up a sibling.
	private boolean medianSlideDown() {
		if (medianMarker == 0) { // if leftmost, like 0 out of 6, or 0 out of 0.
			// median value becomes the key of the next entry to the left
			Map.Entry<Double, Counter> t = sortedElements.lowerEntry(medianLocalVar);
			if (t != null) {
				medianLocalVar = t.getKey();
				medianDuplicates = t.getValue().intValue() - 1;
				medianMarker = medianDuplicates;
				return true;
			}
		} else {
			// decrement marker
			// same median value, same sorted key.
			medianMarker--;
		}
		return false;
	}

	private double computeWindowMedian() {
		// If there are even numbers in the set
		if (medianDuplicates == medianMarker && windowQueue.size() % 2 == 0
				&& sortedElements.lastEntry().getKey() > medianLocalVar) {
			double tempMedian = (medianLocalVar + sortedElements.higherEntry(medianLocalVar).getKey().doubleValue())
					/ 2;
			return tempMedian;
		}
		return medianLocalVar;
	}

	// calculates a weighted delta to be added to the
	// variance Sum of squared deltas.
	private BigDecimal computeVarianceAddend(double elementInserted, double mean, double sum) {

		// assuming that count has already been increased by 1
		// BigDecimal representation of count (for BigDecimal computation)
		BigDecimal countB = new BigDecimal(windowQueue.size());
		// BigDecimal representation of the element inserted
		BigDecimal elementB = new BigDecimal(elementInserted);
		// BigDecimal representation of mean (or average)
		BigDecimal meanB = new BigDecimal(mean);
		// BigDecimal computation of the delta between mean and new element
		double avg = 0;
		if (windowQueue.size() > 1) {
			avg = (double) ((sum - elementInserted) / (windowQueue.size() - 1));
		}
		meanB = new BigDecimal(avg);
		// BigDecimal representation of the delta
		BigDecimal delta = meanB.subtract(elementB);
		// BigDecimal representation of the weighted delta
		BigDecimal weightedDelta = countB.add(Constants.NEGATIVE_ONE).divide(countB, Constants.MATH_CONTEXT)
				.multiply(delta, Constants.MATH_CONTEXT).multiply(delta, Constants.MATH_CONTEXT);
		// varRunningSum = varRunningSum.add(d,Constants.MATH_CONTEXT);

		return weightedDelta;

		// System.out.println("add "+ elementInserted +" n "+ count +" sum
		// "+varRunningSum.toPlainString()+
		// " avg "+ getAvg() + " delta "+ delta.toPlainString()+" var = " +
		// getPopulationVariance() );

	}

	// calculates a weighted delta to be removed from the
	// variance Sum of squared deltas.
	private BigDecimal computeVarianceSubtrahand(double elementEvicted, double mean, double sum, int count) {
		/*
		 * // BigDecimal representation of the Count BigDecimal countB = new
		 * BigDecimal(count); // BigDecimal representation of the element BigDecimal
		 * elementB = new BigDecimal(elementEvicted); // BigDecimal representation of
		 * median (average) BigDecimal meanB = new BigDecimal((double) (sum +
		 * elementEvicted) / (count + 1), Constants.MATH_CONTEXT); // BigDecimal
		 * computation of the delta between mean and evicted element BigDecimal delta =
		 * elementB.subtract(meanB); // adjustment to running Sum of squares BigDecimal
		 * weightedDelta = countB.subtract(Constants.NEGATIVE_ONE).divide(countB,
		 * Constants.MATH_CONTEXT) .multiply(delta.multiply(delta,
		 * Constants.MATH_CONTEXT), Constants.MATH_CONTEXT); // varRunningSum =
		 * varRunningSum.subtract(d,Constants.MATH_CONTEXT); return weightedDelta;
		 */
		// TODO: Under review
		// BigDecimal representation of the Count
		BigDecimal countB = new BigDecimal(count);
		BigDecimal countPlusOneB = new BigDecimal(count + 1);
		// BigDecimal representation of the element
		BigDecimal elementB = new BigDecimal(elementEvicted);
		// BigDecimal representation of median (average)
		BigDecimal originalMeanB = new BigDecimal((double) (sum + elementEvicted) / (count + 1),
				Constants.MATH_CONTEXT);
		// BigDecimal computation of the delta between mean and evicted element
		BigDecimal delta = elementB.subtract(originalMeanB);
		// weighted delta = delta^2 * (count+1) / count
		BigDecimal weightedDelta = delta.multiply(delta, Constants.MATH_CONTEXT)
				.multiply(countPlusOneB, Constants.MATH_CONTEXT).divide(countB, Constants.MATH_CONTEXT);
		return weightedDelta;

	}

	// get silenced at
	public int getSilencedUntil() {
		return silencedUntil;
	}

	private void resetSlopeVars() {

		insertsSinceSlopeReset = windowQueue.size();
		slopeEx = ((1 + insertsSinceSlopeReset) * insertsSinceSlopeReset) / 2;
		slopeEx2 = new BigDecimal(
				(insertsSinceSlopeReset * (insertsSinceSlopeReset + 1) * (2 * insertsSinceSlopeReset + 1)) / 6);

		slopeExy = new BigDecimal(0);
		int x = 1;
		for (ValueWithTimestamp f : windowQueue) {
			BigDecimal xy = new BigDecimal((x++ * f.doubleValue()));
			slopeExy = slopeExy.add(xy);
		}

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
	public WindowSummary trimAndGetWindowSummaryCopy(int currentSecond) {
		trimExpiredWindowElements(currentSecond);
		return getWindowSummaryCopy();
	}

	@Override
	public WindowSummary getWindowSummaryCopy() {
		return new WindowSummary(windowSummary);
	}

	@Override
	public int getWindowCount() {
		return windowSummary.getCount();
	}

	@Override
	public double getWindowLast() {
		return windowSummary.getLast();
	}

	// for debugging. prints to screen.
	public void printElements() {
		String string = "";

		double[] fa = new double[windowQueue.size()];
		int i = 0;
		for (ValueWithTimestamp f : windowQueue) {
			string = string + f.doubleValue() + "\t";
			fa[i++] = f.doubleValue();
		}
		System.out.println(string);
		string = "";
		Arrays.sort(fa);
		for (double f : fa) {
			string = string + f + "\t";
		}
		System.out.println(string);

	}

	// for debugging. prints to screen.
	public void printSorted() {
		String string = "Sorted:";
		for (Entry<Double, Counter> entry : sortedElements.entrySet()) {
			string = string + "\t" + entry.getKey().doubleValue() + "[" + entry.getValue().intValue() + "]";
		}
		System.out.println(string);
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
