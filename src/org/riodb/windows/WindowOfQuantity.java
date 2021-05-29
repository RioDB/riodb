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

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.riodb.engine.RioDB;
import org.riodb.sql.SQLFunctionMap;

import java.util.TreeMap;

public class WindowOfQuantity implements Window {

	// summary of window aggregated stats
	private final WindowSummary windowSummary = new WindowSummary();

	// partitionExpiration - to expire stale partitions.
	private int partitionExpiration;
	private int lastEntryTime;

	// A collection to stack initial elements
	private ArrayDeque<Double> initialQueue;

	// First-in-First-out circular array to store elements after window is full
	private double[] circularArray;

	// marks the position of the oldest element in the circular array
	private int circularArrayMarker;

	// Is the queue window full (using circularArray instead of initialQueue
	// boolean windowIsFull;

	// Stores elements ordered for calculating MEDIAN and/or MODE
	// Double is an element value.
	// Counter is the count of how many elements in the window have that value
	private TreeMap<Double, Counter> sortedElements;
	private boolean sortedElementsRequired;

	// Collection of distinct elements for COUNT_DISTINCT function.
	// is NOT used if sortedElements is already used
	// Double is an element value.
	// Counter is the count of how many elements in the window have that value
	private HashMap<Double, Counter> uniqueElements;

	// For windows larger than 500 elements, MAX data will get summarized into
	// buckets of 100 for faster processing
	// We will refer to these are pages, tracked in an array of paginated MAX (or
	// the max value in each bucket of 100)
	private double maxPaginated[];
	private double minPaginated[];
	// page size
	private final int threshholdForUsingBuckets = 500;
	private final int pageSizeDefault = 100;
	private int pageSize; // will start as 0. Will also serve the purpose of a flag which indicates that
	// we're using pagination when pageSize > 0

	// median requires a local copy
	// This is the actual element known as the median element.
	// whereas in windowSummary, the median sometimes is a point in between the
	// two median elements when the window has an even number of elements.
	private double medianLocalVar;

	// Count of how many elements equal windowMedian
	private int medianDuplicates; //

	// When medianSibings > 1, we need to know which element is the exact median.
	private int medianMarker;

	// Quantity of the mode element
	private int modeQuantity;

	// counts how many inserts since the SLOPE variables were reset
	private long slopeInsertsSinceReset;

	// In regression formula, Ex is the SUM of all positions 1+2+3...+n
	private double slopeEx;

	// In regresion formula, Exy is the SUM of the product of xy. x1*y1 + x2*y2 ...
	// xn*yn
	private BigDecimal slopeExy;

	// In regresion formula, Ex2 is the SUM of all positions squared.
	private BigDecimal slopeEx2;

	// window limit
	private int range;

	private boolean required_Functions[];
	// required functions loaded out of the boolean array into descriptive variables
	// for readability.
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
	public WindowOfQuantity(int range, boolean[] functionsRequired, int partitionExpiration) {

		this.range = range;
		this.partitionExpiration = partitionExpiration;

		this.required_Functions = functionsRequired;
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

		RioDB.rio.getLogger().debug("constructing Window of Quantity");

		// start empty initial stack
		initialQueue = new ArrayDeque<Double>();
		circularArray = null;

		sortedElementsRequired = false;
		// additional collections are initialized as needed
		if (requiresMedian || requiresMode
		// || requiresMax
		// || requiresMin
		) {
			sortedElements = new TreeMap<Double, Counter>();
			sortedElementsRequired = true;
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

		pageSize = 0;
		maxPaginated = null;
		minPaginated = null;

		// some variables get default assignment.
		circularArrayMarker = 0;

	}

	@Override
	public Window makeFreshClone() {
		return new WindowOfQuantity(range, required_Functions, partitionExpiration);
	}

	// a wrapper function that adds an element and returns the windowSummary
	@Override
	public WindowSummary trimAddAndGetWindowSummaryCopy(double element, int currentSecond) {
		// window of quantity does not trim by time. ignore currentSecond
		// timestamp
		if (partitionExpiration > 0) {
			lastEntryTime = currentSecond;
		}
		add(element);
		return getWindowSummaryCopy();
	}

	// Public procedure to add Element to Window
	private void add(double elementInserted) {

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

			int thisArrayMarker = circularArrayMarker;
			// add new element to circular array and retrieve evicted element.
			double elementEvicted = pushAndPoll(elementInserted);

			// if First is required...
			if (requiresFirst) {
				windowSummary.setFirst(getFirst());
			}
			// if the new arriving is same as oldest leaving, we need to do nothing.
			if (elementInserted != elementEvicted) {
				// if SUM is needed, we need to compute windowSum
				if (requiresSum) {
					windowSummary.sumAdd(elementInserted - elementEvicted);
				}

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
					if (requiresMode && windowSummary.getMode() == elementEvicted) {
						windowSummary.setMode(computeWindowMode());
					}

					// part 2 - determine new median
					// The logic:
					// inserted higher, evicted equal or lower: median slides up
					// inserted lower, evicted equal or higher: median slides down
					// inserted equal, evicted lower:median slides up (index)
					// inserted equal, evicted higher: median same
					if (elementInserted > medianLocalVar && elementEvicted <= medianLocalVar) {
						medianSlideUp();
					} else if (elementInserted < medianLocalVar && elementEvicted >= medianLocalVar) {
						medianSlideDown();
					} else if (elementInserted == medianLocalVar) {
						medianDuplicates++;
						if (elementEvicted < medianLocalVar) {
							medianSlideUp();
						}
					} else if (elementEvicted == medianLocalVar) {
						medianDuplicates--;
					}
					windowSummary.setMedian(computeWindowMedian());

					// if CountDistinct is required, and we have a TreeMap, might as well get it
					// from the TreeMap
					if (requiresCountDistinct) {
						windowSummary.setCountDistinct(sortedElements.size());
					}

					// If Max or Min are required and we have a TreeMap, might as well get it from
					// the TreeMap
					if (requiresMax) {
						if (elementInserted > windowSummary.getMax()) {
							windowSummary.setMax(elementInserted);
						} else if (elementEvicted == windowSummary.getMax()) {
							windowSummary.setMax(sortedElements.lastKey().doubleValue());
						}
					}
					if (requiresMin) {
						if (elementInserted < windowSummary.getMin()) {
							windowSummary.setMin(elementInserted);
						} else if (elementEvicted == windowSummary.getMin()) {
							windowSummary.setMin(sortedElements.firstKey().doubleValue());
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
						if (elementInserted > windowSummary.getMax()) {
							windowSummary.setMax(elementInserted);
							if (pageSize > 0) {
								maxPaginated[thisArrayMarker / pageSize] = elementInserted;
							}
						} else {
							// if inserted is not overall MAX but max within page
							if (pageSize > 0 && elementInserted > maxPaginated[thisArrayMarker / pageSize]) {
								maxPaginated[thisArrayMarker / pageSize] = elementInserted;
							}

							// if evicted was overall max
							if (elementEvicted == windowSummary.getMax()) {
								if (pageSize > 0) {
									computePageMax(thisArrayMarker);
								}
								windowSummary.setMax(computeWindowMax());
							}
							// if evicted was not overall max but max within page
							else if (pageSize > 0 && elementEvicted == maxPaginated[thisArrayMarker / pageSize]) {
								computePageMax(thisArrayMarker);
							}
						}

					}

					if (requiresMin) {
						/// Set new min if elementInserted is bigger than min
						if (elementInserted < windowSummary.getMin()) {
							windowSummary.setMin(elementInserted);
							if (pageSize > 0) {
								minPaginated[thisArrayMarker / pageSize] = elementInserted;
							}
						} else {
							// if inserted is not overall min but min within page
							if (pageSize > 0 && elementInserted < minPaginated[thisArrayMarker / pageSize]) {
								minPaginated[thisArrayMarker / pageSize] = elementInserted;
							}

							// if evicted was overall min
							if (elementEvicted == windowSummary.getMin()) {
								if (pageSize > 0) {
									computePageMin(thisArrayMarker);
								}
								windowSummary.setMin(computeWindowMin());
							}
							// if evicted was not overall min but min within page
							else if (pageSize > 0 && elementEvicted == minPaginated[thisArrayMarker / pageSize]) {
								computePageMin(thisArrayMarker);
							}
						}
					}

				}

				// Variance is always calculated the same way, regardless of using sorted
				// TreeMap or not.
				if (requiresVariance) {

					// getCount check to prevent division by zero.
					// TODO: check if still needed. Practically, and empty window should never enter
					// this code block.
					if (getCount() > 0) {
						windowSummary.varRunningSumRemove(computeVarianceSubtrahand(elementInserted, elementEvicted,
								windowSummary.getAvg(), windowSummary.getSum(), getCount()));
						// originalAvg, originalSum, getCount()-1));
					} else {
						windowSummary.setVarRunningSum(0.0);
					}
					windowSummary.varRunningSumAdd(
							computeVarianceAddend(elementInserted, windowSummary.getAvg(), windowSummary.getSum()));
				}

			}

			// If SLOPE is required...
			// Slope can change even when inserted is same as evicted.
			if (requiresSlope) {

				// Restart fresh is sums get TOO large.
				if (slopeExy.precision() + slopeExy.scale() > 20 || slopeEx2.precision() + slopeEx2.scale() > 20) {
					// if ( slopeInsertsSinceReset > 10000) {
					resetSlopeVars();
					// System.out.println("reset");
				} else {
					// count how many inserts since resetSlopeVars()
					slopeInsertsSinceReset++;
					// as we evict the oldest element, we also evict it's position x from the sum of
					// x
					long removedIndex = slopeInsertsSinceReset - circularArray.length;
					// sum of all X positions...
					// Clever: add new X and remove old X is same as adding window length
					slopeEx = slopeEx + circularArray.length;
					// Update the sum of x*y
					// slopeExy = slopeExy + (slopeInsertsSinceReset * elementInserted) -
					// (removedIndex * elementEvicted);

					BigDecimal xy = new BigDecimal(
							((slopeInsertsSinceReset * elementInserted) - (removedIndex * elementEvicted)));
					slopeExy = slopeExy.add(xy, Constants.MATH_CONTEXT);

					// Update the sum of x square
					BigDecimal x2 = new BigDecimal(
							(slopeInsertsSinceReset * slopeInsertsSinceReset) - (removedIndex * removedIndex));

					slopeEx2 = slopeEx2.add(x2, Constants.MATH_CONTEXT);

				}

				// set the Regression Slope for this window.
				windowSummary.setSlope(computeWindowSlope());

				/*
				 * if(windowSummary.getCount() > 1 && windowSummary.getSlope() != 1) {
				 * System.out.println("				count = " + windowSummary.getCount() +
				 * "\r\n				min = " + windowSummary.getMin() +
				 * "\r\n				max = " + windowSummary.getMax() +
				 * 
				 * "\r\n				slopeEx = " + slopeEx + "\r\n				slopeExy = "
				 * + slopeExy + "\r\n				slopeEx2 = " + slopeEx2 +
				 * "\r\n				slope = " + windowSummary.getSlope()); System.exit(0); }
				 */
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
		else if (initialQueue.size() >= 1) {

			// create Double object for arrayDeque.
			Double elementAsDouble = elementInserted;
			initialQueue.add(elementAsDouble);

			// if Count is required...
			if (requiresCount) {
				windowSummary.incrementCount();
			}

			// if SUM is required...
			if (requiresSum) {
				windowSummary.sumAdd(elementInserted);
			}
			// if MAX is required
			if (requiresMax && elementInserted > windowSummary.getMax()) {
				windowSummary.setMax(elementInserted);
			}
			// if MIN is required...
			if (requiresMin && elementInserted < windowSummary.getMin()) {
				windowSummary.setMin(elementInserted);
			}

			// If either Median or Mode is required...
			if (sortedElementsRequired) {
				// Very similar to what we did for a full window,
				// except there's no evicted element to remove.
				// If elementInserted is already in the tree, increment it's counter Integer
				Counter c = sortedElements.get(elementAsDouble);
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

				// If MEDIAN is required...
				if (requiresMedian) {
					// now determine if the median changed.
					// the logic:
					// if inserted == median, and the count is now odd, median slides up
					if (elementInserted == medianLocalVar) {
						medianDuplicates++;
						if (initialQueue.size() % 2 != 0) {
							medianMarker++;
							// windowSummary.setMedian(computeWindowMedian());
						}
					}
					// If inserted > median, and the count is now now odd: median slides up
					else if (elementInserted > medianLocalVar && initialQueue.size() % 2 != 0) { //
						if (medianSlideUp()) {
							// windowSummary.setMedian(computeWindowMedian());
						}
					}
					// If inserted < median, count is now even: median slides down
					else if (elementInserted < medianLocalVar && initialQueue.size() % 2 == 0) {
						if (medianSlideDown()) {
							// windowSummary.setMedian(computeWindowMedian());
						}
					}
					windowSummary.setMedian(computeWindowMedian());
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

			// if SLOPE is required...
			if (requiresSlope) {
				// count up the inserts since last slope reset.
				slopeInsertsSinceReset++;
				// sum of all X positions...
				slopeEx = slopeEx + slopeInsertsSinceReset;
				// sum of x*y
				BigDecimal xy = new BigDecimal((slopeInsertsSinceReset * elementInserted));
				slopeExy = slopeExy.add(xy, Constants.MATH_CONTEXT);
				// sum of x square
				BigDecimal x2 = new BigDecimal((slopeInsertsSinceReset * slopeInsertsSinceReset));
				slopeEx2 = slopeEx2.add(x2, Constants.MATH_CONTEXT);
				// call reusable function to finish computing windowSlope variable.
				windowSummary.setSlope(computeWindowSlope());
			}
			// if variance is required
			if (requiresVariance) {
				// computeWindowVariance();
				windowSummary.varRunningSumAdd(
						computeVarianceAddend(elementInserted, windowSummary.getAvg(), windowSummary.getSum()));
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
			initialQueue.add(elementInserted);
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
			// if SUM is quired
			if (requiresSum) {
				windowSummary.setSum(elementInserted);
			}
			// if a sortedTree is being used..
			if (sortedElementsRequired) {
				sortedElements.put(elementInserted, new Counter());
				if (requiresMedian) {
					medianLocalVar = elementInserted;
					windowSummary.setMedian(elementInserted);
					medianDuplicates = 0;
					medianMarker = 0;
				}
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
			// if Slope is required
			if (requiresSlope) {
				slopeInsertsSinceReset = 1; // starting...
				slopeEx = 1; // starting
				slopeExy = new BigDecimal(elementInserted); // x is 1. x*y = y
				slopeEx2 = Constants.ONE; // starting
				windowSummary.setSlope(computeWindowSlope()); // set windowSummary.slope value.
			}
			// if variance is required
			if (requiresVariance) {
				// computeWindowVariance();
				windowSummary.varRunningSumAdd(
						computeVarianceAddend(elementInserted, windowSummary.getAvg(), windowSummary.getSum()));
			}
		}
		// If the queue just got full, time to switch from ArrayDeque to circular array
		if (!windowSummary.isFull() && initialQueue.size() == range) {
			convertToFullWindow();
			windowSummary.setFull(true);
			windowSummary.setCount(circularArray.length); // redundant
		}

		/// TEST COMPUTED VALUES AGAINST A TRADITIONAL COMPUTATION
		/// only here until we assert with JUnit
		/// This section can be commented out.
		/*
		 * //printElements(); double arr[] = null; if(windowSummary.isFull()) { arr =
		 * Audit.setCircularArrayToZero(circularArray, circularArrayMarker); } else
		 * if(initialQueue.size() >= 1) { Object[] t = initialQueue.toArray(); arr = new
		 * double[t.length]; for(int i = 0; i < t.length; i++) { arr[i] = (double) t[i];
		 * } } if(arr != null) { double sortedArr[] = Audit.sortArray(arr);
		 * if(requiresMedian) { double median = Audit.getMedian(sortedArr); if(median !=
		 * windowSummary.getMedian()) {
		 * System.out.println("median drift! RioDB: "+windowSummary.getMedian() +
		 * " Audit: "+ median); //printElements();
		 * 
		 * System.out.println("medianLocalVar: "+medianLocalVar+
		 * "   medianDuplicates: "+ medianDuplicates + "   medianMarker: "+
		 * medianMarker+ "   count: "+ windowSummary.getCount() + "   size: "+
		 * arr.length );
		 * 
		 * System.exit(0); } } if(requiresSlope) { double slope = Audit.getSlope(arr);
		 * if(slope != windowSummary.getSlope() && windowSummary.getCount() > 2) {
		 * System.out.println("slope drift! RioDB: "+windowSummary.getSlope() +
		 * " Audit: "+ slope); printElements(); System.exit(0); } } if(requiresVariance)
		 * { double variance = Audit.getVariance(arr); if(variance !=
		 * windowSummary.getPopulationVariance()) {
		 * System.out.println("variance drift! RioDB: "+windowSummary.
		 * getPopulationVariance() + " Audit: "+ variance); printElements();
		 * System.out.println("adding "+elementInserted + "  count: "+ getCount());
		 * System.exit(0); } } }
		 */

	}

	// procedure to add element to array and remove oldest lement in one shot
	private double pushAndPoll(double in) {
		// swaps array entry old for new, at marker
		double ret = circularArray[circularArrayMarker];
		circularArray[circularArrayMarker] = in;
		circularArrayMarker++;

		// if marker exceeds array, return to 0
		if (circularArrayMarker == circularArray.length) {
			circularArrayMarker = 0;
		}
		return ret;
	}

	// Procedure to convert from arrayDeque to fixed recycling array:
	private void convertToFullWindow() {
		circularArray = new double[initialQueue.size()];
		// fill array with contents of arrayDeque

		if (requiresMax && range >= threshholdForUsingBuckets) {
			pageSize = pageSizeDefault;
			maxPaginated = new double[(range / pageSize) + 1];
		}
		if (requiresMin && range >= threshholdForUsingBuckets) {
			pageSize = pageSizeDefault;
			minPaginated = new double[(range / pageSize) + 1];
		}

		int pageCount = 0;
		int pageIndex = 0;
		double pageMax = Double.MIN_VALUE;
		double pageMin = Double.MAX_VALUE;

		for (int i = 0; i < circularArray.length; i++) {
			circularArray[i] = initialQueue.poll().doubleValue();

			if (pageSize > 0) {
				if (requiresMax && circularArray[i] > pageMax) {
					pageMax = circularArray[i];
				}
				if (requiresMin && circularArray[i] < pageMin) {
					pageMin = circularArray[i];
				}
				pageCount++;
				if (pageCount == pageSize) {
					pageCount = 0;
					if (requiresMax) {
						maxPaginated[pageIndex] = pageMax;
						pageMax = Double.MIN_VALUE;
					}
					if (requiresMin) {
						minPaginated[pageIndex] = pageMin;
						pageMin = Double.MAX_VALUE;
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
		initialQueue = null;

		// clean up memory...
		System.gc();
	}

	// procedure to find a new max within a page
	// called when the max of a page is evicted
	// should never be called when not using paginated max
	private void computePageMax(int thisArrayMarker) {
		int start = (thisArrayMarker) / 100;
		start = start * 100;
		int end = start + 100;
		if (end >= circularArray.length) {
			end = circularArray.length;
		}
		double max = circularArray[start];
		for (int i = start; i < end; i++) {
			if (circularArray[i] > max) {
				max = circularArray[i];
			}
		}
		maxPaginated[thisArrayMarker / 100] = max;
	}

	// procedure to find a new min within a page
	// called when the min of a page is evicted
	// should never be called when not using paginated Min
	private void computePageMin(int thisArrayMarker) {
		int start = (thisArrayMarker) / 100;
		start = start * 100;
		int end = start + 100;
		if (end >= circularArray.length) {
			end = circularArray.length;
		}
		double min = circularArray[start];
		for (int i = start; i < end; i++) {
			if (circularArray[i] < min) {
				min = circularArray[i];
			}
		}
		minPaginated[thisArrayMarker / 100] = min;
	}

	// Procedure called when we need to traverse all the pages looking for a new
	// overall MAX
	// it updates windowSummar.max
	private double computeWindowMax() {
		// If the window size is greater than threshholdForUsingBuckets, we're using
		// pagination
		if (pageSize > 0) {
			double tempMax = maxPaginated[0];
			// loop through circular array looking for a greater value
			for (int i = 1; i < maxPaginated.length; i++) {
				if (maxPaginated[i] > tempMax) {
					tempMax = maxPaginated[i];
				}
			}
			return tempMax;
		}
		// if the window size is smaller than threshholdForUsingBuckets, we're not using
		// pagination.
		else {
			double tempMax = circularArray[0];
			// loop through circular array looking for a greater value
			for (int i = 1; i < circularArray.length; i++) {
				if (circularArray[i] > tempMax) {
					tempMax = circularArray[i];
				}
			}
			return tempMax;
		}
	}

	// Procedure called when we need to traverse all the pages looking for a new
	// overall MIN
	private double computeWindowMin() {
		if (pageSize > 0) {
			double tempMin = minPaginated[0];
			// loop through circular array looking for a greater value
			for (int i = 1; i < minPaginated.length; i++) {
				if (minPaginated[i] < tempMin) {
					tempMin = minPaginated[i];
				}
			}
			return tempMin;
		} else {
			double tempMin = circularArray[0];
			// loop through circular array looking for a greater value
			for (int i = 1; i < circularArray.length; i++) {
				if (circularArray[i] < tempMin) {
					tempMin = circularArray[i];
				}
			}
			return tempMin;
		}
	}

	// compute the Mode of this window
	private double computeWindowMode() {// (double elementInserted) {
		int highestCounter = 0;
		double newMode = 0;
		// loop through sorted treemap looking for the key with highest Counter.
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

		// float d = (float) ((getCount() * slopeExy - slopeEx * windowSummary.getSum()) / (getCount() * slopeEx2 - slopeEx * slopeEx));
		/// or (part a - part b) / (part c - part d )

		// the formula above executed with BigDecimal operations:

		BigDecimal count = new BigDecimal(getCount());

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

		if (getCount() > 1 && slopeEx > 0)
			a = a.divide(c, Constants.MATH_CONTEXT);

		return a.setScale(3, BigDecimal.ROUND_HALF_DOWN).floatValue();
	}

	// called to reset slope running sums when too large.
	private void resetSlopeVars() {
		slopeInsertsSinceReset = circularArray.length;
		slopeEx = ((1 + slopeInsertsSinceReset) * slopeInsertsSinceReset) / 2;
		slopeEx2 = new BigDecimal(
				(slopeInsertsSinceReset * (slopeInsertsSinceReset + 1) * (2 * slopeInsertsSinceReset + 1)) / 6);

		slopeExy = new BigDecimal(0);
		int incr = circularArray.length;
		int sub = circularArrayMarker - 1;
		for (int i = 0; i < circularArray.length; i++) {
			if (incr > 0 && i == circularArrayMarker) {
				incr = 0;
			}
			BigDecimal incrB = new BigDecimal(((i - sub + incr) * circularArray[i]));
			slopeExy = slopeExy.add(incrB);
		}

	}

	// calculates a weighted delta to be added to the
	// variance Sum of squared deltas.
	private BigDecimal computeVarianceAddend(double elementInserted, double mean, double sum) {

		int count = getCount();
		// assuming that count has already been increased by 1
		// BigDecimal representation of count (for BigDecimal computation)
		BigDecimal countB = new BigDecimal(count);
		// BigDecimal representation of the element inserted
		BigDecimal elementB = new BigDecimal(elementInserted);
		// calculate the previous mean prior to this element being inserted.
		double originalMean = 0;
		if (count > 1) {
			// the previous mean was = (current_sum - new_element) / (count-1)
			originalMean = (double) ((sum - elementInserted) / (count - 1));
		}
		BigDecimal originalMeanB = new BigDecimal(originalMean);
		// BigDecimal computation of the delta between mean and new element
		BigDecimal delta = originalMeanB.subtract(elementB);
		// BigDecimal representation of the weighted delta
		// delta =((count - 1) / count) * delta^2
		BigDecimal weightedDelta = countB.add(Constants.NEGATIVE_ONE).divide(countB, Constants.MATH_CONTEXT)
				.multiply(delta, Constants.MATH_CONTEXT).multiply(delta, Constants.MATH_CONTEXT);
		// varRunningSum = varRunningSum.add(d,Constants.mathContext);

		return weightedDelta;

		// System.out.println("add "+ elementInserted +" n "+ count +" sum
		// "+varRunningSum.toPlainString()+
		// " avg "+ getAvg() + " delta "+ delta.toPlainString()+" var = " +
		// getPopulationVariance() );

	}

	// calculates a weighted delta to be removed from the
	// variance Sum of squared deltas.
	private BigDecimal computeVarianceSubtrahand(double elementInserted, double elementEvicted, double mean, double sum,
			int count) {
		// BigDecimal representation of the Count
		BigDecimal countB = new BigDecimal(count);
		BigDecimal countMinusOneB = new BigDecimal(count - 1);
		// BigDecimal representation of the element
		// BigDecimal elementInsertedB = new BigDecimal(elementInserted);
		BigDecimal elementEvictedB = new BigDecimal(elementEvicted);
		// calculate the original mean prior to these elements being added & removed
		// the previous mean was = (current_sum + element_removed - element_inserted) /
		// (count)
		BigDecimal originalMeanB = new BigDecimal((double) (sum + elementEvicted - elementInserted) / count,
				Constants.MATH_CONTEXT);
		// BigDecimal computation of the delta between mean and evicted element
		BigDecimal delta = originalMeanB.subtract(elementEvictedB);
		// weighted delta = delta^2 * count / (count-1)
		BigDecimal weightedDelta = delta.multiply(delta, Constants.MATH_CONTEXT)
				.multiply(countB, Constants.MATH_CONTEXT).divide(countMinusOneB, Constants.MATH_CONTEXT);
		return weightedDelta;
	}

	// get size
	private int getCount() {
		if (windowSummary.isFull()) {
			return circularArray.length;
		}
		return initialQueue.size();
	}

	private double getFirst() {
		if (circularArray != null) {
			return circularArray[circularArrayMarker];
		}
		return initialQueue.peekFirst().doubleValue();
	}

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
		// if there are even numbers in the set...
		if (getCount() % 2 == 0 && medianDuplicates == medianMarker && sortedElements.size() > 1) {
			double tempMedian = (medianLocalVar + sortedElements.higherEntry(medianLocalVar).getKey().doubleValue())
					/ 2;
			return tempMedian;
		} else {
			return medianLocalVar;
		}
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
	public WindowSummary trimAndGetWindowSummaryCopy(int currentSecond) {
		// window of quantity does NOT trim by time... ignore trim...
		return new WindowSummary(windowSummary);
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

	// for debugging. Prints to screen.
	@Override
	public void printElements() {
		String string = "";
		if (windowSummary.isFull()) {
			string = "Array\t";
			for (int i = circularArrayMarker; i < circularArray.length; i++) {
				string = string + circularArray[i] + "\t";
			}
			for (int i = 0; i < circularArrayMarker; i++) {
				string = string + circularArray[i] + "\t";
			}
		} else {
			string = "Queue\t";
			for (Double f : initialQueue) {
				string = string + f + "\t";
			}
		}
		System.out.println(string);
		if (sortedElements != null) {
			string = "sortedElements:";
			for (Entry<Double, Counter> entry : sortedElements.entrySet()) {
				string = string + "\t" + entry.getKey() + "[" + entry.getValue().intValue() + "]";
			}
			System.out.println(string);
		}

	}

	@Override
	public int trimExpiredWindowElements(int currentSecond) {
		// not applicable. Just here to satisfy interface.
		return 0;
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
	public int getRange() {
		return range;
	}

	@Override
	public boolean isDueForExpiration(int currentSecond) {
		if (currentSecond - lastEntryTime > Window.GRACE_PERIOD) {
			return true;
		}
		return false;
	}

}
