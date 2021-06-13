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

import java.math.BigDecimal;

public class WindowSummary implements WindowSummaryInterface {

	private boolean full;
	private int count;
	private int countDistinct;
	private double first;
	private double last;
	private double max;
	private double median;
	private double min;
	private double mode;
	private double previous;
	private float  slope;
	private double sum;

	// private double variance;
	// a running sum of squared differences
	// based on Welford's variance algorithm
	private BigDecimal varRunningSum;
	
	WindowSummary() {
		full = false;
		count = 0;
		countDistinct = 0;
		first = 0;
		last = 0;
		max = Float.MIN_NORMAL;
		median = 0;
		min = Float.MAX_VALUE;
		mode = 0;
		previous = 0;
		slope = 0;
		sum = 0;
		varRunningSum = null;
	};

	// constructor for clone
	WindowSummary(WindowSummary source) {

		this.full = source.full;
		this.count = source.count;
		this.countDistinct = source.countDistinct;
		this.first = source.first;
		this.last = source.last;
		this.max = source.max;
		this.median = source.median;
		this.min = source.min;
		this.mode = source.mode;
		this.previous = source.previous;
		this.slope = source.slope;
		this.sum = source.sum;
		this.varRunningSum = source.varRunningSum;
	};

	@Override
	public double getAvg() {
		return (double) sum / count;
	}

	@Override
	public int getCount() {
		return count;
	}

	@Override
	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void incrementCount() {
		this.count++;
	}

	@Override
	public void decrementCount() {
		this.count--;
	}

	@Override
	public int getCountDistinct() {
		return countDistinct;
	}

	@Override
	public void setCountDistinct(int countDistinct) {
		this.countDistinct = countDistinct;
	}

	@Override
	public double getFirst() {
		return first;
	}

	@Override
	public void setFirst(double first) {
		this.first = first;
	}

	@Override
	public boolean isFull() {
		return full;
	}

	@Override
	public void setFull(boolean full) {
		this.full = full;
	}

	@Override
	public double getLast() {
		return last;
	}

	@Override
	public void setLast(double last) {
		this.last = last;
	}

	@Override
	public double getMax() {
		return max;
	}

	@Override
	public void setMax(double max) {
		this.max = max;
	}

	@Override
	public double getMedian() {
		return median;
	}

	@Override
	public void setMedian(double median) {
		this.median = median;
	}

	@Override
	public double getMin() {
		return min;
	}

	@Override
	public void setMin(double min) {
		this.min = min;
	}

	@Override
	public double getMode() {
		return mode;
	}

	@Override
	public void setMode(double mode) {
		this.mode = mode;
	}

	@Override
	public double getPrevious() {
		return previous;
	}

	@Override
	public void setPrevious(double previous) {
		this.previous = previous;
	}

	@Override
	public float getSlope() {
		return slope;
	}

	@Override
	public void setSlope(float slope) {
		this.slope = slope;
	}

	@Override
	public double getSum() {
		return sum;
	}

	@Override
	public void setSum(double sum) {
		this.sum = sum;
	}

	@Override
	public void sumAdd(double f) {
		this.sum = this.sum + f;
	}

	@Override
	public void sumSubtract(double f) {
		this.sum = this.sum - f;
	}

	@Override
	public double getPopulationVariance() {
		if (count > 0 && varRunningSum != null)
			return varRunningSum.divide(new BigDecimal(count), Constants.MATH_CONTEXT).doubleValue();
		return Float.NaN;
	}

	@Override
	public double getSampleVariance() {
		if (count > 1 && varRunningSum != null)
			return varRunningSum.divide(new BigDecimal(count - 1), Constants.MATH_CONTEXT).doubleValue();
		return Float.NaN;
	}

	@Override
	public double getPopulationStdDev() {
		if (count > 0 && varRunningSum != null)
			return (double) Math.sqrt(this.getPopulationVariance());
		return Float.NaN;
	}

	@Override
	public double getSampleStdDev() {
		if (count > 1 && varRunningSum != null)
			return (double) Math.sqrt(this.getSampleVariance());
		return Float.NaN;
	}

	public BigDecimal getVarRunningSum() {
		return varRunningSum;
	}

	public void setVarRunningSum(double d) {
		this.varRunningSum = new BigDecimal(d);
	}

	public void setVarRunningSum(BigDecimal b) {
		this.varRunningSum = b;
	}

	protected void resetVarRunningSum() {
		this.varRunningSum = new BigDecimal(0);
	}

	// MUST ONLY BE CALLED AFTER COUNT IS UPDATED
	protected void varRunningSumAdd(BigDecimal weightedDelta) {
		varRunningSum = varRunningSum.add(weightedDelta, Constants.MATH_CONTEXT);
	}

	// MUST ONLY BE CALLED AFTER COUNT IS UPDATED
	protected void varRunningSumRemove(BigDecimal weightedDelta) {
		varRunningSum = varRunningSum.subtract(weightedDelta, Constants.MATH_CONTEXT);
	}

	// get all values
	@Override
	public String getAll() {
		String s = "";

		s += "count: " + count;
		s += "\ncountDistinct: " + countDistinct;
		s += "\nfirst " + first;
		s += "\nlast " + last;
		s += "\nmax " + max;
		s += "\nmedian " + median;
		s += "\nmin " + min;
		s += "\nmode " + mode;
		s += "\nprevious " + previous;
		s += "\nslope " + slope;
		s += "\nsum " + sum;
		s += "\npopulation variance " + getPopulationVariance();
		s += "\nsample variance " + getSampleVariance();
		s += "\npopulation deviation " + getPopulationStdDev();
		s += "\nsample deviation " + getSampleStdDev();

		return s;
	}

	@Override
	public boolean isEmpty() {
		return count == 0;
	}

}
