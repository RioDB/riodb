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

public class WindowSummaryOfOne implements WindowSummaryInterface {

	private boolean full;
	private double value;
	private double previous;

	WindowSummaryOfOne() {
		full = false;
	};

	// clone
	WindowSummaryOfOne(WindowSummaryOfOne source) {
			this.full = source.full;
			this.value = source.value;
			this.previous = source.previous;
		};

	@Override
	public double getAvg() {
		return value;
	}

	@Override
	public int getCount() {
		return full == true ? 1 : 0;
	}

	@Override
	public void setCount(int count) {

	}

	@Override
	public void incrementCount() {

	}

	@Override
	public void decrementCount() {

	}

	@Override
	public int getCountDistinct() {
		return getCount();
	}

	@Override
	public void setCountDistinct(int countDistinct) {

	}

	@Override
	public double getFirst() {
		return value;
	}

	@Override
	public void setFirst(double first) {

	}

	@Override
	public boolean isEmpty() {
		return !full;
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
		return value;
	}

	@Override
	public void setLast(double last) {
		this.value = last;
		if (full == false) {
			full = true;
		}
	}

	@Override
	public double getMax() {
		return value;
	}

	@Override
	public void setMax(double max) {

	}

	@Override
	public double getMedian() {
		return value;
	}

	@Override
	public void setMedian(double median) {

	}

	@Override
	public double getMin() {
		return value;
	}

	@Override
	public void setMin(double min) {

	}

	@Override
	public double getMode() {
		return value;
	}

	@Override
	public void setMode(double mode) {

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
		return 0;
	}

	@Override
	public void setSlope(float slope) {

	}

	@Override
	public double getSum() {
		return value;
	}

	@Override
	public void setSum(double sum) {

	}

	@Override
	public void sumAdd(double f) {

	}

	@Override
	public void sumSubtract(double f) {

	}

	@Override
	public String getAll() {
		String s = "";

		s += "count: " + getCount();
		s += "\ncountDistinct: " + getCountDistinct();
		s += "\nfirst " + value;
		s += "\nlast " + value;
		s += "\nmax " + value;
		s += "\nmedian " + value;
		s += "\nmin " + value;
		s += "\nmode " + value;
		s += "\nprevious " + previous;
		s += "\nslope 0";
		s += "\nsum " + value;
		s += "\nvariance 0";

		return s;
	}

	@Override
	public double getSampleStdDev() {
		return 0;
	}

	@Override
	public double getPopulationStdDev() {
		return 0;
	}

	@Override
	public double getSampleVariance() {
		return 0;
	}

	@Override
	public double getPopulationVariance() {
		return 0;
	}
}
