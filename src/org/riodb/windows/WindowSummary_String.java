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

public class WindowSummary_String implements WindowSummaryInterface_String {

	private boolean full;
	private int count;
	private int countDistinct;
	private String first;
	private String last;
	private String max;
	private String min;
	private String mode;
	private String previous;

	WindowSummary_String() {
		full = false;
		count = 0;
		countDistinct = 0;
		first =  null;
		last =  null;
		max = null;
		min = null;
		mode =  null;
		previous =  null;
	};

	// constructor for clone
	WindowSummary_String(WindowSummary_String source) {

		this.full = source.full;
		this.count = source.count;
		this.countDistinct = source.countDistinct;
		this.first = source.first;
		this.last = source.last;
		this.max = source.max;
		this.min = source.min;
		this.mode = source.mode;
		this.previous = source.previous;
	};

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
	public void incrementCount(int addend) {
		this.count += addend;
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
	public String getFirst() {
		return first;
	}

	@Override
	public void setFirst(String first) {
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
	public String getLast() {
		return last;
	}

	@Override
	public void setLast(String last) {
		this.last = last;
	}

	@Override
	public String getMax() {
		return max;
	}

	@Override
	public void setMax(String max) {
		this.max = max;
	}

	@Override
	public String getMin() {
		return min;
	}

	@Override
	public void setMin(String min) {
		this.min = min;
	}

	@Override
	public String getMode() {
		return mode;
	}

	@Override
	public void setMode(String mode) {
		this.mode = mode;
	}

	@Override
	public String getPrevious() {
		return previous;
	}

	@Override
	public void setPrevious(String previous) {
		this.previous = previous;
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
		s += "\nmin " + min;
		s += "\nmode " + mode;
		s += "\nprevious " + previous;
	
		return s;
	}

	@Override
	public boolean isEmpty() {
		return count == 0;
	}

}
