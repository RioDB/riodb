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

public class WindowSummaryOfOne_String implements WindowSummaryInterface_String {

	private boolean full;
	private String value;
	private String previous;

	WindowSummaryOfOne_String() {
		full = false;
		value = null;
		previous = null;
	};

	// clone
	WindowSummaryOfOne_String(WindowSummaryOfOne_String source) {
		this.full = source.full;
		this.value = source.value;
		this.previous = source.previous;
	};

	@Override
	public int getCount() {
		return full == true ? 1 : 0;
	}

	@Override
	public void setCount(int count) {
		// not used
	}

	@Override
	public void incrementCount() {
		// not used
	}
	
	@Override
	public void incrementCount(int addend) {
		// not used
	}

	@Override
	public void decrementCount() {
		// not used
	}

	@Override
	public int getCountDistinct() {
		return getCount();
	}

	@Override
	public void setCountDistinct(int countDistinct) {
		// not used
	}

	@Override
	public String getFirst() {
		return value;
	}

	@Override
	public void setFirst(String first) {
		// not used
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
	public String getLast() {
		return value;
	}

	@Override
	public void setLast(String last) {
		this.value = last;
		if (full == false) {
			full = true;
		}
	}

	@Override
	public String getMax() {
		return value;
	}

	@Override
	public void setMax(String max) {
		// not used
	}

	@Override
	public String getMin() {
		return value;
	}

	@Override
	public void setMin(String min) {
		// not used
	}

	@Override
	public String getMode() {
		return value;
	}

	@Override
	public void setMode(String mode) {
		// not used
	}

	@Override
	public String getPrevious() {
		return previous;
	}

	@Override
	public void setPrevious(String previous) {
		this.previous = previous;
	}

	@Override
	public String getAll() {
		String s = "";

		s += "count: " + getCount();
		s += "\ncountDistinct: " + getCountDistinct();
		s += "\nfirst " + value;
		s += "\nlast " + value;
		s += "\nmax " + value;
		s += "\nmin " + value;
		s += "\nmode " + value;
		s += "\nprevious " + previous;

		return s;
	}

}
