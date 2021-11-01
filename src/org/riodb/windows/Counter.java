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

   A mutable counter variable class
      
*/



package org.riodb.windows;

public class Counter {
	// counter value is x
	private int n;

	// constructor starts with 1
	public Counter() {
		n = 1;
	}

	// constructor starts with n
	public Counter(int n) {
		this.n = n;
	}

	// compare counter with another int value
	public int compare(int y) {
		return (n < y) ? -1 : ((n == y) ? 0 : 1);
	}
	
	// single method decrements counter and checks if it reaches zero
	public boolean decrementReachZero() {
		if (--n == 0) {
			return true;
		}
		return false;
	}
	
	// gets n
	public int intValue() {
		return n;
	}
	
	// increment n
	public void increment() {
		n++;
	}
	
	// check if n equals int
	public boolean isEQ(int y) {
		return n == y;
	}

	// check if n is greater than int
	public boolean isGT(int y) {
		return n > y;
	}

	// check if n is less than int
	public boolean isLT(int y) {
		return n < y;
	}
}
