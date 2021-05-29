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
 *  a mutable counter variable class
 */

package org.riodb.windows;

public class Counter {
	private int x;

	public Counter() {
		x = 1;
	}

	public Counter(int x) {
		this.x = x;
	}

	public int compare(int y) {
		return (x < y) ? -1 : ((x == y) ? 0 : 1);
	}
	public boolean decrementReachZero() {
		if (--x == 0) {
			return true;
		}
		return false;
	}
	public int intValue() {
		return x;
	}
	public void increment() {
		x++;
	}
	public boolean isEQ(int y) {
		return x == y;
	}

	public boolean isGT(int y) {
		return x > y;
	}

	public boolean isLT(int y) {
		return x < y;
	}
}
