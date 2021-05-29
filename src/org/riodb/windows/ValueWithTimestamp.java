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

public class ValueWithTimestamp implements Comparable<ValueWithTimestamp> {
	private double value;
	private int second;

	ValueWithTimestamp(double value, int second) {
		this.value = value;
		this.second = second;
	}

	public double doubleValue() {
		return value;
	}

	public int getSecond() {
		return second;
	}

	public int compareTo(ValueWithTimestamp anotherVWT) {
		return Double.compare(value, anotherVWT.value);
	}
}
