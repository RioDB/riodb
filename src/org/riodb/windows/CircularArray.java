package org.riodb.windows;
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

	A circular array used for storing values that need to wait for range end date. 
	For example, when using range 1000-100, messages have to wait 100 before 
	getting processed into windows. 

*/

public class CircularArray {

	// array
	private double[] buffer;
	// index of the element where the circular array begins
	private int marker;
	// flag if the array is full, or n has been received.
	private boolean full;

	// constructor
	CircularArray(int size) {
		buffer = new double[size];
		marker = 0;
		full = false;
	}

	/*
	// function to get the greatest value in a range of values.
	private double getSegmentMax(int segmentStart, int segmentSize) {

		int start = (segmentStart) / segmentSize;
		// devide by 100 and multiply by 100 in order to round it to hundreds
		start = start * segmentSize;
		int end = start + segmentSize;
		if (end >= buffer.length) {
			end = buffer.length;
		}
		double max = buffer[start];
		for (int i = start; i < end; i++) {
			if (buffer[i] > max) {
				max = buffer[i];
			}
		}
		return max;
	}

	// function to get the smallest value in a range of values.
	private double getSegmentMin(int segmentStart, int segmentSize) {

		int start = (segmentStart) / segmentSize;
		// devide by 100 and multiply by 100 in order to round it to hundreds
		start = start * segmentSize;
		int end = start + segmentSize;
		if (end >= buffer.length) {
			end = buffer.length;
		}
		double min = buffer[start];
		for (int i = start; i < end; i++) {
			if (buffer[i] < min) {
				min = buffer[i];
			}
		}
		return min;
	}
	*/
	// check if buffer is full
	public boolean isFull() {
		return full;
	}

	// put element into ring buffer
	public void put(double d) {

		buffer[marker] = d;
		marker++;
		if (marker == buffer.length) {
			marker = 0;
			full = true;
		}

	}

	// puts a value and pops the oldest value
	public double putAndPop(double d) {

		// swaps array entry old for new, at marker
		double ret = buffer[marker];
		buffer[marker] = d;
		marker++;
		// if marker exceeds array, return to 0
		if (marker == buffer.length) {
			marker = 0;
		}
		return ret;

	}
	
	// get queueSize -- elements waiting
	public int size() {
		if (isFull()) {
			return buffer.length;
		}
		return marker;
	}

}
