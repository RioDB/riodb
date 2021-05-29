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

public class CircularArray {

	private float[] array;
	private int		marker;
	
	CircularArray(int size){
		array = new float[size];
		marker = 0;
	}
	
	public float pushAndPoll(float in) {
		float ret = array[marker];
		array[marker] = in;
		marker++;
		if(marker == array.length)
			marker = 0;
		
		return ret;
	}
	public int size() {
		return array.length;
	}
	
	public float peekFirst() {
		return array[marker];
	}
	public float peekLast() {
		if(marker+1 < array.length)
			return array[marker+1];
		else
			return array[0];
	}
	
	public float getStdDev(float avg) {
		double s = 0;

		for (float f : array) {
			s += Math.pow(f - avg, 2);
		}
		s = Math.sqrt((s / avg));

		return (float) s;
	}
	public float findMax() {
		float max = array[0];
		for (int i = 1; i < array.length; i++) {
			if (array[i] > max)
				max = array[i];
		}
		return max;
	}
	public float findMin() {
		float min = array[0];
		for (int i = 1; i < array.length; i++) {
			if (array[i] < min)
				min = array[i];
		}
		return min;
	}
}
