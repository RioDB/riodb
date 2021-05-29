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

import java.util.Arrays;

public final class Audit {

	public static double getMax(double[] sortedGroup) {
		return sortedGroup[sortedGroup.length - 1];
	}

	public static double getMedian(double[] sortedGroup) {

		double m;

		if (sortedGroup.length % 2 == 0) {
			m = ((double) sortedGroup[sortedGroup.length / 2] + (double) sortedGroup[sortedGroup.length / 2 - 1]) / 2;
		} else {
			m = (double) sortedGroup[sortedGroup.length / 2];
		}

		return m;
	}

	public static double getMin(double[] sortedGroup) {
		return sortedGroup[0];
	}

	public static double getMode(double[] sortedGroup) {

		double mode = sortedGroup[0];
		int modeCount = 1;
		double lastVal = sortedGroup[0];
		int currentCount = 1;

		for (int i = 1; i < sortedGroup.length; i++) {
			if (sortedGroup[i] == lastVal) {
				currentCount++;
				if (currentCount > modeCount) {
					mode = sortedGroup[i];
					modeCount = currentCount;
				}
			} else {
				lastVal = sortedGroup[i];
				currentCount = 1;
			}
		}

		return mode;
	}

	public static double getSlope(double[] group) {

		/*
		 * 
		 * double d = (getCount() * slopeExy - slopeEx * sum) / (getCount() * slopeEx2 -
		 * slopeEx * slopeEx); return (double) d;
		 * 
		 */

		double slopeExy = group[0];
		double slopeEx2 = 1;
		double slopeEx = 1;
		double sum = group[0];
		for (int i = 1; i < group.length; i++) {
			slopeEx += (i + 1);
			slopeExy = slopeExy + ((i + 1) * group[i]);
			slopeEx2 = slopeEx2 + (i + 1) * (i + 1);
			sum += group[i];
		}

		double s = (group.length * slopeExy - slopeEx * sum) / (group.length * slopeEx2 - slopeEx * slopeEx);

		return (double) s;
	}

	public static double getVariance(double[] group) {

		double sum = 0;
		double mean;
		for (int i = 0; i < group.length; i++) {
			sum = sum + group[i];
		}
		mean = (double) sum / group.length;
		
		sum = 0;
		for (int i = 0; i < group.length; i++) {
			sum += (double) ((group[i] - mean)*(group[i] - mean));
		}
		mean = (double) sum / group.length;
		
		//double deviation = Math.sqrt(mean);
		return mean;
	}
	
	public static double[] sortArray(double group[]) {
		double[] newArray = new double[group.length];
		for(int i = 0; i < group.length; i++)
			newArray[i] = group[i];
		Arrays.sort(newArray);
		return newArray;
	}
	
	public static double[] setCircularArrayToZero(double group[], int start) {
		double[] newArr = new double[group.length];
		int counter = 0;
		for(int i = start; i < group.length; i++) {
			newArr[counter++] = group[i];
		}
		for(int i = 0; i < start; i++) {
			newArr[counter++] = group[i];
		}
		return newArr;
	}

}
