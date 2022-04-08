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

public interface WindowSummaryInterface {
	
	public void decrementCount();
	public String getAll();
	public double getAvg();
	public int getCount();
	public int getCountDistinct();
	public double getFirst();
	public double getLast();
	public double getMax();
	public double getMedian();
	public double getMin();
	public double getMode();
	public double getPrevious();
	public float getSlope();
	public double getSampleStdDev();
	public double getPopulationStdDev();
	public double getSum();
	public double getSampleVariance();
	public double getPopulationVariance();
	public void incrementCount();
	public void incrementCount(int addend);
	public boolean isEmpty();
	public boolean isFull();
	public void setCount(int count);
	public void setCountDistinct(int countDistinct);
	public void setFirst(double first);
	public void setFull(boolean full);
	public void setLast(double last);
	public void setMax(double max);
	public void setMedian(double median);
	public void setMin(double min);
	public void setMode(double mode);
	public void setPrevious(double previous);
	public void setSlope(float slope);
	public void setSum(double sum);
	public void sumAdd(double f);
	public void sumSubtract(double f);
}
