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

public interface WindowSummaryInterface_String {
	
	public void decrementCount();
	public String getAll();
	public int getCount();
	public int getCountDistinct();
	public String getFirst();
	public String getLast();
	public String getMax();
	public String getMin();
	public String getMode();
	public String getPrevious();
	public void incrementCount();
	public void incrementCount(int addend);
	public boolean isEmpty();
	public boolean isFull();
	public void setCount(int count);
	public void setCountDistinct(int countDistinct);
	public void setFirst(String first);
	public void setFull(boolean full);
	public void setLast(String last);
	public void setMax(String max);
	public void setMin(String min);
	public void setMode(String mode);
	public void setPrevious(String previous);
}
