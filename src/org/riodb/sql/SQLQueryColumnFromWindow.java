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

package org.riodb.sql;

import org.riodb.windows.WindowSummary;

import org.riodb.plugin.RioDBStreamMessage;

public class SQLQueryColumnFromWindow implements SQLQueryColumn {
	
	private int windowId;
	private int functionId;
	private String heading;
	
	SQLQueryColumnFromWindow(int windowId, int functionId, String heading){
		this.windowId = windowId;
		this.functionId = functionId;
		this.heading = heading;
	}
	@Override
	public String getValue(RioDBStreamMessage message, WindowSummary[] windowSummaries) throws ExceptionSQLExecution {

		
		// moved LAST and SUM to top because of popularity
		if(functionId == 5) {
			return String.valueOf(windowSummaries[windowId].getLast());
		}
		else if(functionId == 16) {
			return String.valueOf(windowSummaries[windowId].getSum());
		}

		
		else if(functionId == 0) {
			return String.valueOf(windowSummaries[windowId].getAvg());
		}
		else if(functionId == 1) {
			return String.valueOf(windowSummaries[windowId].getCount());
		}
		else if(functionId == 2) {
			return String.valueOf(windowSummaries[windowId].getCountDistinct());
		}
//		else if(functionId == 3) {
//			return String.valueOf(windowSummaries[windowId].getCountIf());
//		}
		else if(functionId == 4) {
			return String.valueOf(windowSummaries[windowId].getFirst());
		}
		else if(functionId == 6) {
			return String.valueOf(windowSummaries[windowId].getMax());
		}
		else if(functionId == 7) {
			return String.valueOf(windowSummaries[windowId].getMedian());
		}
		else if(functionId == 8) {
			return String.valueOf(windowSummaries[windowId].getMin());
		}
		else if(functionId == 9) {
			return String.valueOf(windowSummaries[windowId].getMode());
		}
		else if(functionId == 10) {
			return String.valueOf(windowSummaries[windowId].getPopulationStdDev() ) ;
		}
		else if(functionId == 11) {
			return String.valueOf(windowSummaries[windowId].getPopulationVariance());
		}
		else if(functionId == 12) {
			return String.valueOf(windowSummaries[windowId].getPrevious());
		}
		else if(functionId == 13) {
			return String.valueOf(windowSummaries[windowId].getSampleStdDev());
		}
		else if(functionId == 14) {
			return String.valueOf(windowSummaries[windowId].getSampleVariance());
		}
		else if(functionId == 15) {
			return String.valueOf(windowSummaries[windowId].getSlope());
		}
//		else if(functionId == 17) {
//			return String.valueOf(windowSummaries[windowId].getSumIf());
//		}
		return "";
	}

	@Override
	public String getHeading() {
		return heading;
	}

	

}
