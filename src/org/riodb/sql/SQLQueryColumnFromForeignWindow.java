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

import org.riodb.engine.RioDB;
import org.riodb.windows.WindowSummary;

import org.riodb.plugin.RioDBStreamEvent;

public class SQLQueryColumnFromForeignWindow implements SQLQueryColumn {
	
	private int streamId;
	private int windowId;
	private int functionId;
	private String heading;
	
	SQLQueryColumnFromForeignWindow(int streamId, int windowId, int functionId, String heading){
		this.streamId = streamId;
		this.windowId = windowId;
		this.functionId = functionId;
		this.heading = heading;
	}
	@Override
	public String getValue(RioDBStreamEvent event, WindowSummary[] windowSummaries) throws ExceptionSQLExecution {

		
		// moved LAST and SUM to top because of popularity
		if(functionId == 5) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getLast());
		}
		else if(functionId == 16) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getSum());
		}

		
		else if(functionId == 0) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getAvg());
		}
		else if(functionId == 1) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getCount());
		}
		else if(functionId == 2) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getCountDistinct());
		}
//		else if(functionId == 3) {
//			return String.valueOf(RioDB.rio.getStreamMgr().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getCountIf());
//		}
		else if(functionId == 4) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getFirst());
		}
		else if(functionId == 6) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getMax());
		}
		else if(functionId == 7) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getMedian());
		}
		else if(functionId == 8) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getMin());
		}
		else if(functionId == 9) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getMode());
		}
		else if(functionId == 10) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getPopulationStdDev() ) ;
		}
		else if(functionId == 11) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getPopulationVariance());
		}
		else if(functionId == 12) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getPrevious());
		}
		else if(functionId == 13) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getSampleStdDev());
		}
		else if(functionId == 14) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getSampleVariance());
		}
		else if(functionId == 15) {
			return String.valueOf(
					RioDB.rio.getEngine().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getSlope());
		}
//		else if(functionId == 17) {
//			return String.valueOf(RioDB.rio.getStreamMgr().getStream(streamId).getWindowMgr().getWindow(windowId).getWindowSummary().getSumIf());
//		}
		return "";
	}

	@Override
	public String getHeading() {
		return heading;
	}

	

}
