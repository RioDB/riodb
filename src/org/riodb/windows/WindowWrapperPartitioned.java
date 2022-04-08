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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.riodb.engine.RioDB;
import org.riodb.sql.ExceptionSQLExecution;
import org.riodb.sql.SQLFunctionMap;
import org.riodb.sql.SQLWindowCondition;

import org.riodb.plugin.RioDBStreamMessage;

public class WindowWrapperPartitioned extends WindowWrapper {

	// what data stream these queries run against
	private HashMap<String, Window> windowMap;
	private int partitionByStringFieldId;

	public WindowWrapperPartitioned(int streamId, String windowName, Window window, int fieldId,
			SQLWindowCondition windowCondition, boolean rangeByTime, boolean rangeByTimeIsTimestamp,
			int partitionByStringColumnId) {

		super(streamId, windowName, window, fieldId, windowCondition, rangeByTime, rangeByTimeIsTimestamp);

		windowMap = new HashMap<String, Window>();

		this.partitionByStringFieldId = RioDB.rio.getEngine().getStream(streamId).getDef()
				.getStringFieldIndex(partitionByStringColumnId);

	}

	public String describeWindow() {
		String s = "{\"name\":\"" + windowName + "\",\n \"steam\":\""
				+ RioDB.rio.getEngine().getStream(streamId).getName() + "\",\n \"field\":\""
				+ RioDB.rio.getEngine().getStream(streamId).getDef().getNumericFieldName(numericFieldIndex)
				+ "\",\n \"where\": \"" + windowCondition.getExpression() + "\",\n \"running\":["
				+ defaultWindow.getAggregations() + "]" + ",\n \"partition_by\":\""
				+ RioDB.rio.getEngine().getStream(this.streamId).getDef().getStringFieldName(partitionByStringFieldId)
				+ "\",\n \"range_by\": ";
		if (rangeByTime) {
			if (rangeByTimeFieldNumericIndexId == -1) {
				s = s + "\"clock\"";
			} else {
				s = s + "\"" + RioDB.rio.getEngine().getStream(streamId).getDef()
						.getNumericFieldName(rangeByTimeFieldNumericIndexId) + "\"";
			}
		} else {
			s = s + "\"quantity\"";
		}

		s = s + ",\n \"range\": " + defaultWindow.getRange() + "}";
		return s;
	}

	@Override
	public WindowSummaryInterface getWindowSummary() {
		return null;
	}

	public WindowSummaryInterface getWindowSummary(String key) {
		if (windowMap.containsKey(key)) {
			return windowMap.get(key).getWindowSummaryCopy();
		}
		return null;
	}

	public WindowSummaryInterface putMessageRef(RioDBStreamMessage message, int currentSecond) {
		try {

			// if there's a required condition and it doesn't match
			Window w = windowMap.get(message.getString(partitionByStringFieldId));

			if (hasCondition && !windowCondition.match(message)) {
				// then we just read the summary. no updates made.
				if (w == null) {
					return null;
				} else if (rangeByTime && rangeByTimeIsTimestamp) {
					return w.trimAndGetWindowSummaryCopy(
							(int) (message.getDouble(rangeByTimeFieldNumericIndexId) / 1000d));
					// else (for window of quantity or range by clock, we pass current second
				} else {
					return w.trimAndGetWindowSummaryCopy(currentSecond);
				}
			} else {
				// there's no condition, or the condition matches. We update and read summary:
				double d = message.getDouble(numericFieldIndex);

				// for range by time using timestamp, we pass in the timestamp
				if (rangeByTime && rangeByTimeIsTimestamp) {
					// key exists:
					if (w != null) {
						return w.trimAddAndGetWindowSummaryCopy(d,
								(int) (message.getDouble(rangeByTimeFieldNumericIndexId) / 1000d));
					} else {
						// key not found. Make new window and put in hashmap.
						w = defaultWindow.makeEmptyClone();
						WindowSummaryInterface ws = w.trimAddAndGetWindowSummaryCopy(d,
								(int) (message.getDouble(rangeByTimeFieldNumericIndexId) / 1000d));

						windowMap.put(message.getString(partitionByStringFieldId), w);
						return ws;
					}
				}
				// else (for window of quantity or range by clock, we pass current second
				else {

					if (w != null) {
						return w.trimAddAndGetWindowSummaryCopy(d, currentSecond);
					} else {
						w = defaultWindow.makeEmptyClone();
						WindowSummaryInterface ws = w.trimAddAndGetWindowSummaryCopy(d, currentSecond);
						windowMap.put(message.getString(partitionByStringFieldId), w);
						return ws;
					}

				}
			}
		} catch (ExceptionSQLExecution e) {
			if (!errorAlreadyCaught) {
				e.printStackTrace();
				errorAlreadyCaught = true;
			}
			return null;
		}

	}

	public boolean windowRequiresFunction(int functionId) {
		if (functionId >= SQLFunctionMap.functionsAvailable() || functionId < 0)
			return false;
		return defaultWindow.requiresFunction(functionId);
	}

	public void trimExpiredWindowElements(int currentSecond) {

		Iterator<Map.Entry<String, Window>> iter = windowMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, Window> entry = iter.next();
			if (entry.getValue().isDueForExpiration(currentSecond)) {
				iter.remove();
				RioDB.rio.getSystemSettings().getLogger().debug("removed expired window.");
			} else {
				defaultWindow.trimExpiredWindowElements(currentSecond);
			}
		}
	}
	
	public void resetWindow() {
		windowMap = new HashMap<String, Window>();
	}
}
