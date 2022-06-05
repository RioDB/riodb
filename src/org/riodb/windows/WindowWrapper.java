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

import org.riodb.engine.RioDB;
import org.riodb.sql.ExceptionSQLExecution;
import org.riodb.sql.SQLAggregateFunctions;
import org.riodb.sql.SQLWindowCondition;
import org.riodb.sql.SQLWindowSourceExpression;
import org.riodb.plugin.RioDBStreamMessage;

public class WindowWrapper {

	// what data stream these queries run against
	protected int streamId;
	protected String windowName;

	protected boolean windowOfNumericExpression;
	protected SQLWindowSourceExpression windowSourceExpression;

	protected int numericFieldIndex;
	protected boolean hasCondition;

	protected boolean rangeByTime;
	protected boolean rangeByTimeIsTimestamp;
	protected int rangeByTimeFieldNumericIndexId;

	protected Window defaultWindow;
	protected SQLWindowCondition windowCondition;
	protected boolean errorAlreadyCaught;
	protected String  status;

	protected boolean keepPreviousMessage;
	protected RioDBStreamMessage previousMessage;
	protected RioDBStreamMessage currentMessage;
	protected boolean firstMessage;

	public WindowWrapper(int streamId, String windowName, Window window, int fieldId,
			SQLWindowCondition windowCondition, boolean rangeByTime, boolean rangeByTimeIsTimestamp,
			SQLWindowSourceExpression windowSourceExpression) {

		this.streamId = streamId;
		this.windowName = windowName;
		this.defaultWindow = window;

		this.windowOfNumericExpression = false;
		if (windowSourceExpression != null) {
			windowOfNumericExpression = true;
			this.windowSourceExpression = windowSourceExpression;
		} else {
			this.numericFieldIndex = RioDB.rio.getEngine().getStream(streamId).getDef().getNumericFieldIndex(fieldId);
		}

		this.rangeByTime = rangeByTime;
		this.rangeByTimeIsTimestamp = rangeByTimeIsTimestamp;
		if (rangeByTimeIsTimestamp)
			this.rangeByTimeFieldNumericIndexId = RioDB.rio.getEngine().getStream(streamId).getDef()
					.getTimestampNumericFieldId();

		this.windowCondition = windowCondition;
		hasCondition = false;
		if (windowCondition != null) {
			hasCondition = true;
		}

		errorAlreadyCaught = false;
		status = "ok";

		keepPreviousMessage = false;
		if (windowSourceExpression != null && windowSourceExpression.requiresPrevious()) {
			keepPreviousMessage = true;
			firstMessage = true;
		}
		previousMessage = null;
	}

	public String getName() {
		return windowName;
	}
	
	public String getStatus() {
		return status;
	}

	public String describeWindow() {

		String s = "{\"name\":\"" + windowName + "\",\n \"steam\":\""
				+ RioDB.rio.getEngine().getStream(streamId).getName() + "\",\n \"field\":\"";

		if (windowOfNumericExpression) {
			s = s + windowSourceExpression.getExpression();
		} else {
			s = s + RioDB.rio.getEngine().getStream(streamId).getDef().getNumericFieldName(numericFieldIndex);
		}

		s = s + "\",\n";

		if (windowCondition != null) {
			s = s + " \"where\": \"" + windowCondition.getExpression() + "\",\n";
		}

		s = s + " \"running\":[" + defaultWindow.getAggregations() + "]" + ",\n \"range_by\": ";
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

	public WindowSummaryInterface getWindowSummary() {
		return defaultWindow.getWindowSummaryCopy();
	}

	public WindowSummaryInterface putMessageRef(RioDBStreamMessage message, int currentSecond) {

		if (keepPreviousMessage) {
			previousMessage = currentMessage;
			currentMessage = message;
			if(firstMessage) {
				firstMessage = false;
				return new WindowSummary();
			}
		}

		try {

			// if there's a required condition and it doesn't match
			if (hasCondition && !windowCondition.match(message, previousMessage)) {
				// then we just read the summary. no updates made.
				if (rangeByTime && rangeByTimeIsTimestamp) {
					return defaultWindow.trimAndGetWindowSummaryCopy(
							(int) (message.getDouble(rangeByTimeFieldNumericIndexId) / 1000d));
				}
				// else (for window of quantity or range by clock, we pass current second
				else {
					return defaultWindow.trimAndGetWindowSummaryCopy(currentSecond);
				}

				// else... there's no condition, or the condition matches. We update and read
				// summary:
			} else {

				double d;

				// if this window is NOT sourced from an expression:
				if (!windowOfNumericExpression) {
					d = message.getDouble(numericFieldIndex);
				}
				// else, window is sourced from a numeric expression:
				else {
					d = windowSourceExpression.getNumber(message, previousMessage);
				}
				// for range by time using timestamp, we pass in the timestamp
				if (rangeByTime && rangeByTimeIsTimestamp) {
					return defaultWindow.trimAddAndGetWindowSummaryCopy(d,
							(int) (message.getDouble(rangeByTimeFieldNumericIndexId) / 1000d));
					// else (for window of quantity or range by clock, we pass current second
				} else {
					return defaultWindow.trimAddAndGetWindowSummaryCopy(d, currentSecond);
				}
			}

		} catch (ExceptionSQLExecution e) {
			if (!errorAlreadyCaught) {
				RioDB.rio.getSystemSettings().getLogger()
						.error("Window " + windowName + ": " + e.getMessage().replace("\n", " ").replace("\r", " "));
				errorAlreadyCaught = true;
				this.status = e.getMessage().replace("\"", "'").replace("\n", "\\t");
			}
			return null;
		}

	}

	public boolean windowRequiresFunction(int functionId) {
		if (functionId >= SQLAggregateFunctions.functionsAvailable() || functionId < 0)
			return false;
		return defaultWindow.requiresFunction(functionId);
	}

	public int getStreamId() {
		return streamId;
	}

	public void trimExpiredWindowElements(int currentSecond) {
		if (rangeByTime && rangeByTimeFieldNumericIndexId == -1) {
			defaultWindow.trimExpiredWindowElements(currentSecond);
		}
	}

	public void resetWindow() {
		defaultWindow = defaultWindow.makeEmptyClone();
		previousMessage = null;
	}

}
