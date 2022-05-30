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
 *   A container of Windows, so that multiple windows can be created
 *   in the same stream 
 */

package org.riodb.windows;

import java.util.ArrayList;

import org.riodb.engine.RioDB;

import org.riodb.plugin.RioDBStreamMessage;

public class WindowManager {

	private int streamId;

	// an arraylist of windows OF NUMERIC FIELD created for a parent stream.
	// the array index is the windowId
	private final ArrayList<WindowWrapper> windowWrapperList = new ArrayList<WindowWrapper>();

	// an arraylist of windows OF STRING FIELD created for a parent stream.
	// The ID of the window is always negative for Strings. So the ID is the ((index
	// * -1) -1)
	private final ArrayList<WindowWrapper_String> windowWrapperList_String = new ArrayList<WindowWrapper_String>();

	public WindowManager() {
		// this.streamId = streamId;
	}

	// streamId setter. It's not done in constructor because we're making
	// WIndowManager final.
	public void setStreamId(int streamId) {
		this.streamId = streamId;
	}

	// add a window to this stream's windowManager
	public void addWindow(WindowWrapper newWindow) {
		windowWrapperList.add(newWindow);
		RioDB.rio.getSystemSettings().getLogger().trace("    window '" + newWindow.getName() + "' added to WindowManager");
	}

	// add a window to this stream's windowManager
	public void addWindow_String(WindowWrapper_String newWindow) {
		windowWrapperList_String.add(newWindow);
		RioDB.rio.getSystemSettings().getLogger().trace("    window '" + newWindow.getName() + "' added to WindowManager");
	}

	// drop a window (sync in case of concurrent requests)
	public synchronized boolean dropWindow(String windowName) {

		for (int i = 0; i < windowWrapperList.size(); i++) {
			if (windowName.equals(windowWrapperList.get(i).getName())) {
				windowWrapperList.remove(i);
				return true;
			}
		}

		for (int i = 0; i < windowWrapperList_String.size(); i++) {
			if (windowName.equals(windowWrapperList_String.get(i).getName())) {
				windowWrapperList_String.remove(i);
				return true;
			}
		}

		return false;
	}

	// get count of windows in this stream windowManager
	public int getWindowCount() {
		return windowWrapperList.size() + windowWrapperList_String.size();
	}

	// get WindowWrapper by index
	public WindowWrapper getWindow(int index) {
		return windowWrapperList.get(index);
	}

	// get WindowWrapper by index
	public WindowWrapper_String getWindow_String(int index) {
		// Windows of String have negative ID.
		// window -1 = index 0
		// window -2 = index 1
		// window -3 = index 2
		return windowWrapperList_String.get((index + 1) * -1);
	}

	// get WindowWrapper by name
	public WindowWrapper getWindow(String windowName) {
		if (this.hasWindow_Number(windowName))
			for (WindowWrapper w : windowWrapperList) {
				if (w.getName().equals(windowName))
					return w;
			}
		return null;
	}

	// get WindowWrapper by name
	public WindowWrapper_String getWindow_String(String windowName) {
		if (this.hasWindow(windowName))
			for (WindowWrapper_String w : windowWrapperList_String) {
				if (w.getName().equals(windowName))
					return w;
			}
		return null;
	}

	// Check if window requires a certain function
	public boolean windowRequiresFunction(int windowId, int functionId) {
		if (windowId >= 0) {
			if (windowId >= windowWrapperList.size()) {
				return false;
			}
			return windowWrapperList.get(windowId).windowRequiresFunction(functionId);
		} else {
			// Windows of String have negative ID.
			// window -1 = index 0
			// window -2 = index 1
			// window -3 = index 2
			int convertedId = ((windowId + 1) * -1);
			if (convertedId >= windowWrapperList_String.size()) {
				return false;
			}
			return windowWrapperList_String.get(convertedId).windowRequiresFunction(functionId);
		}
	}

	// get window ID (index) by name
	public int getWindowId(String windowName) {
		for (int i = 0; i < windowWrapperList.size(); i++) {
			if (windowWrapperList.get(i).getName().equals(windowName))
				return i;
		}
		for (int i = 0; i < windowWrapperList_String.size(); i++) {
			if (windowWrapperList_String.get(i).getName().equals(windowName))
				return (i+1) * -1;
		}
		return Integer.MIN_VALUE;
	}

	// get window name by id
	public String getWindowName(int windowId) {

		if (windowId >= 0) {
			if (windowId >= windowWrapperList.size()) {
				return null;
			}
			return windowWrapperList.get(windowId).getName();
		} else {
			// Windows of String have negative ID.
			// window -1 = index 0
			// window -2 = index 1
			// window -3 = index 2
			int convertedId = ((windowId + 1) * -1);
			if (convertedId >= windowWrapperList_String.size()) {
				return null;
			}
			return windowWrapperList_String.get(convertedId).getName();
		}

	}

	// window manager has window named...
	public boolean hasWindow(String windowName) {
		
		if(hasWindow_Number(windowName)) {
			return true;
		}
		
		if(hasWindow_String(windowName)) {
			return true;
		}

		return false;
	}

	// window manager has window named...
	public boolean hasWindow_Number(String windowName) {
		for (WindowWrapper w : windowWrapperList) {
			if (w.getName().equals(windowName))
				return true;
		}
		return false;
	}

	// window manager has window_String named...
	public boolean hasWindow_String(String windowName) {
		for (WindowWrapper_String w : windowWrapperList_String) {
			if (w.getName().equals(windowName))
				return true;
		}

		return false;
	}

	// Check if any query depends on a stream
	public boolean hasWindowDependantOnStream(int streamId) {
		for (int i = 0; i < windowWrapperList.size(); i++) {
			if (windowWrapperList.get(i) != null)
				if (windowWrapperList.get(i).getStreamId() == streamId) {
					return true;
				}
		}

		for (int i = 0; i < windowWrapperList_String.size(); i++) {
			if (windowWrapperList_String.get(i) != null)
				if (windowWrapperList_String.get(i).getStreamId() == streamId) {
					return true;
				}
		}

		return false;
	}

	public int windowCount() {
		return windowWrapperList.size() + windowWrapperList_String.size();
	}

	// get all window names in JSON format
	public String listAllWindows() {
		String response = "";
		for (int i = 0; i < windowWrapperList.size(); i++) {
			response = response + "{\"window_id\": " + i + ", \"stream\":\""
					+ RioDB.rio.getEngine().getStream(streamId).getName() + "\", \"window_name\":\""
					+ windowWrapperList.get(i).getName() + "\"},";
		}
		for (int i = 0; i < windowWrapperList_String.size(); i++) {
			response = response + "{\"window_id\": " + ((i * -1) - 1) + ", \"stream\":\""
					+ RioDB.rio.getEngine().getStream(streamId).getName() + "\", \"window_name\":\""
					+ windowWrapperList_String.get(i).getName() + "\"},";
		}

		return response;
	}

	// get all window names
	public String describeWindow(String windowName) {
		for (int i = 0; i < windowWrapperList.size(); i++) {
			if (windowWrapperList.get(i).getName().equals(windowName)) {
				String description = windowWrapperList.get(i).describeWindow();
				return description;
			}
		}

		for (int i = 0; i < windowWrapperList_String.size(); i++) {
			if (windowWrapperList_String.get(i).getName().equals(windowName)) {
				String description = windowWrapperList_String.get(i).describeWindow();
				return description;
			}
		}

		return "{}";
	}

	/*
	 * Method to process a message through all windows. It loops through all
	 * windows, passing the message to each of them and collecting the windowSummary
	 * as response. All windowSummaries are collected into an array and returned.
	 * 
	 */
	public WindowSummary[] putMessageRef(RioDBStreamMessage message) {

		WindowSummary results[] = new WindowSummary[windowWrapperList.size()];
		// guarantee that currentSecond is the same for all windows.
		int currentSecond = RioDB.rio.getEngine().getClock().getCurrentSecond();
		for (int i = 0; i < results.length; i++) {
			results[i] = (WindowSummary) windowWrapperList.get(i).putMessageRef(message, currentSecond);
		}

		return results;

	}

	/*
	 * Method to process a message through all windows of STRING. It loops through
	 * all windows of STRING, passing the message to each of them and collecting the
	 * windowSummary_String as response. All windowSummaries are collected into an
	 * array and returned.
	 */
	public WindowSummary_String[] putMessageRef_String(RioDBStreamMessage message) {

		WindowSummary_String results[] = new WindowSummary_String[windowWrapperList_String.size()];
		// guarantee that currentSecond is the same for all windows.
		int currentSecond = RioDB.rio.getEngine().getClock().getCurrentSecond();
		for (int i = 0; i < results.length; i++) {
			results[i] = (WindowSummary_String) windowWrapperList_String.get(i).putMessageRef(message, currentSecond);
		}

		return results;

	}

	// Trim windows to evict elements that are old (for window of time)
	public void trimExpiredWindowElements(int currentSecond) {
		for (int i = 0; i < windowWrapperList.size(); i++) {
			windowWrapperList.get(i).trimExpiredWindowElements(currentSecond);
		}

		for (int i = 0; i < windowWrapperList_String.size(); i++) {
			windowWrapperList_String.get(i).trimExpiredWindowElements(currentSecond);
		}

	}

	// Reset a window back to empty, to await first entry
	public void resetWindow(String windowName) {

		if (hasWindow_Number(windowName)) {
			getWindow(windowName).resetWindow();
		}

		else if (hasWindow_String(windowName)) {
			getWindow_String(windowName).resetWindow();
		}
	}

	// Reset all windows to empty, awaiting first entry
	public void resetAllWindows() {
		for (int i = 0; i < windowWrapperList.size(); i++) {
			windowWrapperList.get(i).resetWindow();
		}
		for (int i = 0; i < windowWrapperList_String.size(); i++) {
			windowWrapperList_String.get(i).resetWindow();
		}
	}

}
