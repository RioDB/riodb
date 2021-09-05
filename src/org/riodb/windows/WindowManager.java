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

import org.riodb.plugin.RioDBStreamEvent;

public class WindowManager {

	private int streamId;

	// an arraylist of windows created for a parent stream.
	private final ArrayList<WindowWrapper> windowWrapperList = new ArrayList<WindowWrapper>();

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
		RioDB.rio.getSystemSettings().getLogger().debug("Window '" + newWindow.getName() + "' added");
	}

	// drop a window (sync in case of concurrent requests)
	public synchronized boolean dropWindow(String windowName) {
		
		for (int i = 0; i < windowWrapperList.size(); i++) {
			if(windowName.equals(windowWrapperList.get(i).getName())) {
				windowWrapperList.remove(i);
				return true;
			}
		}
		return false;
	}

	// get count of windows in this stream windowManager
	public int getWindowCount() {
		return windowWrapperList.size();
	}

	// get WindowWrapper by index
	public WindowWrapper getWindow(int index) {
		return windowWrapperList.get(index);
	}

	// get WindowWrapper by name
	public WindowWrapper getWindow(String windowName) {
		if (this.hasWindow(windowName))
			for (WindowWrapper w : windowWrapperList) {
				if (w.getName().equals(windowName))
					return w;
			}
		return null;
	}

	// Check if window requires a certain function
	public boolean windowRequiresFunction(int windowId, int functionId) {
		if (windowId >= windowWrapperList.size() || windowId < 0) {
			return false;
		}
		return windowWrapperList.get(windowId).windowRequiresFunction(functionId);
	}

	// get window ID (index) by name
	public int getWindowId(String windowName) {
		if (windowWrapperList.size() == 0) {
			return -1;
		}
		for (int i = 0; i < windowWrapperList.size(); i++) {
			if (windowWrapperList.get(i).getName().equals(windowName))
				return i;
		}
		return -1;
	}
	
	// get window name by id
	public String getWindowName(int windowId) {
			if (windowWrapperList.size() < windowId) {
				return null;
			}
			return windowWrapperList.get(windowId).getName();
	}

	// window manager has window named...
	public boolean hasWindow(String windowName) {
		for (WindowWrapper w : windowWrapperList) {
			if (w.getName().equals(windowName))
				return true;
		}
		return false;
	}
	
	// Check if any query depends on a stream
	public boolean hasWindowDependantOnStream(int streamId) {
		for (int i = 0; i < windowWrapperList.size(); i++) {
			if(windowWrapperList.get(i) != null)
				if(windowWrapperList.get(i).getStreamId() == streamId) {
					return true;
				}
		}
		return false;
	}

	public int windowCount() {
		return windowWrapperList.size();
	}

	// get all window names in JSON format
	public String listAllWindows() {
		String response = "";
		for (int i = 0; i < windowWrapperList.size(); i++) {
			response = response + "{\"window_id\": " + i + ", \"stream\":\""
					+ RioDB.rio.getEngine().getStream(streamId).getName() + "\", \"window_name\":\""
					+ windowWrapperList.get(i).getName() + "\"},";
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
		return "{}";
	}

	/*
	 * 	Method to process an event through all windows. 
	 *  It loops through all windows, passing the event
	 *  to each of them and collecting the windowSummary as response.
	 *  All windowSummaries are collected into an array
	 *  and returned. 
	 * 
	 */
	public WindowSummary[] putEventRef(RioDBStreamEvent event) {

		WindowSummary results[] = new WindowSummary[windowWrapperList.size()];
		// guarantee that currentSecond is the same for all windows.
		int currentSecond = RioDB.rio.getEngine().getClock().getCurrentSecond();
		for (int i = 0; i < results.length; i++) {
			results[i] = (WindowSummary) windowWrapperList.get(i).putEventRef(event, currentSecond);
		}

		return results;

	}

	// Trim windows to evict elements that are old (for window of time)
	public void trimExpiredWindowElements(int currentSecond) {
		for (int i = 0; i < windowWrapperList.size(); i++) {
			windowWrapperList.get(i).trimExpiredWindowElements(currentSecond);
			;
		}
	}

	// Reset a window back to empty, to await first entry
	public void resetWindow(String windowName) {
		getWindow(windowName).resetWindow();
	}

	// Reset all windows to empty, awaiting first entry
	public void resetAllWindows() {
		for (int i = 0; i < windowWrapperList.size(); i++) {
			windowWrapperList.get(i).resetWindow();
		}
	}

}
