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
 *   A container of Windows, so that multiple windows can be created. 
 */

package org.riodb.windows;

import java.util.ArrayList;

import org.riodb.engine.RioDB;

import org.riodb.plugin.RioDBStreamEvent;

public class WindowManager {

	private int streamId;

	// an arraylist of windows created for a parent stream.
	private final ArrayList<WindowWrapper> windowWrapperList = new ArrayList<WindowWrapper>();

	public WindowManager(int streamId) {
		this.streamId = streamId;
	}

	public void addWindow(WindowWrapper newWindow) {
		windowWrapperList.add(newWindow);
		RioDB.rio.getSystemSettings().getLogger().debug("Window '" + newWindow.getName() + "' added");
	}

	public boolean removeWindow(int index) {
		if(windowWrapperList.size() > index) {
			windowWrapperList.remove(index);
			return true;
		}
		return false;
	}

	// window count
	public int getWindowCount() {
		return windowWrapperList.size();
	}

	// get WindowWrapper
	public WindowWrapper getWindow(int index) {
		return windowWrapperList.get(index);
	}
	
	// get WindowWrapper
		public WindowWrapper getWindow(String windowName) {
			if(this.hasWindow(windowName))
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

	// window manager has window named...
	public boolean hasWindow(String windowName) {
		for (WindowWrapper w : windowWrapperList) {
			if (w.getName().equals(windowName))
				return true;
		}
		return false;
	}
	
	public int windowCount() {
		return windowWrapperList.size();
	}

	// get all window names
	public String listAllWindows() {
		String response = "";
		for (int i = 0; i < windowWrapperList.size(); i++) {
			response = response + "{\"window_id\": "+ i +", \"stream\":\""+ RioDB.rio.getEngine().getStream(streamId).getName() +"\", \"window_name\":\"" + windowWrapperList.get(i).getName() + "\"},";
		}
		if (response.length() > 2)
			response = response.substring(0, response.length() - 1);
		return response;
	}

	// get all window names
	public String describeWindow(String windowName) {
		for (int i = 0; i < windowWrapperList.size(); i++) {
			if (windowWrapperList.get(i).getName().equals(windowName)) {
				return windowWrapperList.get(i).describeWindow();
			}
		}
		return "";
	}

	public WindowSummary[] putEventRef(RioDBStreamEvent event) {

		WindowSummary results[] = new WindowSummary[windowWrapperList.size()];
		// guarantee that currentSecond is the same for all windows. 
		int currentSecond = RioDB.rio.getEngine().getClock().getCurrentSecond();
		for (int i = 0; i < windowWrapperList.size(); i++) {
			results[i] = (WindowSummary) windowWrapperList.get(i).putEventRef(event, currentSecond);
		}

		return results;
		
	}
	
	public void trimExpiredWindowElements(int currentSecond) {
		for (int i = 0; i < windowWrapperList.size(); i++) {
			windowWrapperList.get(i).trimExpiredWindowElements(currentSecond);;
		}
	}

}
