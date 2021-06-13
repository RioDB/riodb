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

package org.riodb.engine;

public class Clock implements Runnable {

	/*
	 * On Windows OS, system.currentTimeMillis() is basically free. We could run it
	 * for every request. On Linux, however, it is expensive to run it for every
	 * request. Therefore, we have an internal wall clock that updates every 1
	 * second, with precision near 1ms. All processes that use wall clock for time
	 * rounded to the nearest second can point to this clock
	 * 
	 */

	private static volatile int currentSecond;
	private Thread clockThread;

	public Clock() {
	//	currentSecond = Long.valueOf(System.currentTimeMillis() / 1000).intValue();
	//	clockThread = new Thread(this);
	//	clockThread.setName("RIO_CLOCK_THREAD");
	}

	public int getCurrentSecond() {
		return currentSecond;
	}

	public void start() {
		currentSecond = Long.valueOf(System.currentTimeMillis() / 1000).intValue();
		clockThread = new Thread(this);
		clockThread.setName("RIO_CLOCK_THREAD");
		
		clockThread.start();
	}

	public void stop() {
		clockThread.interrupt();
	}
	
	@Override
	public void run() {
		try {
			RioDB.rio.getSystemSettings().getLogger().debug("Starting clock...");
			while (true) {
				int now = (int) (System.currentTimeMillis() / 1000L);
				if(now > currentSecond) {
					currentSecond = now;
					RioDB.rio.getEngine().trimExpiredWindowElements(currentSecond);
				}
				Thread.sleep(1);
			}
		} catch (InterruptedException e) {
			RioDB.rio.getSystemSettings().getLogger().debug("Clock stopped.");
		}
	}

	// just a handy quickPause function to be used between start-up sequence steps.
	public static final void quickPause() {
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			;
		}
	}
	
}
