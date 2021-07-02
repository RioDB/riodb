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
	 * request. Therefore, we have an internal app clock that updates 
	 * with precision near 1ms, and changes every 1 second. 
	 * All processes that use the clock for time
	 * rounded to the nearest second can point to this clock.
	 * 
	 * Ideally, events received by RioDB data source already contain a timestamp field
	 * that is organized by the dataSource class. If there's no timestamp, the app Clock
	 * can be used to get the current second. The precision of about 1ms is totally fine
	 * in this use case because network packets received by the data source class
	 * usually experience variable network lag anyway.  So it's always best to obtain
	 * event data that already comes with timestamp.  
	 */

	private static volatile int currentSecond;
	private Thread clockThread;

	public Clock() {
	//	currentSecond = Long.valueOf(System.currentTimeMillis() / 1000).intValue();
	//	clockThread = new Thread(this);
	//	clockThread.setName("RIO_CLOCK_THREAD");
	}

	// return the current second (which updates about every 1 second.) 
	public int getCurrentSecond() {
		return currentSecond;
	}

	// start clock thread
	public void start() {
		currentSecond = Long.valueOf(System.currentTimeMillis() / 1000).intValue();
		clockThread = new Thread(this);
		clockThread.setName("RIO_CLOCK_THREAD");
		
		clockThread.start();
	}

	// Stope clock sthread. 
	public void stop() {
		clockThread.interrupt();
	}
	
	// run the Clock thread (that updates current second every 1ms)
	@Override
	public void run() {
		try {
			RioDB.rio.getSystemSettings().getLogger().debug("Starting clock...");
			// Run in a loop updating "current second" field about every 1 ms.  
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
