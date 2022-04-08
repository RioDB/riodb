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

	A waiting queue for storing messages that need to wait for range end date. 
	For example, when using range 1000-100, messages have to wait 100 before 
	getting processed into windows. 

*/

package org.riodb.windows;

import org.riodb.plugin.RioDBStreamMessage;
import java.util.ArrayDeque;
import java.util.LinkedList;

public class TimeBuffer {
	
	

	// queue
	private ArrayDeque<MessageWithTimestamp> buffer;
	// amount of time a message should wait for
	private int		waitTime;

	// constructor
	TimeBuffer(int waitTime){
		buffer = new ArrayDeque<MessageWithTimestamp>();
		this.waitTime = waitTime;
	}
	
	public LinkedList<MessageWithTimestamp> pushAndPoll(RioDBStreamMessage msg, int currentSecond) {
		
		buffer.add(new MessageWithTimestamp(msg, currentSecond));
		
		LinkedList<MessageWithTimestamp> newList = new LinkedList<MessageWithTimestamp>();
		
		while(!buffer.isEmpty()) {
			if (currentSecond - buffer.peek().getTimestamp() >= waitTime) {
				newList.add(buffer.pop());
			} else {
				break;
			}
		}
		
		return newList;
		
	}
	
	public int size() {
		return buffer.size();
	}

}
