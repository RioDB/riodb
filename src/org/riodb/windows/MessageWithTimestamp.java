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

	A wrapper that stores a RioDBStreamMessage and a timestamp

*/

package org.riodb.windows;

import org.riodb.plugin.RioDBStreamMessage;

class MessageWithTimestamp {
	RioDBStreamMessage message;
	int	timestamp;
	MessageWithTimestamp(RioDBStreamMessage message, int	timestamp){
		this.message = message;
		this.timestamp = timestamp;
	}
	
	protected RioDBStreamMessage getMessage() {
		return message;
	}
	protected int getTimestamp() {
		return timestamp;
	}
}
