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

	RioDB has a default output plugin. And it's not the STDOUT console. 
	The default output is for when a Query statement is requested via HTTP API,
	by default, the response is made to that API requester. 
	
	This default output implements RioDBOutput, and is a mechanism to
	route query responses to the API request. 

 */
package org.riodb.queries;
import org.riodb.engine.RioDB;
import org.riodb.plugin.RioDBOutput;
import org.riodb.plugin.RioDBPluginException;
import org.riodb.plugin.RioDBPluginStatus;

public class DefaultOutput implements RioDBOutput{
	
	// Column headers from SELECT ... statement
	String[] columnHeaders;
	// SessionId of the API request that submitted the query
	Integer  sessionId;
	// Status of this output plugin
	final RioDBPluginStatus status = new RioDBPluginStatus(1);
	// The streamId of the stream driving the query calling this output
	int streamId = 0;

	// return the output type (aka output plugin name)
	@Override
	public String getType() {
		return "API_REQUEST_RESPONSE";
	}
	
	// constructor
	public DefaultOutput(int streamId, int sessionId, String[] columnHeaders) {
		this.columnHeaders = columnHeaders;
		this.streamId = streamId;
		this.sessionId = sessionId;
	}

	// initializer (Plugins initialize after class loading)
	@Override
	public void init(String params, String[] columnHeaders) throws RioDBPluginException {
	}

	// post response to the output. 
	@Override
	public void post(String[] columns) {
		
		String reply = "{";
		for (int i = 0; i < Math.max(columnHeaders.length, columns.length); i++) {
			reply = reply + " \""+columnHeaders[i]+"\" : \""+ columns[i] +"\",";
		}
		// remove last comma and close bracket
		reply = reply.substring(0,reply.length()-1) + " }";
		
		RioDB.rio.getEngine().getStream(streamId).sendQueryResponse(sessionId, reply);
		
	}

	// get plugin status
	@Override
	public RioDBPluginStatus status() {
		return status;
	}

}
