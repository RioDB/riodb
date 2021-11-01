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

package org.riodb.sql;

import org.riodb.engine.RioDB;

public class SQLQueryResource {

	private int streamId;
	private int windowId;
	private String code;
	private String alias;

	SQLQueryResource(int streamId, int windowId, String code, String alias) throws ExceptionSQLStatement{
		if(code == null || code.length()==0) {
			throw new ExceptionSQLStatement("Attempted to create a resource with no name.");
		}
		this.streamId = streamId;
		this.windowId = windowId;
		this.code = code;
		if(alias != null) {
			this.alias = alias;
		}
		else {
			this.alias = code;
		}
		RioDB.rio.getSystemSettings().getLogger().debug("FROM resource: " + code + ", alias: " + alias );
	}

	public String getCode() {
		return code;
	}

	public String getAlias() {
		return alias;
	}

	public int getStreamId() {
		return streamId;
	}

	public int getWindowId() {
		return windowId;
	}

}

