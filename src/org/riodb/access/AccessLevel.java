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

AccessLevel is an object to definal the FINAL values of what users can and cannot do.


 */


package org.riodb.access;

public class AccessLevel {
	
	// final types of access levels in order of lowest to highest
	static final String[] accessLevelCodes = {"NONE","QUERY","WINDOW","STREAM","ADMIN"};
	
	// what access level a user has
	int accessLevel;
	
	// constructor
	public AccessLevel(int accessLevel) {
		this.accessLevel = accessLevel;
	}
	
	// check if user's access level permits an action
	public boolean can(String verb) throws ExceptionAccessMgt {
		return accessLevel >= AccessLevel.getAccessLevelCode(verb);
	}
	
	// gets the display name of an access level, for logging purposes. 
	public String stringValue() {
		return (accessLevelCodes[accessLevel]);
	}
	
	// get the numeric id (array index) of an access code display name. 
	public static int getAccessLevelCode(String verb) throws ExceptionAccessMgt {
		for(int i = 0; i < accessLevelCodes.length; i++) {
			if(accessLevelCodes[i].equals(verb)) {
				return i;
			}
		}
		throw new ExceptionAccessMgt("Invalid access level.");
	}
	
}
