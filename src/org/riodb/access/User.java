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

	Class to define a user.
	Users are only enabled when HTTPS API is enabled. 

*/


package org.riodb.access;

public class User {

	// user password hash
	private String userPwdHash;
	// user access level
	private int accessLevel = 0;

	// constructor
	public User(String userPwd) {
		this.userPwdHash = userPwd;
	}

	// getter for access level
	public AccessLevel getUserAccessLevel() {
		return new AccessLevel(accessLevel);
	}

	// getter for password hash
	public String getPwd() {
		return userPwdHash;
	}

	// password reset
	public void resetPwd(String newPwdHash) {
		//TODO: Check if request is called with permission. 
		//And create separate function for resseting own passwd. 
		userPwdHash = newPwdHash;
	}

	// set user permission
	public void setAccess(String verb) throws ExceptionAccessMgt {
		//TODO: Check if request is called with permission. 
		this.accessLevel = AccessLevel.getAccessLevelCode(verb);
	}

}
