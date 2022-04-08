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

	Container class for users. 
	Users are only enabled with HTTPS API is enabled. 

 */
package org.riodb.access;

import java.io.BufferedWriter;

import java.io.File; // Import the File class
import java.io.FileNotFoundException; // Import this class to handle errors
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner; // Import the Scanner class to read text files

import org.riodb.engine.RioDB;
import org.riodb.sql.ExceptionSQLStatement;
import org.riodb.sql.SQLParser;

import com.google.common.hash.Hashing;

public class UserManager {

	// map of users (in memory)
	private HashMap<String, User> users;

	// Location of password file (it's updated when users are
	// created,modified,deleted)
	private String pwdFile;

	// regex requirements for username.
	private final String userNameRegex = "^[a-zA-Z0-9_]*$";
	private final String userNameRequirement = "Must contain only letters, numbers and underscores.";

	// regex requirements for user password
	private final String userPwdRegex = "^(?=.*[a-z])(?=.*[A-Z])(?=.*\\d)[a-zA-Z\\d\\W]{8,63}$";
	private final String userPwdRequirement = "Must contain at least 8 characters, lowecase, uppercase, number and special character.";

	// constructor
	public UserManager(String pwdFile) throws ExceptionAccessMgt {
		users = new HashMap<String, User>();
		this.pwdFile = pwdFile;
		loadPwdFile();
	}

	// authenticate a user.
	public boolean authenticate(String userName, String userPwd) {

		if (userName == null || userName.length() == 0 || userPwd == null || userPwd.length() == 0) {
			return false;
		}

		// usernames are stored in uppercase by convention.
		userName = userName.toUpperCase();

		User u = users.get(userName);
		if (u != null) {
			// make hash of password provided, and check if it matches stored hash.
			String pwdHash = Hashing.sha256().hashString(userPwd, StandardCharsets.UTF_8).toString();
			if (u.getPwd().equals(pwdHash)) {
				return true;
			}
		}
		return false;
	}

	/*
	 * All user management operations are channeled through a single synchronized
	 * procedure to avoid race conditions on writing output file.
	 */
	public synchronized String changeRequest(String requestStmt, String actingUserName)
			throws ExceptionAccessMgt, ExceptionSQLStatement {

		// format SQL statement
		String formattedStmt = SQLParser.formatStmt(requestStmt + ";");

		String response = "";
		if (formattedStmt.contains("create user ")) {
			// original statement because the formatted command modifies passwords to
			// lowercase
			createUser(requestStmt, actingUserName);
			response = "User created.";
		} else if (formattedStmt.contains("change user")) {
			if (formattedStmt.contains(" set access ")) {
				setAccess(formattedStmt, actingUserName);
			} else if (formattedStmt.contains(" set password ")) {
				resetPwd(requestStmt, actingUserName);
			} else {
				throw new ExceptionAccessMgt("Request type unknown.");
			}
			response = "User changed.";
		} else if (formattedStmt.contains("drop user ")) {
			dropUser(formattedStmt, actingUserName);
			response = "User dropped.";
		} else {
			throw new ExceptionAccessMgt("Request type unknown.");
		}

		// update password file.
		writeToFile();

		return response;

	}

	// create a user
	private void createUser(String stmt, String actingUserName) throws ExceptionAccessMgt {

		// check that requester has permissions to do this
		if (!userIsAdmin(actingUserName) && !actingUserName.equals("SYSTEM") && !actingUserName.equals("ADMIN")) {
			throw new ExceptionAccessMgt("Not authorized.");
		}

		String newStmt = stmt.trim();

		if (newStmt.contains(" ")) {

			newStmt = newStmt.replace("\r", " ");
			newStmt = newStmt.replace("\n", " ");
			while (newStmt.contains("  ")) {
				newStmt = newStmt.replace("  ", " ");
			}
			newStmt.trim();

			String[] params = newStmt.split(" ");

			if (params.length != 4
					|| !(params[0].toLowerCase().equals("create")) && params[1].toLowerCase().equals("user")) {
				throw new ExceptionAccessMgt(
						"Invalid statement. Try: CREATE USER name password; all in one line. No comments.");
			}

			// extract username and password from command
			String newUserName = params[2].trim().toUpperCase();
			String newUserPwd = params[3].trim().replace(";", "");

			// check for invalid usernames
			if (newUserName.equals("ADMIN"))
				throw new ExceptionAccessMgt("Cannot crate user named ADMIN.");
			if (newUserName.equals("SYSTEM"))
				throw new ExceptionAccessMgt("Cannot crate user named SYSTEM.");

			// check for username regex requirements
			if (!newUserName.matches(userNameRegex)) {
				throw new ExceptionAccessMgt(userNameRequirement);
			}

			// check for password requirements
			if (!newUserPwd.matches(userPwdRegex)) {
				throw new ExceptionAccessMgt(userPwdRequirement);
			}

			// check if user doesn't already exist
			if (users.containsKey(newUserName)) {
				throw new ExceptionAccessMgt("User already exists.");
			}

			// make hash of password (for storing hash)
			String pwdHash = Hashing.sha256().hashString(newUserPwd, StandardCharsets.UTF_8).toString();
			User u = new User(pwdHash);

			// add user to map
			users.put(newUserName, u);

		} else {
			throw new ExceptionAccessMgt("New user credentials not formatted properly. CREATE USER name pwd;");
		}

	}

	// drop user
	private void dropUser(String stmt, String actingUserName) throws ExceptionAccessMgt {

		String userToDrop = stmt.replace("drop user ", "").trim();

		if (userToDrop == null || userToDrop.length() == 0) {
			throw new ExceptionAccessMgt("Missing username.");
		}

		userToDrop = userToDrop.toUpperCase().replace(";", "");

		// Check that requester is not trying to drop ADMIN or SYSTEM users
		if (userToDrop.equals("ADMIN")) {
			throw new ExceptionAccessMgt("User ADMIN cannot be dropped.");
		}

		if (!userIsAdmin(actingUserName) && !actingUserName.equals("SYSTEM") && !actingUserName.equals("ADMIN") &&
		// user can drop themselves
				!actingUserName.equals(userToDrop)) {
			throw new ExceptionAccessMgt("Not authorized.");
		}

		if (users.containsKey(userToDrop)) {
			users.remove(userToDrop);

		} else {
			throw new ExceptionAccessMgt("User not found.");
		}

	}

	// get access level of a user
	public AccessLevel getUserAccessLevel(String userName) {

		if (userName != null) {

			if (userName.equals("SYSTEM")) {
				try {
					return new AccessLevel(AccessLevel.getAccessLevelCode("ADMIN"));
				} catch (ExceptionAccessMgt e) {
					// nothing here. "ADMIN" must exist.
				}
			}

			User u = users.get(userName);
			if (u != null) {
				// return the users access level
				return u.getUserAccessLevel();
			}
		}

		// if user doesn't exist, return access level NONE
		return new AccessLevel(0); // NO ACCESS
	}

	// set access level of a user
	private void setAccess(String stmt, String actingUserName) throws ExceptionAccessMgt {

		// check if request is made with permission
		if (!userIsAdmin(actingUserName) && !actingUserName.equals("SYSTEM") && !actingUserName.equals("ADMIN")) {
			throw new ExceptionAccessMgt("Not authorized.");
		}

		String[] params = stmt.split(" ");

		// example: CHANGE USER john SET ACCESS WINDOW;
		if (params.length != 6 || !(params[0].toLowerCase().equals("change") && params[1].toLowerCase().equals("user")
				&& params[3].toLowerCase().equals("set") && params[4].toLowerCase().equals("access"))) {
			throw new ExceptionAccessMgt("Invalid statement. Try: CHANGE USER name SET ACCESS verb;");
		}

		String user = params[2].toUpperCase();
		String verb = params[5].toUpperCase().replace(";", "").trim();

		// make sure user exists, first
		if (users.containsKey(user)) {
			// update user access
			users.get(user).setAccess(verb);
		} else {
			throw new ExceptionAccessMgt("User not found.");
		}
	}

	// list all users.
	public String listUsers() {

		// no special priv required to list users. It helps users police the system.

		String out = "[";

		boolean notFirst = false;
		for (Map.Entry<String, User> entry : users.entrySet()) {
			String userLine = "";
			if (notFirst) {
				userLine = ",\r\n ";
			} else {
				notFirst = true;
			}
			userLine = userLine + "{\"userName\": \"" + entry.getKey() + "\", \"accessLevel\": \""
					+ entry.getValue().getUserAccessLevel().stringValue() + "\"}";

			out = out + userLine;
		}
		out = out + "]";

		return out;

	}

	// load password file during start up (if any was indicated in Conf file)
	private void loadPwdFile() throws ExceptionAccessMgt {

		// brand new map of users
		users.clear();

		// try opening and reading the provided password file
		try {
			File myObj = new File(pwdFile);
			Scanner myReader = new Scanner(myObj);

			// loop. for each user in file, load them into memory (hashmap)
			while (myReader.hasNextLine()) {
				String data = myReader.nextLine().trim();
				if (data != null && data.contains(" ")) {
					String creds[] = data.split(" ");

					if (creds.length < 2) {
						myReader.close();
						throw new ExceptionAccessMgt("Password file missing data for user.");
					}

					String userName = creds[0].trim().toUpperCase();
					String userPwd = creds[1];

					if (!userName.matches(userNameRegex)) {
						myReader.close();
						throw new ExceptionAccessMgt("Password file contains invalid username. " + userNameRequirement);
					}

					User newUser = new User(userPwd);

					if (userName.equals("ADMIN")) {
						newUser.setAccess("ADMIN");
						if (userPwd.equals("ce8fb32e15aaa5b84495638c795f554ccf3e08681782763241b8aaba88d93214")) {
							RioDB.rio.getSystemSettings().getLogger()
									.warn("ADMIN password is still set to default. Changing it!");
						}

					} else if (creds.length > 2) {
						if (creds[2].equals("ADMIN")) {
							newUser.setAccess(creds[2]);
						}
					}
					users.put(userName, newUser);
					RioDB.rio.getSystemSettings().getLogger()
					.trace("Adding user "+ userName +" ("+ newUser.getUserAccessLevel().stringValue() +").");
				}
			}
			myReader.close();
		} catch (FileNotFoundException e) {
			throw new ExceptionAccessMgt("Password file provided is not found.");
		}
		RioDB.rio.getSystemSettings().getLogger().debug("Password File provided " + users.size() + " user accounts.");

	}

	// reset user's password
	private void resetPwd(String stmt, String actingUserName) throws ExceptionAccessMgt {
		String newStmt = stmt.trim();

		if (newStmt.contains(" ")) {

			newStmt = newStmt.replace("\r", " ");
			newStmt = newStmt.replace("\n", " ");
			while (newStmt.contains("  ")) {
				newStmt = newStmt.replace("  ", " ");
			}
			newStmt.trim();

			String[] params = newStmt.split(" ");

			// example: CHANGE USER john SET PASSWORD helloW23!;

			if (params.length != 6
					|| !(params[0].toLowerCase().equals("change") && params[1].toLowerCase().equals("user")
							&& params[3].toLowerCase().equals("set") && params[4].toLowerCase().equals("password"))) {
				throw new ExceptionAccessMgt("Invalid statement. Try: CHANGE USER name SET PASSWORD password;");
			}

			String userName = params[2].trim().toUpperCase();
			String newUserPwd = params[5].trim().replace(";", "");

			if (!userIsAdmin(actingUserName) && !actingUserName.equals("SYSTEM") && !actingUserName.equals("ADMIN")
					&& !actingUserName.equals(userName)) { // user can change own password
				throw new ExceptionAccessMgt("Not authorized.");
			}

			if (!newUserPwd.matches(userPwdRegex)) {
				throw new ExceptionAccessMgt(userPwdRequirement);
			}

			if (users.containsKey(userName)) {
				String pwdHash = Hashing.sha256().hashString(newUserPwd, StandardCharsets.UTF_8).toString();
				users.get(userName).resetPwd(pwdHash);
			}

		} else {
			throw new ExceptionAccessMgt("Invalid statement.");
		}

	}

	// Check if user is admin
	private boolean userIsAdmin(String userName) {
		User u = users.get(userName);
		if (u != null) {
			try {
				return u.getUserAccessLevel().can("ADMIN");
			} catch (ExceptionAccessMgt e) {
				// nothing here. ADMIN must exist.
			}
		}
		return false;
	}

	// Save the user data that is in-memory to a file on the disk.
	// Basically, update the same password file provided in conf file.
	private void writeToFile() {

		String fileContent = "";

		for (Map.Entry<String, User> entry : users.entrySet()) {
			String userLine = entry.getKey() + " " + entry.getValue().getPwd() + " "
					+ entry.getValue().getUserAccessLevel().stringValue();

			fileContent = fileContent + userLine + "\r\n";
		}

		try {
			BufferedWriter writer;

			writer = new BufferedWriter(new FileWriter(pwdFile));
			writer.write(fileContent);

			writer.close();
		} catch (IOException e) {
			RioDB.rio.getSystemSettings().getLogger()
					.error("Error writing password file: " + e.getMessage().replace("\n", "").replace("\r", ""));
		}

	}

}
