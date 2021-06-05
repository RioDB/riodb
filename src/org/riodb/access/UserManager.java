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

	private HashMap<String, User> users;
	private String pwdFile;

	private final String userNameRegex = "^[a-zA-Z0-9_]*$";
	private final String userNameRequirement = "Must contain only letters, numbers and underscores.";

	private final String userPwdRegex = "^(?=.*[a-z])(?=.*[A-Z])(?=.*\\d)[a-zA-Z\\d\\W]{8,63}$";
	private final String userPwdRequirement = "Must contain at least 8 characters, lowecase, uppercase, number and special character.";

	public UserManager(String pwdFile) throws ExceptionAccessMgt {
		users = new HashMap<String, User>();
		this.pwdFile = pwdFile;
		loadPwdFile();
	}

	public boolean authenticate(String userName, String userPwd) {

		if (userName == null || userName.length() == 0 || userPwd == null || userPwd.length() == 0) {
			return false;
		}

		userName = userName.toUpperCase();

		User u = users.get(userName);
		if (u != null) {

			String pwdHash = Hashing.sha256().hashString(userPwd, StandardCharsets.UTF_8).toString();
			if (u.getPwd().equals(pwdHash)) {
				return true;
			}

		}
		return false;
	}

	// All user management operations are channeled through a single synchronized
	// procedure to avoid race conditions on writing output file.
	public synchronized String changeRequest(String requestStmt, String actingUserName)
			throws ExceptionAccessMgt, ExceptionSQLStatement {

		String formattedStmt = SQLParser.formatStmt(requestStmt + ";");

		String response = "";
		if (formattedStmt.contains("create user ")) {
			// original statement because the formatted version forces passwords to
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

		writeToFile();
		
		return response;

	}

	private void createUser(String stmt, String actingUserName) throws ExceptionAccessMgt {

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

			String newUserName = params[2].trim().toUpperCase();
			String newUserPwd = params[3].trim().replace(";", "");

			if (newUserName.equals("ADMIN"))
				throw new ExceptionAccessMgt("Cannot crate user named ADMIN.");
			if (newUserName.equals("SYSTEM"))
				throw new ExceptionAccessMgt("Cannot crate user named SYSTEM.");

			if (!newUserName.matches(userNameRegex)) {
				throw new ExceptionAccessMgt(userNameRequirement);
			}
			if (!newUserPwd.matches(userPwdRegex)) {
				System.out.println("password: " + newUserPwd);
				throw new ExceptionAccessMgt(userPwdRequirement);
			}

			if (users.containsKey(newUserName)) {
				throw new ExceptionAccessMgt("User already exists.");
			}

			String pwdHash = Hashing.sha256().hashString(newUserPwd, StandardCharsets.UTF_8).toString();
			User u = new User(pwdHash);
			users.put(newUserName, u);

		} else {
			throw new ExceptionAccessMgt("New user credentials not formatted properly. CREATE USER name pwd;");
		}

	}

	private void dropUser(String stmt, String actingUserName) throws ExceptionAccessMgt {

		String userToDrop = stmt.replace("drop user ", "").trim();

		if (userToDrop == null || userToDrop.length() == 0) {
			throw new ExceptionAccessMgt("Missing username.");
		}

		userToDrop = userToDrop.toUpperCase().replace(";", "");

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

	public AccessLevel getUserAccessLevel(String userName) {
		
		if (userName != null) {

			if(userName.equals("SYSTEM")) {
				try {
					return new AccessLevel(AccessLevel.getAccessLevelCode("ADMIN"));
				} catch (ExceptionAccessMgt e) {
					// nothing here. "ADMIN" must exist. 
				}
			}
			
			User u = users.get(userName);
			if (u != null) {
				return u.getUserAccessLevel();
			}
		}

		return new AccessLevel(0); // NO ACCESS
	}

	private void setAccess(String stmt, String actingUserName) throws ExceptionAccessMgt {

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

		if (users.containsKey(user)) {
			users.get(user).setAccess(verb);
		} else {
			throw new ExceptionAccessMgt("User not found.");
		}
	}

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

	private void loadPwdFile() throws ExceptionAccessMgt {

		users.clear();

		try {
			File myObj = new File(pwdFile);
			Scanner myReader = new Scanner(myObj);
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

					if (userName.equals("ADMIN"))
						newUser.setAccess("ADMIN");
					else if (creds.length > 2) {
						if (creds[2].equals("ADMIN")) {
							newUser.setAccess(creds[2]);
						}
					}
					users.put(userName, newUser);
				}
			}
			myReader.close();
		} catch (FileNotFoundException e) {
			throw new ExceptionAccessMgt("Password file provided is not found.");
		}
		RioDB.rio.getSystemSettings().getLogger().debug("Password File provided " + users.size() + " user accounts.");

	}

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
