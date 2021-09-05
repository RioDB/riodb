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

	A Class for persisting user statements (from API) to a file,
	so that statements resurrect after reboot. 
	For example, if a user creates a window and a query, the 2
	statements are written to a hidden .sql file. 
	Upon reboot, the statements get reloaded into RioDB. 
	
		
*/

package org.riodb.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.riodb.sql.SQLParser;

public class PersistedStatements {

	// maintain a map for "create stream" statements
	TreeMap<String, String> streamStatements;
	// maintain a map for "create window" statements
	TreeMap<String, String> windowStatements;
	// maintain a map for "select" statements
	TreeMap<Integer, String> queryStatements;
	// flag if the hidden sql file has already been created.
	boolean fileExists = false;

	// constructor
	public PersistedStatements() {
		streamStatements = new TreeMap<String, String>();
		windowStatements = new TreeMap<String, String>();
		queryStatements = new TreeMap<Integer, String>();

		// we check if the file exists, but no reason to create one just yet.
		File persistedStmtFile = new File(RioDB.rio.getSystemSettings().getPersistedStmtFile());
		fileExists = persistedStmtFile.exists();

	}

	// load stream statement from persisted file during boot
	public void loadStreamStmt(String name, String statement) {
		RioDB.rio.getSystemSettings().getLogger().debug("Tracking statement for " + name);
		streamStatements.put(name, statement);
	}

	// load window statement from persisted file during boot
	public void loadWindowStmt(String name, String statement) {
		windowStatements.put(name, statement);
	}

	// load select statement from persisted file during boot
	public void loadQueryStmt(Integer queryId, String statement) {
		queryStatements.put(queryId, statement);
	}

	// load newly created stream statement
	public void addNewStreamStmt(String name, String statement) {
		RioDB.rio.getSystemSettings().getLogger().debug("Tracking statement for " + name);
		streamStatements.put(name, SQLParser.textDecode(statement));
		RioDB.rio.getSystemSettings().getLogger().debug("Adding statement to sql file for " + name);
		updatePersistedStmtFile();
	}

	// load newly created window statement
	public void addNewWindowStmt(String name, String statement) {
		windowStatements.put(name, SQLParser.textDecode(statement));
		updatePersistedStmtFile();
	}

	// load newly created select statement
	public void addNewQueryStmt(Integer queryId, String statement) {
		queryStatements.put(queryId, SQLParser.textDecode(statement));
		updatePersistedStmtFile();
	}

	// drop stream statement
	public void dropStreamStmt(String name) {
		/*
		 * System.out.println("Persisted streams. Before removal: "); for (Entry<String,
		 * String> entry : streamStatements.entrySet()) {
		 * System.out.println(entry.getValue()); }
		 */

		RioDB.rio.getSystemSettings().getLogger().debug("Removing stream " + name + " from statement list.");
		streamStatements.remove(name);
		updatePersistedStmtFile();
		/*
		 * System.out.println("Persisted streams. After removal: "); for (Entry<String,
		 * String> entry : streamStatements.entrySet()) {
		 * System.out.println(entry.getValue()); }
		 */

	}

	// drop window statement
	public void dropWindowStmt(String name) {
		windowStatements.remove(name);
		updatePersistedStmtFile();
	}

	// drop select statement
	public void dropQueryStmt(Integer queryId) {
		if (queryStatements.containsKey(queryId)) {
			queryStatements.remove(queryId);
			updatePersistedStmtFile();
		}
	}

	// create a file for persisting statements, if one doesn't already exist
	private boolean createPersistedStmtFile() {
		if (!fileExists) {
			// get file name from system settings
			File persistedStmtFile = new File(RioDB.rio.getSystemSettings().getPersistedStmtFile());
			try {
				persistedStmtFile.createNewFile();
				RioDB.rio.getSystemSettings().getLogger().debug("File to persist statements created as '"
						+ RioDB.rio.getSystemSettings().getPersistedStmtFile() + "'");
			} catch (IOException e) {
				RioDB.rio.getSystemSettings().getLogger()
						.error("Unable to create file for persisting user statements to disk: "
								+ e.getMessage().replace("\n", " ").replace("\r", " ").replace("\"", "'"));
				return false;
			}
			// set fileExists flag so we don't have to check all the time.
			fileExists = persistedStmtFile.exists();
		}
		return true;
	}

	// update (replacing contents) persistent stmt file.
	private void updatePersistedStmtFile() {

		if (fileExists || createPersistedStmtFile()) {

			String newFileStr = "";
			for (Entry<String, String> entry : streamStatements.entrySet()) {
				newFileStr += entry.getValue() + "\n\r";
			}
			for (Entry<String, String> entry : windowStatements.entrySet()) {
				newFileStr += entry.getValue() + "\n\r";
			}
			for (Entry<Integer, String> entry : queryStatements.entrySet()) {
				newFileStr += entry.getValue() + "\n\r";
			}

			PrintWriter prw = null;
			try {
				RioDB.rio.getSystemSettings().getLogger().debug("Updating statement file.");
				prw = new PrintWriter(RioDB.rio.getSystemSettings().getPersistedStmtFile());
				prw.println(newFileStr);

				RioDB.rio.getSystemSettings().getLogger().debug("Statement persisted to local file '"
						+ RioDB.rio.getSystemSettings().getPersistedStmtFile() + "'");

			} catch (FileNotFoundException e) {
				RioDB.rio.getSystemSettings().getLogger().error("Error persisting user statements to disk: "
						+ e.getMessage().replace("\n", " ").replace("\r", " ").replace("\"", "'"));
			} finally {
				if (prw != null)
					prw.close();
			}
		}

	}

}
