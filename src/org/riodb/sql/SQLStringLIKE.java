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
 * 	Instead of a final class (without variables) that checks if a string matches another with wildcards,
 *  this class is initialized with pre-determined variables to expedite comparisons, 
 *  which improves performance when the same pattern is being evaluated many, many times.  
 * 
 *  note: NULL and Empty String "" are treated the same. 
 */

package org.riodb.sql;

public class SQLStringLIKE {

	private short comparisonType;
	/*	0 is null
	 *  1 is equal to
	 *  2 begins with
	 *  3 ends with
	 *  4 begins with and ends with
	 *  5 begins with, contains, and ends with
	 *  6 begins with and contains
	 *  7 ends with and contains
	 *  8 contains single
	 *  9 contains multiple
	*/
	// firstHalf is a flag to indicate that the comparisonType is between 0-4 (true) or 5-9 (false).  
	// acting like a 2-layer tree, so that we don't have to test EVERY case before getting to the last comparisonTypes. 
	private boolean firstHalf; 
	private String patterns[];

	SQLStringLIKE(String pattern){
		
		if(pattern == null || pattern.length() == 0) {
			this.comparisonType = 0;
			firstHalf = true;
//			System.out.println("is null");
		}
		else {
			int countWildCard = 0;
			for(int i = 0; i < pattern.length(); i++) {
				if(pattern.charAt(i)=='%') {
					countWildCard++;
				}
			}

			if(countWildCard == 0) {
				comparisonType = 1;
				patterns = null;
				firstHalf = true;
//				System.out.println("is equal to");
			}
			else if(countWildCard == 1) {
				firstHalf = true;
				if(pattern.charAt(pattern.length()-1)=='%') {
					//begins with
					comparisonType = 2;
					patterns = new String[1];
					patterns[0] = pattern.substring(0,pattern.length()-1);
//					System.out.println("begins with '"+ patterns[0]+"'");
				}
				else if(pattern.charAt(0)=='%') {
					//ends with
					comparisonType = 3;
					patterns = new String[1];
					patterns[0] = pattern.substring(1);
//					System.out.println("ends with '"+ patterns[0]+"'");
				}
				else {
					//begins with and ends with
					comparisonType = 4;
					patterns = new String[2];
					patterns[0] = pattern.substring(0,pattern.indexOf('%'));
					patterns[1] = pattern.substring(pattern.indexOf('%')+1);
//					System.out.println("begins with '"+ patterns[0] + "' and ends with '"+ patterns[1] +"'");
				}
			}
			else {
				firstHalf = false;
				patterns = pattern.split("%");
				if(pattern.charAt(0)!='%' &&
						pattern.charAt(pattern.length()-1)!='%') {
					//begins with, contains, ends with
					comparisonType = 5;
//					System.out.println("begins with, contins, ends with");
				}
				else if(pattern.charAt(0)!='%') {
					//begins with and contains
					comparisonType = 6;
//					System.out.println("begins with and contains");
				}
				else if (pattern.charAt(pattern.length()-1)!='%'){
					//ends with and contains
					comparisonType = 7;

					// remove the first entry of the array because it's null. 
					String newPatterns[] = new String[patterns.length-1];
					for(int i = 0; i<newPatterns.length; i++) {
						newPatterns[i] = patterns[i+1];
					}
					patterns = newPatterns;
//					System.out.println("ends with and contains");
				}
				else {
					//contains single
					if(countWildCard == 2) {
						comparisonType = 8;
//						System.out.println("contains single");
					}
					else {
						// contains multiple
						comparisonType = 9;
//						System.out.println("contains multiple");
					}
					// remove the first entry of the array because it's null. 
					String newPatterns[] = new String[patterns.length-1];
					for(int i = 0; i<newPatterns.length; i++) {
						newPatterns[i] = patterns[i+1];
					}
					patterns = newPatterns;

				}
			}
		}
	}

	public boolean match(String s) {

		//received null or empty string
		if (s == null || s.length() == 0) {
			return comparisonType == 0;
		}
		// first 5 comparisonTypes
		else if(firstHalf) {
			if (comparisonType == 0) {
				// To check if it's null. We already know it's not. 
				return false;
			}
			else if (comparisonType == 1) {
				// is equals to pattern
				return s.equals(patterns[0]);
			}
			else if (comparisonType == 2) {
				// begins with pattern
				return s.startsWith(patterns[0]);
			}
			else if (comparisonType == 3) {
				// ends with pattern
				return s.endsWith(patterns[0]);
			}
			// comparisonType can only be 4
			else{
				return s.startsWith(patterns[0]) && s.endsWith(patterns[1]);
			}
		}
		// rest of comparisonTypes
		else {
			// 8 seems like a more popular search %contains%. So it goes first. 
			if (comparisonType == 8) {
				// contains pattern
				return s.indexOf(patterns[0]) > -1;
			}
			else if (comparisonType == 6) {
				// begins with and contains
				if(!s.startsWith(patterns[0]))
					return false;
				int index = patterns[0].length();
				for(int i = 1; i < patterns.length; i++) {
					index = s.substring(index).indexOf(patterns[i]);
					if(index == -1)
						return false;
				}
				return true;
			}
			else if (comparisonType == 7) {
				// ends with and contains
				if(!s.endsWith(patterns[patterns.length-1]))
					return false;
				int index = 0;
				for(int i = 0; i < patterns.length-1; i++) {
					index = s.substring(index).indexOf(patterns[i]);
					if(index == -1)
						return false;
				}
				return true;
			}
			else if (comparisonType == 5) {
				// begins with and contains and ends with
				if(!s.startsWith(patterns[0]))
					return false;
				if(!s.endsWith(patterns[patterns.length-1]))
					return false;
				int index = patterns[0].length();
				for(int i = 1; i < patterns.length-1; i++) {
					index = s.substring(index).indexOf(patterns[i]);
					if(index == -1)
						return false;
				}
				
				return true;
			}
			// comparisonType can only be 9
			else {
				int index = 0;
				for(int i = 1; i < patterns.length; i++) {
					index = s.substring(index).indexOf(patterns[i]);
					if(index == -1)
						return false;
				}
				return true;
			}
		}
	}
}
