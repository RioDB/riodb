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

public final class SQLFunctionMap {

	private static final String functions[] = {
			"avg",
			"count",
			"count_distinct",
			"count_if",
			"first",
			"last",
			"max",
			"median",
			"min",
			"mode",
			"stddev_pop",
			"variance_pop",
			"previous",
			"stddev",
			"variance",
			"slope",
			"sum",
			"sum_if"
	};
	
	private static final String functionCalls[] = {
			"getAvg()",
			"getCount()",
			"getCountDistinct()",
			"getCountIf()",
			"getFirst()",
			"getLast()",
			"getMax()",
			"getMedian()",
			"getMin()",
			"getMode()",
			"getStddevPop()",
			"getVariancePop()",
			"getPrevious()",
			"getStddev()",
			"getVariance()",
			"getSlope()",
			"getSum()",
			"getSumIf()"
	};
	
	public static final int functionsAvailable() {
		return functions.length;
	}
	
	public static final boolean isFunction(String s) {
		if(s == null)
			return false;
		for(String function : functions) {
			if(function.equals(s))
				return true;
		}
		return false;
	}

	public static final int getFunctionId(String function) {

		if (function != null) {

			for(int i = 0; i < functions.length; i++) {
				if(functions[i].equals(function)){
					return i;
				}
			}
		}
		return -1;
	}

	public static final String getFunction(int id) {

		if(id >= functions.length || id < 0) {
			return "";
		}
		return functions[id];
	}
	
	public static final String getFunctionCall(int id) {

		if(id >= functionCalls.length || id < 0) {
			return "";
		}
		return functionCalls[id];
	}

	
	public static final boolean[] getFunctionsRequired(String functionStr) {
		boolean functionsRequired[] = new boolean[SQLFunctionMap.functionsAvailable()];
		for(int i = 0; i < functionsRequired.length; i++) {
			functionsRequired[i] = false;
		}
		String[] functions = functionStr.split(",");
		for(int i = 0; i < functions.length; i++) {
			functions[i] = functions[i].trim(); 
			if(functions[i] != null && functions[i].equals("avg")) {
				functionsRequired[SQLFunctionMap.getFunctionId("avg")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("count")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("sum")] = true;
			}
			if(functions[i] != null && functions[i].equals("count")) {
				functionsRequired[SQLFunctionMap.getFunctionId("count")] = true;
			}
			if(functions[i] != null && functions[i].equals("count_distinct")) {
				functionsRequired[SQLFunctionMap.getFunctionId("count_distinct")] = true;
			}
			if(functions[i] != null && functions[i].equals("count_if")) {
				functionsRequired[SQLFunctionMap.getFunctionId("count_if")] = true;
			}
			if(functions[i] != null && functions[i].equals("first")) {
				functionsRequired[SQLFunctionMap.getFunctionId("first")] = true;
			}
			if(functions[i] != null && functions[i].equals("last")) {
				functionsRequired[SQLFunctionMap.getFunctionId("last")] = true;
			}
			if(functions[i] != null && functions[i].equals("max")) {
				functionsRequired[SQLFunctionMap.getFunctionId("max")] = true;
			}
			if(functions[i] != null && functions[i].equals("median")) {
				functionsRequired[SQLFunctionMap.getFunctionId("median")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("count")] = true;
			}
			if(functions[i] != null && functions[i].equals("min")) {
				functionsRequired[SQLFunctionMap.getFunctionId("min")] = true;
			}
			if(functions[i] != null && functions[i].equals("mode")) {
				functionsRequired[SQLFunctionMap.getFunctionId("mode")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("count")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("last")] = true;
			}
			if(functions[i] != null && functions[i].equals("stddev_pop")) {
				functionsRequired[SQLFunctionMap.getFunctionId("stddev_pop")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("count")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("sum")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("variance")] = true;
			}
			if(functions[i] != null && functions[i].equals("variance_pop")) {
				functionsRequired[SQLFunctionMap.getFunctionId("variance_pop")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("count")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("sum")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("variance")] = true;
			}
			if(functions[i] != null && functions[i].equals("previous")) {
				functionsRequired[SQLFunctionMap.getFunctionId("previous")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("last")] = true;
			}
			if(functions[i] != null && functions[i].equals("stddev")) {
				functionsRequired[SQLFunctionMap.getFunctionId("stddev")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("count")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("sum")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("variance")] = true;
			}
			if(functions[i] != null && functions[i].equals("variance")) {
				functionsRequired[SQLFunctionMap.getFunctionId("variance")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("count")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("sum")] = true;
			}
			if(functions[i] != null && functions[i].equals("slope")) {
				functionsRequired[SQLFunctionMap.getFunctionId("slope")] = true;
				functionsRequired[SQLFunctionMap.getFunctionId("sum")] = true;
			}
			if(functions[i] != null && functions[i].equals("sum")) {
				functionsRequired[SQLFunctionMap.getFunctionId("sum")] = true;
			}
			if(functions[i] != null && functions[i].equals("sum_if")) {
				functionsRequired[SQLFunctionMap.getFunctionId("sum_if")] = true;
			}
		}
		return functionsRequired;
	}
	
	public static final String getFunctionsAvailable(boolean[] functionsRequired) {
		String s = "";
		
		if(functionsRequired.length < functionsAvailable())
			return s;
		
		for(int i = 0; i < functions.length; i++) {
			if(functionsRequired[i]) {
				s = s + ",\""+ functions[i] +"\"";
			}
		}

		if(s.length() == 0)
			return s;
		
		s = s.substring(1); 
		
		return s;
	}
}

