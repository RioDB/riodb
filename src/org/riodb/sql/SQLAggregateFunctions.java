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

public final class SQLAggregateFunctions {

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
		boolean functionsRequired[] = new boolean[SQLAggregateFunctions.functionsAvailable()];
		for(int i = 0; i < functionsRequired.length; i++) {
			functionsRequired[i] = false;
		}
		String[] functions = functionStr.split(",");
		for(int i = 0; i < functions.length; i++) {
			functions[i] = functions[i].trim(); 
			
			if(functions[i] != null && functions[i].equals("all")) {
				for(int j = 0; j < functionsRequired.length; j++) {
					functionsRequired[j] = true;
				}
				return functionsRequired;
			}
			
			if(functions[i] != null && functions[i].equals("avg")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("avg")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("count")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("sum")] = true;
			}
			if(functions[i] != null && functions[i].equals("count")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("count")] = true;
			}
			if(functions[i] != null && functions[i].equals("count_distinct")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("count_distinct")] = true;
			}
			if(functions[i] != null && functions[i].equals("count_if")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("count_if")] = true;
			}
			if(functions[i] != null && functions[i].equals("first")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("first")] = true;
			}
			if(functions[i] != null && functions[i].equals("last")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("last")] = true;
			}
			if(functions[i] != null && functions[i].equals("max")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("max")] = true;
			}
			if(functions[i] != null && functions[i].equals("median")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("median")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("count")] = true;
			}
			if(functions[i] != null && functions[i].equals("min")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("min")] = true;
			}
			if(functions[i] != null && functions[i].equals("mode")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("mode")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("count")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("last")] = true;
			}
			if(functions[i] != null && functions[i].equals("stddev_pop")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("stddev_pop")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("count")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("sum")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("variance")] = true;
			}
			if(functions[i] != null && functions[i].equals("variance_pop")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("variance_pop")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("count")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("sum")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("variance")] = true;
			}
			if(functions[i] != null && functions[i].equals("previous")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("previous")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("last")] = true;
			}
			if(functions[i] != null && functions[i].equals("stddev")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("stddev")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("count")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("sum")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("variance")] = true;
			}
			if(functions[i] != null && functions[i].equals("variance")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("variance")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("count")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("sum")] = true;
			}
			if(functions[i] != null && functions[i].equals("slope")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("slope")] = true;
				functionsRequired[SQLAggregateFunctions.getFunctionId("sum")] = true;
			}
			if(functions[i] != null && functions[i].equals("sum")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("sum")] = true;
			}
			if(functions[i] != null && functions[i].equals("sum_if")) {
				functionsRequired[SQLAggregateFunctions.getFunctionId("sum_if")] = true;
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

