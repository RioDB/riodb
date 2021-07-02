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

	Numeric Constants that are used throughout RioDB windows

 */


package org.riodb.windows;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public final class Constants {

	// Math Context for BigDecimal computation everywhere
	public static final MathContext MATH_CONTEXT = new MathContext(20, RoundingMode.HALF_UP);
	// A static BigDecimal assigned -1
	public static final BigDecimal NEGATIVE_ONE = new BigDecimal(-1);
	// A static BigDecimal assigned 1
	public static final BigDecimal ONE = new BigDecimal(1);
	
}
