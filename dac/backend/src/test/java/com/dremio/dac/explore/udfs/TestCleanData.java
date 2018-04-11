/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.dac.explore.udfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Class for testing individual helper functions that do not require starting the server.
 */
public class TestCleanData {

  @Test
  public void testFloatConversionSanityFilterRegex() {
    String[] inputData = {
        "+5", "001", ".6", "00.01", "0", "100", "-103.5",
        "5555.5", "-30", "0.01", "-0.2", "100e5",
        "100E5", "-5.1e10", "5.1e-10", "7e+6",
        "-100E+5", "+5.1e10"
    };
    for (String s : inputData) {
      assertTrue("regex did not match input expected to be valid number: " + s, CleanData.PATTERN.matcher(s).matches());
      // will throw runtime exception for invalid numbers
      Double.parseDouble(s);
    }
    String[] badInputData = { "--5", "", "++5.0", "a", "a5", "1:00", "1990-01-01", "[]", "{}", "{\"a\": 1}", "1,2,3", "[1,2,3]" };
    for (String s : badInputData) {
      assertFalse("regex did not filter out invalid number: " + s, CleanData.PATTERN.matcher(s).matches());
      try {
        Double.parseDouble(s);
        throw new RuntimeException("Expected number parsing failure did not occur for string: " + s);
      } catch (NumberFormatException ex) {
        // expected exception
      }
    }
  }

}
