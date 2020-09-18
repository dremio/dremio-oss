/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.dac.util;

/**
 * Utility class to Invalid Query Error message converter
 */
public class InvalidQueryErrorConverter {

  private static final String NULL_USE = "Illegal use of 'NULL'";
  private static final String NULL_USE_USER_MSG = "We do not support NULL as a return data type in SELECT. Cast it " +
    "to a different type.";

  /**
   * Coverts system query error message to user friendly message
   */
  public static String convert(String errorMessage) {
    switch (errorMessage) {
      case NULL_USE:
        return NULL_USE_USER_MSG;
      default:
        return errorMessage;

    }
  }
}
