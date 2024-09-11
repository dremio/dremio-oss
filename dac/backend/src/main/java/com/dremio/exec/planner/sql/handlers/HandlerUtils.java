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
package com.dremio.exec.planner.sql.handlers;

public class HandlerUtils {
  protected static final String REFERENCE_ALREADY_EXISTS_MESSAGE =
      "Reference %s already exists in Source %s.";

  // SQL_STATUS string will be used in sql result's "status" column to represent whether the sql has
  // been succeeded or failed.
  public static final String SQL_STATUS_SUCCESS = "SUCCESS";
  public static final String SQL_STATUS_FAILURE = "FAILURE";

  public static String statusCode(boolean success) {
    if (success) {
      return SQL_STATUS_SUCCESS;
    }
    return SQL_STATUS_FAILURE;
  }
}
