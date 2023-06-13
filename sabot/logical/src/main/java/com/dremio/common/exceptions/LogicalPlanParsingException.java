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
package com.dremio.common.exceptions;

public class LogicalPlanParsingException extends RuntimeException{

  public LogicalPlanParsingException() {
    super();
  }

  public LogicalPlanParsingException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public LogicalPlanParsingException(String message, Throwable cause) {
    super(message, cause);
  }

  public LogicalPlanParsingException(String message) {
    super(message);
  }

  public LogicalPlanParsingException(Throwable cause) {
    super(cause);
  }

}
