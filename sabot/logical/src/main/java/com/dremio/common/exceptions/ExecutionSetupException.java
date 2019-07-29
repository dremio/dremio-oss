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

import java.lang.reflect.InvocationTargetException;

public class ExecutionSetupException extends Exception {

  public static ExecutionSetupException fromThrowable(String message, Throwable cause) {
    Throwable t = cause instanceof InvocationTargetException
        ? ((InvocationTargetException)cause).getTargetException() : cause;
    if (t instanceof ExecutionSetupException) {
      return ((ExecutionSetupException) t);
    }
    return new ExecutionSetupException(message, t);
  }
  public ExecutionSetupException() {
    super();

  }

  public ExecutionSetupException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);

  }

  public ExecutionSetupException(String message, Throwable cause) {
    super(message, cause);

  }

  public ExecutionSetupException(String message) {
    super(message);

  }

  public ExecutionSetupException(Throwable cause) {
    super(cause);

  }
}
