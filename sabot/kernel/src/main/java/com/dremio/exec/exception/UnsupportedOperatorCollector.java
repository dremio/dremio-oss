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
package com.dremio.exec.exception;

import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.exec.work.foreman.UnsupportedDataTypeException;
import com.dremio.exec.work.foreman.UnsupportedFunctionException;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;

public class UnsupportedOperatorCollector {
  private SqlUnsupportedException.ExceptionType exceptionType;
  private String message;

  public UnsupportedOperatorCollector() {
    exceptionType = SqlUnsupportedException.ExceptionType.NONE;
  }

  public boolean convertException() throws SqlUnsupportedException {
    switch (exceptionType) {
      case RELATIONAL:
        clean();
        throw new UnsupportedRelOperatorException(message);
      case DATA_TYPE:
        clean();
        throw new UnsupportedDataTypeException(message);
      case FUNCTION:
        clean();
        throw new UnsupportedFunctionException(message);
      default:
        break;
    }

    return false;
  }

  public void setException(SqlUnsupportedException.ExceptionType exceptionType, String message) {
    if (this.exceptionType != SqlUnsupportedException.ExceptionType.NONE) {
      throw new IllegalStateException("Exception was set already");
    }

    this.exceptionType = exceptionType;
    this.message = message;
  }

  public void setException(SqlUnsupportedException.ExceptionType exceptionType) {
    setException(exceptionType, "");
  }

  public void clean() {
    exceptionType = SqlUnsupportedException.ExceptionType.NONE;
  }
}
