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
package com.dremio.exec.vector.complex.fn;

import java.io.IOException;

/** Describes Exception while during Transformations in "COPY-INTO" command */
public class TransformationException extends IOException {

  private final int lineNumber;
  private final String fieldName;

  public TransformationException(String message, int lineNumber) {
    this(message, lineNumber, null);
  }

  public TransformationException(String message, int lineNumber, String fieldName) {
    super(message);
    this.lineNumber = lineNumber;
    this.fieldName = fieldName;
  }

  public int getLineNumber() {
    return lineNumber;
  }

  public String getFieldName() {
    return fieldName;
  }
}
