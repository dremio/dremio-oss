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
package com.dremio.exec.catalog;

/** Thrown when dataset has a complex type column that is deeply nested */
public class ColumnNestedTooDeepException extends DatasetMetadataTooLargeException {
  private static final long serialVersionUID = 7765148243332604248L;

  public static final String MESSAGE =
      "Column ‘%s’ exceeded the maximum number of nested levels of %d";

  public ColumnNestedTooDeepException(String name, int limit) {
    super(String.format(MESSAGE, name, limit));
  }
}
