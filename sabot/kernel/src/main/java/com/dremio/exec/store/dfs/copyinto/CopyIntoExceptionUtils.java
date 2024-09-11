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
package com.dremio.exec.store.dfs.copyinto;

import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.vector.complex.fn.TransformationException;
import com.fasterxml.jackson.core.JsonParseException;

/** Utility class for handling exceptions that occur during the COPY INTO operation. */
public class CopyIntoExceptionUtils {
  private CopyIntoExceptionUtils() {}

  /**
   * /**
   *
   * <p>Redacts the sensitive information in the exception message and recreates it.
   *
   * @param t The throwable object (exception).
   * @return The redacted exception.
   */
  public static Throwable redactException(Throwable t) {
    if (t instanceof TransformationException) {
      return new TransformationException(
          redactMessage(t.getMessage()),
          ((TransformationException) t).getLineNumber(),
          ((TransformationException) t).getFieldName());
    } else if (t instanceof JsonParseException) {
      return new JsonParseException(((JsonParseException) t).getOriginalMessage());
    }
    return t;
  }

  /**
   * Redacts the exception message by removing specific sensitive information.
   *
   * @param message The exception message that needs redaction.
   * @return The redacted exception message.
   */
  public static String redactMessage(String message) {
    if (message.contains("No column name matches target schema")) {
      return message.replace(
          String.format(", %s::varchar", ColumnUtils.COPY_HISTORY_COLUMN_NAME), "");
    }

    String searchTerm = "is not in Parquet format.";
    if (message.contains(searchTerm)) {
      return message.substring(0, message.indexOf(searchTerm) + searchTerm.length());
    }
    return message;
  }
}
