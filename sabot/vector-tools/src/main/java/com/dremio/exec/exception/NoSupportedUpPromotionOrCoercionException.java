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

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.expression.CompleteType;

/**
 * Specific exception thrown in case no up promotion or coercion rules exist.
 */
public class NoSupportedUpPromotionOrCoercionException extends RuntimeException {
  private final CompleteType fileType;
  private final CompleteType tableType;
  private final List<String> columns;
  private List<String> datasetPath;
  private String filePath;

  public NoSupportedUpPromotionOrCoercionException(CompleteType fileType, CompleteType tableType) {
    this.fileType = fileType;
    this.tableType = tableType;
    columns = new ArrayList<>();
  }

  @Override
  public String getMessage() {
    StringBuilder sb = new StringBuilder();
    sb.append("Unable to coerce from the file's data type \"");
    sb.append(fileType);
    sb.append("\" to the column's data type \"");
    sb.append(tableType);
    sb.append("\"");
    if (datasetPath != null && datasetPath.size() > 0) {
      sb.append(" in table \"");
      for (int i = 0; i < datasetPath.size() - 1; i++) {
        sb.append(datasetPath.get(i));
        sb.append(".");
      }
      sb.append(datasetPath.get(datasetPath.size() - 1));
      sb.append("\"");
    }
    if (columns != null && columns.size() > 0) {
      sb.append(", column \"");
      for (int i = columns.size() - 1; i > 0; i--) {
        sb.append(columns.get(i));
        sb.append(".");
      }
      sb.append(columns.get(0));
      sb.append("\"");
    }
    if (filePath != null) {
      sb.append(" and file \"");
      sb.append(filePath);
      sb.append("\"");
    }
    return sb.toString();
  }

  public void addColumnName(String name) {
    columns.add(name);
  }

  public void addFilePath(String path) {
    this.filePath = path;
  }

  public void addDatasetPath(List<String> datasetPath) {
    this.datasetPath = datasetPath;
  }
}
