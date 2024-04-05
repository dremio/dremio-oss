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
package com.dremio.exec.store.dfs;

import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.file.proto.FileConfig;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Field;

/** Describes previous state of dataset. */
public class PreviousDatasetInfo {
  private final FileConfig fileConfig;
  private final BatchSchema schema;
  private final List<String> sortColumns;
  private final List<Field> droppedColumns;
  private final List<Field> modifiedColumns;
  private final boolean isSchemaLearningEnabled;

  public PreviousDatasetInfo(
      FileConfig fileConfig,
      BatchSchema schema,
      List<String> sortColumns,
      List<Field> droppedColumns,
      List<Field> modifiedColumns,
      boolean isSchemaLearningEnabled) {
    super();
    this.fileConfig = fileConfig;
    this.schema = schema;
    this.sortColumns = sortColumns;
    this.droppedColumns = droppedColumns;
    this.modifiedColumns = modifiedColumns;
    this.isSchemaLearningEnabled = isSchemaLearningEnabled;
  }

  public FileConfig getFileConfig() {
    return fileConfig;
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public List<String> getSortColumns() {
    return sortColumns;
  }

  public List<Field> getDropColumns() {
    return droppedColumns;
  }

  public List<Field> getModifiedColumns() {
    return modifiedColumns;
  }

  public boolean isSchemaLearningEnabled() {
    return isSchemaLearningEnabled;
  }
}
