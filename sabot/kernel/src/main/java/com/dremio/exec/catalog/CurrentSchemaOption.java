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

import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.MetadataOption;
import com.dremio.exec.record.BatchSchema;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.Field;

public class CurrentSchemaOption
    implements GetMetadataOption, GetDatasetOption, ListPartitionChunkOption {

  private final BatchSchema batchSchema;
  private final List<Field> droppedColumns;
  private final List<Field> updatedColumns;
  private boolean isSchemaLearningEnabled = true;

  public CurrentSchemaOption(
      BatchSchema batchSchema, BatchSchema updatedColumns, BatchSchema droppedColumns) {
    this.batchSchema = batchSchema;
    this.droppedColumns = droppedColumns.getFields();
    this.updatedColumns = updatedColumns.getFields();
  }

  public CurrentSchemaOption(
      BatchSchema batchSchema,
      BatchSchema updatedColumns,
      BatchSchema droppedColumns,
      boolean isSchemaLearningEnabled) {
    this.batchSchema = batchSchema;
    this.droppedColumns = droppedColumns.getFields();
    this.updatedColumns = updatedColumns.getFields();
    this.isSchemaLearningEnabled = isSchemaLearningEnabled;
  }

  public BatchSchema getBatchSchema() {
    return batchSchema;
  }

  public List<Field> getDroppedColumns() {
    return droppedColumns;
  }

  public List<Field> getUpdatedColumns() {
    return updatedColumns;
  }

  public boolean isSchemaLearningEnabled() {
    return isSchemaLearningEnabled;
  }

  public static List<Field> getDroppedColumns(MetadataOption... options) {
    return Stream.of(options)
        .filter(o -> o instanceof CurrentSchemaOption)
        .findFirst()
        .map(o -> ((CurrentSchemaOption) o).getDroppedColumns())
        .orElse(null);
  }

  public static BatchSchema getSchema(MetadataOption... options) {
    return Stream.of(options)
        .filter(o -> o instanceof CurrentSchemaOption)
        .findFirst()
        .map(o -> ((CurrentSchemaOption) o).getBatchSchema())
        .orElse(null);
  }

  public static List<Field> getUpdatedColumns(MetadataOption... options) {
    return Stream.of(options)
        .filter(o -> o instanceof CurrentSchemaOption)
        .findFirst()
        .map(o -> ((CurrentSchemaOption) o).getUpdatedColumns())
        .orElse(null);
  }

  public static Boolean isSchemaLearningEnabled(MetadataOption... options) {
    return Stream.of(options)
        .filter(o -> o instanceof CurrentSchemaOption)
        .findFirst()
        .map(o -> ((CurrentSchemaOption) o).isSchemaLearningEnabled())
        .orElse(true);
  }
}
