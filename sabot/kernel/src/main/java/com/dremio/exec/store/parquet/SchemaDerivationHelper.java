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
package com.dremio.exec.store.parquet;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.store.parquet.ParquetReaderUtility.DateCorruptionStatus;
import com.google.common.base.Preconditions;

/**
 * Contains set of configuration properties and output schema based on original table schema to help interpret data
 * from an underlying parquet file.
 *
 * We have various reinterpretation of parquet data based on metadata outside of the parquet file.
 * Following are such metadata:
 *   1) Read INT96 as TIMESTAMP
 *   2) Read BINARY as VARCHAR if the type is stored as VARCHAR in Hive table metadata,
 *   but the parquet file has no logical type (this is a problem for files created in Impala)
 *   3) Date correction logic for incorrectly written date in older Drill versions
 */
public class SchemaDerivationHelper {
  private final boolean readInt96AsTimeStamp;
  private final DateCorruptionStatus dateCorruptionStatus;
  private final boolean noSchemaLearning;
  private final BatchSchema schemaFromTableMetadata;
  private final boolean allowMixedDecimals;
  private final boolean limitListItems;

  private SchemaDerivationHelper(final boolean readInt96AsTimeStamp, final DateCorruptionStatus dateCorruptionStatus,
      final boolean noSchemaLearning, final boolean allowMixedDecimals,
      final BatchSchema schemaFromTableMetadata, final boolean limitListItems) {
    this.readInt96AsTimeStamp = readInt96AsTimeStamp;
    this.dateCorruptionStatus = dateCorruptionStatus;
    this.noSchemaLearning = noSchemaLearning;
    this.schemaFromTableMetadata = schemaFromTableMetadata;
    this.allowMixedDecimals = allowMixedDecimals;
    this.limitListItems = limitListItems;
  }

  /**
   * Tells the reader to interpret INT96 physical type as timestamp
   * @return
   */
  public boolean readInt96AsTimeStamp() {
    return readInt96AsTimeStamp;
  }

  /**
   * Drill had a bug where it incorrectly wrote DATE type values (see https://issues.apache.org/jira/browse/DRILL-4203)
   * Returned value determines the status of corruption.
   * @return
   */
  public DateCorruptionStatus getDateCorruptionStatus() {
    return dateCorruptionStatus;
  }

  /**
   * Is the given column which is of binary should be reinterpreted as UTF-8 encoded varchar? Impala writes varchars
   * as binary without UTF-8 annotation.
   * See https://issues.apache.org/jira/browse/IMPALA-2069 for details.
   *
   * @param column
   * @return
   */
  public boolean isVarChar(final SchemaPath column) {
    if (!noSchemaLearning) {
      return false;
    }
    final TypedFieldId id = schemaFromTableMetadata.getFieldId(column);
    return id != null && CompleteType.VARCHAR.equals(id.getFinalType());
  }

  /**
   *
   * @return returns true if reader is capable of handling mixed decimals
   */
  public boolean isAllowMixedDecimals() {
    return this.allowMixedDecimals;
  }

  /**
   *
   * @return true if number of list items should be limited
   */
  public boolean isLimitListItems() {
    return limitListItems;
  }
  /**
   * Get builder class
   * @return
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Schema handle for readers to handle precision and scale compatibility
   * @param filteredColumn
   * @return
   */
  public CompleteType getType(String filteredColumn) {
    return schemaFromTableMetadata.getFieldId(SchemaPath.getCompoundPath(filteredColumn.split("\\."))).getFinalType();
  }

  public static class Builder {
    private boolean readInt96AsTimeStamp;
    private DateCorruptionStatus dateCorruptionStatus;
    private boolean noSchemaLearning;
    private BatchSchema schemaFromTableMetadata;
    private boolean allowMixedDecimals = false;
    private boolean limitListItems = false;

    private Builder() { }

    public Builder readInt96AsTimeStamp(boolean readInt96AsTimeStamp) {
      this.readInt96AsTimeStamp = readInt96AsTimeStamp;
      return this;
    }

    public Builder dateCorruptionStatus(DateCorruptionStatus dateCorruptionStatus) {
      this.dateCorruptionStatus = dateCorruptionStatus;
      return this;
    }

    public Builder allowMixedDecimals(boolean allowMixedDecimals) {
      this.allowMixedDecimals = allowMixedDecimals;
      return this;
    }

    public Builder noSchemaLearning(BatchSchema schemaFromTableMetadata) {
      Preconditions.checkNotNull(schemaFromTableMetadata, "Expected a non-null schema from table metadata");
      this.noSchemaLearning = true;
      this.schemaFromTableMetadata = schemaFromTableMetadata;
      return this;
    }

    public Builder limitListItems(boolean limitListItems) {
      this.limitListItems = limitListItems;
      return this;
    }

    public SchemaDerivationHelper build() {
      return new SchemaDerivationHelper(readInt96AsTimeStamp, dateCorruptionStatus, noSchemaLearning,
          allowMixedDecimals, schemaFromTableMetadata, limitListItems);
    }
  }
}
