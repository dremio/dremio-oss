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

import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.record.VectorContainer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for building {@link VectorContainer} to represent records in the
 * "copy_job_history" table copy-into operation. It provides a method to construct the records for
 * different schema versions.
 */
public final class CopyJobHistoryTableRecordBuilder {

  private static final Logger logger =
      LoggerFactory.getLogger(CopyJobHistoryTableRecordBuilder.class);

  private CopyJobHistoryTableRecordBuilder() {}

  /**
   * Builds and returns a {@link VectorContainer} object representing records for the specified schema version.
   * @param allocator     The allocator used fpr allocating value vectors.
   * @param schemaVersion The version of the schema for which to build the record.
   * @param info          The {@link CopyIntoFileLoadInfo} containing error information to include in the record.
   * @param numSuccess    The number of successful operations to include in the record.
   * @param numErrors     The number of error occurrences to include in the record.
   * @return The {@link VectorContainer| object representing the records for the given schema version.
   * @throws UnsupportedOperationException If the specified schema version is not supported.
   */
  public static VectorContainer buildVector(
      BufferAllocator allocator,
      long schemaVersion,
      CopyIntoFileLoadInfo info,
      long numSuccess,
      long numErrors) {
    Schema schema = CopyJobHistoryTableSchemaProvider.getSchema(schemaVersion);
    if (schemaVersion == 1) {
      return buildVectorForV1Schema(allocator, schema, info, numSuccess, numErrors);
    }
    throw new UnsupportedOperationException(
        String.format(
            "Cannot create copy_job_history table records vector for schema version %s",
            schemaVersion));
  }

  /**
   * Builds and returns a {@link VectorContainer object representing a record for schema version 1. Each vector in the
   * container will contain exactly on element.
   * @param allocator  The allocator used fpr allocating value vectors.
   * @param schema     The schema of the record to build.
   * @param info       The {@link CopyIntoFileLoadInfo } containing error information to include in the record.
   * @param numSuccess The number of successful operations to include in the record.
   * @param numErrors  The number of error occurrences to include in the record.
   * @return The {@link VectorContainer} object representing the record for schema version 1.
   * @throws UnsupportedOperationException If there is an unrecognized column in the schema.
   */
  private static VectorContainer buildVectorForV1Schema(
      BufferAllocator allocator,
      Schema schema,
      CopyIntoFileLoadInfo info,
      long numSuccess,
      long numErrors) {
    VectorContainer container = new VectorContainer(allocator);
    for (Types.NestedField col : schema.columns()) {
      FieldVector vector;
      switch (col.fieldId()) {
        case 1:
          vector = new TimeStampMilliVector(col.name(), allocator);
          ((TimeStampMilliVector) vector).allocateNew(1);
          ((TimeStampMilliVector) vector).set(0, System.currentTimeMillis());
          break;
        case 2:
          vector = new VarCharVector(col.name(), allocator);
          ((VarCharVector) vector).allocateNew(1);
          ((VarCharVector) vector).set(0, new Text(info.getQueryId()));
          break;
        case 3:
          vector = new VarCharVector(col.name(), allocator);
          ((VarCharVector) vector).allocateNew(1);
          ((VarCharVector) vector).set(0, new Text(info.getTableName()));
          break;
        case 4:
          vector = new BigIntVector(col.name(), allocator);
          ((BigIntVector) vector).allocateNew(1);
          ((BigIntVector) vector).set(0, numSuccess);
          break;
        case 5:
          vector = new BigIntVector(col.name(), allocator);
          ((BigIntVector) vector).allocateNew(1);
          ((BigIntVector) vector).set(0, numErrors);
          break;
        case 6:
          vector = new VarCharVector(col.name(), allocator);
          ((VarCharVector) vector).allocateNew(1);
          ((VarCharVector) vector)
              .set(0, new Text(CopyIntoFileLoadInfo.Util.getJson(info.getFormatOptions())));
          break;
        case 7:
          vector = new VarCharVector(col.name(), allocator);
          ((VarCharVector) vector).allocateNew(1);
          ((VarCharVector) vector).set(0, new Text(info.getQueryUser()));
          break;
        case 8:
          vector = new BigIntVector(col.name(), allocator);
          ((BigIntVector) vector).allocateNew(1);
          ((BigIntVector) vector).set(0, info.getSnapshotId());
          break;
        case 9:
          vector = new VarCharVector(col.name(), allocator);
          ((VarCharVector) vector).allocateNew(1);
          ((VarCharVector) vector).set(0, new Text(info.getStorageLocation()));
          break;
        case 10:
          vector = new VarCharVector(col.name(), allocator);
          ((VarCharVector) vector).allocateNew(1);
          ((VarCharVector) vector).set(0, new Text(info.getFileFormat()));
          break;
        default:
          throw new UnsupportedOperationException(
              "Unrecognized copy_job_history table column. Make sure that mapping to the iceberg schema is correct.");
      }
      vector.setValueCount(1);
      container.add(vector);
    }
    container.setRecordCount(1);
    container.buildSchema();
    logger.debug(
        "Created copy_job_history iceberg table records vector matching iceberg schema {}", schema);
    return container;
  }
}
