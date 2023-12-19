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

import java.util.List;
import java.util.stream.IntStream;

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

import com.dremio.exec.physical.config.copyinto.CopyIntoErrorInfo;
import com.dremio.exec.record.VectorContainer;

/**
 *  This class is responsible for building {@link VectorContainer} to represent records in the "copy_file_history" table
 *  copy-into operation. It provides a method to construct the records for different schema versions.
 */
public final class CopyFileHistoryTableRecordBuilder {

  private static final Logger logger = LoggerFactory.getLogger(CopyFileHistoryTableRecordBuilder.class);

  private CopyFileHistoryTableRecordBuilder() {}

  public static VectorContainer buildVector(BufferAllocator allocator, long schemaVersion, List<CopyIntoErrorInfo> errorInfos) {
    Schema schema = CopyFileHistoryTableSchemaProvider.getSchema(schemaVersion);
    if (schemaVersion == 1) {
      return buildVectorForV1Schema(allocator, schema, errorInfos);
    }
    throw new UnsupportedOperationException(
      String.format("Cannot create copy_job_history table records vector for schema version %s", schemaVersion));
  }

  private static VectorContainer buildVectorForV1Schema(BufferAllocator allocator, Schema schema, List<CopyIntoErrorInfo> errorInfos) {
    VectorContainer container = new VectorContainer(allocator);
    if (errorInfos.isEmpty()) {
      container.setRecordCount(0);
      return container;
    }
    int recordCount = errorInfos.size();
    for (Types.NestedField col : schema.columns()) {
      FieldVector vector;
      switch (col.fieldId()) {
        case 1:
          vector = new TimeStampMilliVector(col.name(), allocator);
          ((TimeStampMilliVector) vector).allocateNew(recordCount);
          long currentTimeMillis = System.currentTimeMillis();
          IntStream.range(0, recordCount).forEach(i -> ((TimeStampMilliVector) vector).setSafe(i, currentTimeMillis));
          break;
        case 2:
          vector = new VarCharVector(col.name(), allocator);
          ((VarCharVector) vector).allocateNew(recordCount);
          IntStream.range(0, recordCount).forEach(i -> ((VarCharVector) vector)
            .setSafe(i, new Text(errorInfos.get(i).getQueryId())));
          break;
        case 3:
          vector = new VarCharVector(col.name(), allocator);
          ((VarCharVector) vector).allocateNew(recordCount);
          IntStream.range(0, recordCount).forEach(i -> ((VarCharVector) vector).setSafe(i, new Text(errorInfos.get(i).getFilePath())));
          break;
        case 4:
          vector = new VarCharVector(col.name(), allocator);
          ((VarCharVector) vector).allocateNew(recordCount);
          IntStream.range(0, recordCount).forEach(i -> ((VarCharVector) vector).setSafe(i, new Text(errorInfos.get(i).getFileState().name())));
          break;
        case 5:
          vector = new BigIntVector(col.name(), allocator);
          ((BigIntVector) vector).allocateNew(recordCount);
          IntStream.range(0, recordCount).forEach(i -> ((BigIntVector) vector).setSafe(i, errorInfos.get(i).getRecordsLoadedCount()));
          break;
        case 6:
          vector = new BigIntVector(col.name(), allocator);
          ((BigIntVector) vector).allocateNew(recordCount);
          IntStream.range(0, recordCount).forEach(i -> ((BigIntVector) vector).setSafe(i, errorInfos.get(i).getRecordsRejectedCount()));
          break;
        default:
          throw new UnsupportedOperationException(
            "Unrecognized copy_file_history table column. Make sure that mapping to the iceberg schema is correct.");
      }
      vector.setValueCount(recordCount);
      container.add(vector);
    }
    container.setRecordCount(recordCount);
    container.buildSchema();
    logger.debug("Created copy_file_history iceberg table records vector matching iceberg schema {}", schema);
    return container;
  }
}
