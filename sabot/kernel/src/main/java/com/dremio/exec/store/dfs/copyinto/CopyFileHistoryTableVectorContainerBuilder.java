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

import com.dremio.common.expression.BasePath;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.util.VectorUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for building {@link VectorContainer} to represent records in the
 * "copy_file_history" table copy-into operation. It provides a method to construct the records for
 * different schema versions.
 */
public final class CopyFileHistoryTableVectorContainerBuilder {

  private static final Logger logger =
      LoggerFactory.getLogger(CopyFileHistoryTableVectorContainerBuilder.class);

  private CopyFileHistoryTableVectorContainerBuilder() {}

  /**
   * Initializes a {@link VectorContainer} with the schema corresponding to the specified version.
   *
   * @param allocator The buffer allocator to use for vector allocation.
   * @param schema The iceberg schema.
   * @return A {@link VectorContainer} initialized with the specified schema version.
   */
  public static VectorContainer initializeContainer(BufferAllocator allocator, Schema schema) {
    VectorContainer container = new VectorContainer(allocator);
    container.addCollection(initializeVectors(allocator, schema));
    container.setAllCount(0);
    container.buildSchema();
    logger.debug(
        "Created empty vector container for copy_file_history table matching schema {}", schema);
    return container;
  }

  private static List<ValueVector> initializeVectors(BufferAllocator allocator, Schema schema) {
    List<ValueVector> vectors = new ArrayList<>();
    for (Types.NestedField col : schema.columns()) {
      FieldVector vector;
      switch (col.fieldId()) {
          // Initialize vectors based on the schema column definitions
        case 1: // event_timestamp
        case 10: // file_notification_timestamp
          vector = new TimeStampMilliVector(col.name(), allocator);
          break;
        case 2: // job_id
        case 3: // file_path
        case 4: // file_state
        case 7: // pipe_id
        case 9: // first_error_message
        case 11: // ingestion_source_type
        case 12: // request_id
          vector = new VarCharVector(col.name(), allocator);
          break;
        case 5: // records_loaded_count
        case 6: // records_rejected_count
        case 8: // file_size
          vector = new BigIntVector(col.name(), allocator);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "Cannot initialize copy_file_history column vector with name %s at index %s. Make sure that mapping to the iceberg schema is correct.",
                  col.name(), col.fieldId()));
      }
      vectors.add(vector);
    }
    return vectors;
  }

  /**
   * Writes a record represented by {@link CopyIntoFileLoadInfo} into the {@link VectorContainer}.
   *
   * @param container The {@link VectorContainer} to write the record to.
   * @param fileLoadInfo The {@link CopyIntoFileLoadInfo} representing the record.
   * @param recordTimestamp The timestamp of the record.
   */
  public static void writeRecordToContainer(
      VectorContainer container, CopyIntoFileLoadInfo fileLoadInfo, long recordTimestamp) {
    int recordCount = container.getRecordCount();
    BatchSchema schema = container.getSchema();
    for (Field field : schema.getFields()) {
      String fieldName = field.getName();
      TypedFieldId fieldId = schema.getFieldId(BasePath.getSimple(fieldName));
      ValueVector vector = VectorUtil.getVectorFromSchemaPath(container, fieldName);
      writeValueToVector(
          vector, fieldId.getFieldIds()[0], fileLoadInfo, recordTimestamp, recordCount);
    }
    container.setAllCount(recordCount + 1);
  }

  private static void writeValueToVector(
      ValueVector vector,
      int fieldId,
      CopyIntoFileLoadInfo fileLoadInfo,
      long recordTimestamp,
      int recordCount) {
    switch (fieldId) {
      case 0:
        // event_timestamp
        ((TimeStampMilliVector) vector).setSafe(recordCount, recordTimestamp);
        break;
      case 1:
        // job_id
        writeText(vector, fileLoadInfo.getQueryId(), recordCount);
        break;
      case 2:
        // file_path
        writeText(vector, fileLoadInfo.getFilePath(), recordCount);
        break;
      case 3:
        // file_state
        writeText(vector, fileLoadInfo.getFileState().name(), recordCount);
        break;
      case 4:
        // records_loaded_count
        ((BigIntVector) vector).setSafe(recordCount, fileLoadInfo.getRecordsLoadedCount());
        break;
      case 5:
        // records_rejected_count
        ((BigIntVector) vector).setSafe(recordCount, fileLoadInfo.getRecordsRejectedCount());
        break;
      case 6:
        // pipe_id
        writeText(vector, fileLoadInfo.getPipeId(), recordCount);
        break;
      case 7:
        // file_size
        ((BigIntVector) vector).setSafe(recordCount, fileLoadInfo.getFileSize());
        break;
      case 8:
        // first_error_message
        writeText(vector, fileLoadInfo.getFirstErrorMessage(), recordCount);
        break;
      case 9:
        // file_notification_timestamp
        if (fileLoadInfo.getFileNotificationTimestamp() != null) {
          ((TimeStampMilliVector) vector)
              .setSafe(recordCount, fileLoadInfo.getFileNotificationTimestamp());
        } else {
          ((TimeStampMilliVector) vector).setNull(recordCount);
        }
        break;
      case 10:
        // ingestion_source_type
        writeText(vector, fileLoadInfo.getIngestionSourceType(), recordCount);
        break;
      case 11:
        // request_id
        writeText(vector, fileLoadInfo.getRequestId(), recordCount);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unrecognized copy_file_history table column at index %s. Make sure that mapping to the iceberg schema is correct.",
                fieldId));
    }
  }

  /**
   * Writes a text value to the specified index of the given {@link FieldVector}.
   *
   * @param vector The ValueVector to write the text value to.
   * @param value The text value to write.
   * @param idx The index at which to write the text value.
   */
  private static void writeText(ValueVector vector, String value, int idx) {
    if (value == null) {
      ((VarCharVector) vector).setNull(idx);
    } else {
      ((VarCharVector) vector).setSafe(idx, new Text(value));
    }
  }
}
