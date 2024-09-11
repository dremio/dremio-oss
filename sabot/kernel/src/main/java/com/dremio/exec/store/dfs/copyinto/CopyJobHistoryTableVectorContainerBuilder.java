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
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for building {@link VectorContainer} to represent records in the
 * "copy_job_history" table copy-into operation. It provides a method to construct the records for
 * different schema versions.
 */
public final class CopyJobHistoryTableVectorContainerBuilder {

  private static final Logger logger =
      LoggerFactory.getLogger(CopyJobHistoryTableVectorContainerBuilder.class);

  private CopyJobHistoryTableVectorContainerBuilder() {}

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
        "Created empty vector container for copy_job_history table matching schema {}", schema);
    return container;
  }

  /**
   * Initializes a list of {@link ValueVector}s based on the provided iceberg schema.
   *
   * @param allocator The buffer allocator to use for vector allocation.
   * @param schema The iceberg schema.
   * @return A list of {@link ValueVector}s initialized based on the provided iceberg schema.
   */
  private static List<ValueVector> initializeVectors(BufferAllocator allocator, Schema schema) {
    List<ValueVector> vectors = new ArrayList<>();
    for (Types.NestedField col : schema.columns()) {
      FieldVector vector;
      switch (col.fieldId()) {
        case 1: // executed_at
          vector = new TimeStampMilliVector(col.name(), allocator);
          break;
        case 2: // job_id
        case 3: // table_name
        case 6: // copy_options
        case 7: // user_name
        case 9: // storage_location
        case 10: // file_format
        case 11: // branch
        case 12: // pipe_name
        case 13: // pipe_id
          vector = new VarCharVector(col.name(), allocator);
          break;
        case 4: // records_loaded_count
        case 5: // records_rejected_count
        case 8: // base_snapshot_id
        case 14: // time_elapsed
          vector = new BigIntVector(col.name(), allocator);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "Cannot initialize copy_job_history column vector with name %s at index %s. Make sure that mapping to the iceberg schema is correct.",
                  col.name(), col.fieldId()));
      }
      vectors.add(vector);
    }
    return vectors;
  }

  /**
   * Writes a record to the specified {@link VectorContainer} based on the provided file load
   * information and other parameters.
   *
   * @param container The {@link VectorContainer} to write the record to.
   * @param fileLoadInfo The {@link CopyIntoFileLoadInfo} object containing file load information.
   * @param numSuccess The number of successfully loaded records.
   * @param numErrors The number of records that were rejected.
   * @param processingTime The time taken for processing the load operation.
   * @param recordTimestamp The timestamp of the record.
   */
  public static void writeRecordToContainer(
      VectorContainer container,
      CopyIntoFileLoadInfo fileLoadInfo,
      long numSuccess,
      long numErrors,
      long processingTime,
      long recordTimestamp) {
    BatchSchema schema = container.getSchema();
    for (Field field : schema.getFields()) {
      String fieldName = field.getName();
      TypedFieldId fieldId = schema.getFieldId(BasePath.getSimple(fieldName));
      ValueVector vector = VectorUtil.getVectorFromSchemaPath(container, fieldName);
      writeValueToVector(
          vector,
          fieldId.getFieldIds()[0],
          fileLoadInfo,
          numSuccess,
          numErrors,
          processingTime,
          recordTimestamp);
    }
    container.setAllCount(container.getRecordCount() + 1);
  }

  /**
   * Writes a value to the specified {@link ValueVector} based on the provided file load information
   * and other parameters.
   *
   * @param vector The {@link ValueVector} to write the value to.
   * @param fieldId The field ID of the value vector.
   * @param fileLoadInfo The {@link CopyIntoFileLoadInfo} object containing file load information.
   * @param numSuccess The number of successfully loaded records.
   * @param numErrors The number of records that were rejected.
   * @param processingTime The time taken for processing the load operation.
   * @param recordTimestamp The timestamp of the record.
   */
  private static void writeValueToVector(
      ValueVector vector,
      int fieldId,
      CopyIntoFileLoadInfo fileLoadInfo,
      long numSuccess,
      long numErrors,
      long processingTime,
      long recordTimestamp) {
    switch (fieldId) {
      case 0:
        // executed_at
        ((TimeStampMilliVector) vector).setSafe(0, recordTimestamp);
        break;
      case 1:
        // job_id
        writeText(vector, fileLoadInfo.getQueryId());
        break;
      case 2:
        // table_name
        writeText(vector, fileLoadInfo.getTableName());
        break;
      case 3:
        // records_loaded_count
        ((BigIntVector) vector).setSafe(0, numSuccess);
        break;
      case 4:
        // records_rejected_count
        ((BigIntVector) vector).setSafe(0, numErrors);
        break;
      case 5:
        // copy_options
        writeText(vector, CopyIntoFileLoadInfo.Util.getJson(fileLoadInfo.getFormatOptions()));
        break;
      case 6:
        // user_name
        writeText(vector, fileLoadInfo.getQueryUser());
        break;
      case 7:
        // base_snapshot_id
        ((BigIntVector) vector).setSafe(0, fileLoadInfo.getSnapshotId());
        break;
      case 8:
        // storage_location
        writeText(vector, fileLoadInfo.getStorageLocation());
        break;
      case 9:
        // file_format
        writeText(vector, fileLoadInfo.getFileFormat());
        break;
      case 10:
        // branch
        writeText(vector, fileLoadInfo.getBranch());
        break;
      case 11:
        // pipe_name
        writeText(vector, fileLoadInfo.getPipeName());
        break;
      case 12:
        // pipe_id
        writeText(vector, fileLoadInfo.getPipeId());
        break;
      case 13:
        // time_elapsed
        ((BigIntVector) vector).setSafe(0, processingTime);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unrecognized copy_file_history table column at index %s. Make sure that mapping to the iceberg schema is correct.",
                fieldId));
    }
  }

  /**
   * Constructs a {@link FieldVector} for a specified column in the copy_job_history table based on
   * the provided information.
   *
   * @param allocator The buffer allocator to use for vector allocation.
   * @param info The {@link CopyIntoFileLoadInfo} object containing file load information.
   * @param numSuccess The number of successfully loaded records.
   * @param numErrors The number of records that were rejected.
   * @param processingTime The time taken for processing the load operation.
   * @param recordTimestamp The timestamp of the record.
   * @param col The column definition from the iceberg schema.
   * @return A {@link FieldVector} representing the specified column for the given file load
   *     information.
   * @throws UnsupportedOperationException If the column fieldId is not recognized.
   */
  private static FieldVector getFieldVector(
      BufferAllocator allocator,
      CopyIntoFileLoadInfo info,
      long numSuccess,
      long numErrors,
      long processingTime,
      long recordTimestamp,
      NestedField col) {
    FieldVector vector;
    switch (col.fieldId()) {
      case 1:
        // executed_at
        vector = new TimeStampMilliVector(col.name(), allocator);
        ((TimeStampMilliVector) vector).allocateNew(1);
        ((TimeStampMilliVector) vector).setSafe(0, recordTimestamp);
        break;
      case 2:
        // job_id
        vector = new VarCharVector(col.name(), allocator);
        ((VarCharVector) vector).allocateNew(1);
        writeText(vector, info.getQueryId());
        break;
      case 3:
        // table_name
        vector = new VarCharVector(col.name(), allocator);
        ((VarCharVector) vector).allocateNew(1);
        writeText(vector, info.getTableName());
        break;
      case 4:
        // records_loaded_count
        vector = new BigIntVector(col.name(), allocator);
        ((BigIntVector) vector).allocateNew(1);
        ((BigIntVector) vector).setSafe(0, numSuccess);
        break;
      case 5:
        // records_rejected_count
        vector = new BigIntVector(col.name(), allocator);
        ((BigIntVector) vector).allocateNew(1);
        ((BigIntVector) vector).setSafe(0, numErrors);
        break;
      case 6:
        // copy_options
        vector = new VarCharVector(col.name(), allocator);
        ((VarCharVector) vector).allocateNew(1);
        writeText(vector, CopyIntoFileLoadInfo.Util.getJson(info.getFormatOptions()));
        break;
      case 7:
        // user_name
        vector = new VarCharVector(col.name(), allocator);
        ((VarCharVector) vector).allocateNew(1);
        writeText(vector, info.getQueryUser());
        break;
      case 8:
        // base_snapshot_id
        vector = new BigIntVector(col.name(), allocator);
        ((BigIntVector) vector).allocateNew(1);
        ((BigIntVector) vector).setSafe(0, info.getSnapshotId());
        break;
      case 9:
        // storage_location
        vector = new VarCharVector(col.name(), allocator);
        ((VarCharVector) vector).allocateNew(1);
        writeText(vector, info.getStorageLocation());
        break;
      case 10:
        // file_format
        vector = new VarCharVector(col.name(), allocator);
        ((VarCharVector) vector).allocateNew(1);
        writeText(vector, info.getFileFormat());
        break;
        // V2 schema
      case 11:
        // branch
        vector = new VarCharVector(col.name(), allocator);
        ((VarCharVector) vector).allocateNew(1);
        writeText(vector, info.getBranch());
        break;
      case 12:
        // pipe_name
        vector = new VarCharVector(col.name(), allocator);
        ((VarCharVector) vector).allocateNew(1);
        writeText(vector, info.getPipeName());
        break;
      case 13:
        // pipe_id
        vector = new VarCharVector(col.name(), allocator);
        ((VarCharVector) vector).allocateNew(1);
        writeText(vector, info.getPipeId());
        break;
      case 14:
        // time_elapsed
        vector = new BigIntVector(col.name(), allocator);
        ((BigIntVector) vector).allocateNew(1);
        ((BigIntVector) vector).setSafe(0, processingTime);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unrecognized copy_job_history table column. Make sure that mapping to the iceberg schema is correct.");
    }
    return vector;
  }

  /**
   * Writes a text value to the given {@link FieldVector}.
   *
   * @param vector The FieldVector to write the text value to.
   * @param value The text value to write.
   */
  private static void writeText(ValueVector vector, String value) {
    if (value == null) {
      ((VarCharVector) vector).setNull(0);
    } else {
      ((VarCharVector) vector).setSafe(0, new Text(value));
    }
  }
}
