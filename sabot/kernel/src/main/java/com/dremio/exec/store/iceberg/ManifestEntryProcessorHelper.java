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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.store.iceberg.IcebergPartitionData.getPartitionColumnClass;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

/**
 * Helper functions for ManifestEntryProcessor
 */
public class ManifestEntryProcessorHelper {

  @VisibleForTesting
  public static PartitionProtobuf.NormalizedPartitionInfo getDataFilePartitionInfo(
    PartitionSpec icebergPartitionSpec,
    Set<String> invalidColumnsForPruning,
    Schema fileSchema,
    Map<String, Field> nameToFieldMap,
    ContentFile<? extends ContentFile<?>> currentFile,
    long version) {
    PartitionProtobuf.NormalizedPartitionInfo.Builder partitionInfoBuilder = PartitionProtobuf.NormalizedPartitionInfo.newBuilder().setId(String.valueOf(1));

    // get table partition spec
    StructLike partitionStruct = currentFile.partition();
    for (int partColPos = 0; partColPos < partitionStruct.size(); ++partColPos) {
      PartitionField field = icebergPartitionSpec.fields().get(partColPos);
      /**
       * we can not send partition column value for 1. nonIdentity columns or 2. columns which was not partition but later added as partition columns.
       * because in case 1. information will be partial and scan will get incorrect values
       * in case2. initially when column was not partition we don't have value.
       */
      if(invalidColumnsForPruning != null && invalidColumnsForPruning.contains(fileSchema.findField(field.sourceId()).name())) {
        continue;
      }

      if(!field.transform().isIdentity()) {
        continue;
      }

      PartitionProtobuf.PartitionValue.Builder partitionValueBuilder = PartitionProtobuf.PartitionValue.newBuilder();
      String partColName = fileSchema.findColumnName(field.sourceId());
      partitionValueBuilder.setColumn(partColName);
      Object value = partitionStruct.get(partColPos, getPartitionColumnClass(icebergPartitionSpec, partColPos));
      writePartitionValue(partitionValueBuilder, value, nameToFieldMap.get(partColName.toLowerCase()));
      partitionInfoBuilder.addValues(partitionValueBuilder.build());
    }
    addImplicitCols(partitionInfoBuilder, version);
    return partitionInfoBuilder.build();
  }

  private static void addImplicitCols(PartitionProtobuf.NormalizedPartitionInfo.Builder partitionInfoBuilder, long version) {
    PartitionProtobuf.PartitionValue.Builder partitionValueBuilder = PartitionProtobuf.PartitionValue.newBuilder();
    partitionValueBuilder.setColumn(IncrementalUpdateUtils.UPDATE_COLUMN);
    partitionValueBuilder.setLongValue(version);
    partitionInfoBuilder.addValues(partitionValueBuilder.build());
  }

  private static void writePartitionValue(PartitionProtobuf.PartitionValue.Builder partitionValueBuilder, Object value, Field field) {
    if (value == null) {
      return;
    }
    if (value instanceof Long) {
      if (field.getType().equals(CompleteType.TIMESTAMP.getType())) {
        partitionValueBuilder.setLongValue((Long) value / 1_000);
      } else if (field.getType().equals(CompleteType.TIME.getType())) {
        partitionValueBuilder.setIntValue((int) ((Long) value / 1_000));
      } else {
        partitionValueBuilder.setLongValue((Long) value);
      }
    } else if (value instanceof Integer) {
      if (field.getType().equals(CompleteType.DATE.getType())) {
        partitionValueBuilder.setLongValue(TimeUnit.DAYS.toMillis((Integer) value));
      } else {
        partitionValueBuilder.setIntValue((Integer) value);
      }
    } else if (value instanceof String) {
      partitionValueBuilder.setStringValue((String) value);
    } else if (value instanceof Double) {
      partitionValueBuilder.setDoubleValue((Double) value);
    } else if (value instanceof Float) {
      partitionValueBuilder.setFloatValue((Float) value);
    } else if (value instanceof Boolean) {
      partitionValueBuilder.setBitValue((Boolean) value);
    } else if (value instanceof BigDecimal) {
      partitionValueBuilder.setBinaryValue(ByteString.copyFrom(((BigDecimal) value).unscaledValue().toByteArray()));
    } else if (value instanceof ByteBuffer) {
      partitionValueBuilder.setBinaryValue(ByteString.copyFrom(((ByteBuffer) value).array()));
    } else {
      throw new UnsupportedOperationException("Unexpected partition column value type: " + value.getClass());
    }
  }
}
