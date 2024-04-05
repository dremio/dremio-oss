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
import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.safeGetTransformType;
import static com.dremio.service.namespace.dataset.proto.PartitionProtobuf.IcebergTransformType;
import static com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import static com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionValue;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.store.SystemSchemas;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;

/** Helper functions for ManifestEntryProcessor */
public class ManifestEntryProcessorHelper {

  @VisibleForTesting
  public static NormalizedPartitionInfo getDataFilePartitionInfo(
      PartitionSpec icebergPartitionSpec,
      Set<String> invalidColumnsForPruning,
      Schema fileSchema,
      Map<String, Field> nameToFieldMap,
      ContentFile<? extends ContentFile<?>> currentFile,
      long version,
      long sequenceNo,
      final boolean addIcebergTransformationInfo) {
    NormalizedPartitionInfo.Builder partitionInfoBuilder =
        NormalizedPartitionInfo.newBuilder().setId(String.valueOf(1));

    // get table partition spec
    StructLike partitionStruct = currentFile.partition();
    for (int partColPos = 0; partColPos < partitionStruct.size(); ++partColPos) {
      PartitionField field = icebergPartitionSpec.fields().get(partColPos);
      /**
       * we can not send partition column value for 1. nonIdentity columns or 2. columns which was
       * not partition but later added as partition columns. because in case 1. information will be
       * partial and scan will get incorrect values in case2. initially when column was not
       * partition we don't have value.
       */
      final boolean isFieldInvalidForPruning =
          (invalidColumnsForPruning != null)
              && invalidColumnsForPruning.contains(fileSchema.findField(field.sourceId()).name());
      if (!isFieldInvalidForPruning && field.transform().isIdentity()) {
        final PartitionValue partitionValue =
            buildPartitionValue(
                fileSchema,
                field,
                partitionStruct,
                partColPos,
                icebergPartitionSpec,
                nameToFieldMap,
                true,
                field.transform().isIdentity());
        partitionInfoBuilder.addValues(partitionValue);
      }
      // The main idea is that if addIcebergTransformationInfo we will attach some Iceberg specific
      // information
      // to the PartitionProtobuf.NormalizedPartitionInfo that this function returns.
      // This includes the iceberg transform for each value, any arguments, and the raw iceberg
      // value.
      // The raw value is needed as writePartitionValue sometimes writes the value with reduced
      // precision,
      // and when we need it, it can be hard to determine if writePartitionValue modified the
      // original value or not
      if (addIcebergTransformationInfo) {
        addIcebergPartitionInfoToBuilder(
            partitionInfoBuilder,
            field,
            fileSchema,
            partitionStruct,
            partColPos,
            icebergPartitionSpec,
            nameToFieldMap);
      }
    }
    addImplicitCols(partitionInfoBuilder, version, sequenceNo);
    return partitionInfoBuilder.build();
  }

  /**
   * Attaches an IcebergPartitionInfo to partitionInfoBuilder It contains more detailed information
   * about the partition such as iceberg transform for each value, any arguments, and the raw
   * iceberg value.
   *
   * @param partitionInfoBuilder builder to attach the IcebergPartitionInfo to
   * @param field current field
   * @param fileSchema the schema for this file
   * @param partitionStruct partition information
   * @param partColPos column position
   * @param icebergPartitionSpec iceberg spec
   * @param nameToFieldMap mapping between name and field
   */
  private static void addIcebergPartitionInfoToBuilder(
      final NormalizedPartitionInfo.Builder partitionInfoBuilder,
      final PartitionField field,
      final Schema fileSchema,
      final StructLike partitionStruct,
      final int partColPos,
      final PartitionSpec icebergPartitionSpec,
      final Map<String, Field> nameToFieldMap) {
    final String transformName = field.transform().toString();
    Optional<IcebergTransformType> transformType = safeGetTransformType(transformName);
    if (transformType.isPresent()) {
      final PartitionValue partitionValueIceberg =
          buildPartitionValue(
              fileSchema,
              field,
              partitionStruct,
              partColPos,
              icebergPartitionSpec,
              nameToFieldMap,
              false, // Iceberg uses microseconds, so passing false here
              IcebergTransformType.IDENTITY.equals(transformType.get()));
      partitionInfoBuilder.addIcebergValues(partitionValueIceberg);
    }
  }

  private static PartitionValue buildPartitionValue(
      final Schema fileSchema,
      final PartitionField field,
      final StructLike partitionStruct,
      final int partColPos,
      final PartitionSpec icebergPartitionSpec,
      final Map<String, Field> nameToFieldMap,
      final boolean useMilliSeconds,
      boolean isIdentityTransform) {
    final PartitionValue.Builder partitionValueBuilder = PartitionValue.newBuilder();
    final String partColName = fileSchema.findColumnName(field.sourceId());
    partitionValueBuilder.setColumn(partColName);
    final Object value =
        partitionStruct.get(partColPos, getPartitionColumnClass(icebergPartitionSpec, partColPos));
    writePartitionValue(
        partitionValueBuilder,
        value,
        nameToFieldMap.get(partColName.toLowerCase()),
        useMilliSeconds,
        isIdentityTransform);
    return partitionValueBuilder.build();
  }

  private static void addImplicitCols(
      NormalizedPartitionInfo.Builder partitionInfoBuilder, long version, long sequenceNo) {
    PartitionValue.Builder partitionValueBuilder = PartitionValue.newBuilder();
    partitionValueBuilder.setColumn(IncrementalUpdateUtils.UPDATE_COLUMN);
    partitionValueBuilder.setLongValue(version);
    partitionInfoBuilder.addValues(partitionValueBuilder.build());

    // Sequence number as an implicit partition col value, so it can be projected if needed along
    // with the data.
    partitionInfoBuilder.addValues(
        PartitionValue.newBuilder()
            .setColumn(SystemSchemas.IMPLICIT_SEQUENCE_NUMBER)
            .setLongValue(sequenceNo)
            .build());
  }

  public static void writePartitionValue(
      final PartitionValue.Builder partitionValueBuilder,
      final Object value,
      final Field field,
      final boolean useMilliSeconds,
      final boolean isIdentityTransform) {
    if (value == null) {
      return;
    }
    if (value instanceof Long) {
      if (useMilliSeconds && field.getType().equals(CompleteType.TIMESTAMP.getType())) {
        partitionValueBuilder.setLongValue((Long) value / 1_000);
      } else if (useMilliSeconds && field.getType().equals(CompleteType.TIME.getType())) {
        partitionValueBuilder.setIntValue((int) ((Long) value / 1_000));
      } else {
        partitionValueBuilder.setLongValue((Long) value);
      }
    } else if (value instanceof Integer) {
      if (useMilliSeconds && field.getType().equals(CompleteType.DATE.getType())) {
        partitionValueBuilder.setLongValue(TimeUnit.DAYS.toMillis((Integer) value));
      } else if (isIdentityTransform && field.getType().equals(CompleteType.DATE.getType())) {
        partitionValueBuilder.setLongValue(TimeUnit.DAYS.toMicros((Integer) value));
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
      partitionValueBuilder.setBinaryValue(
          ByteString.copyFrom(((BigDecimal) value).unscaledValue().toByteArray()));
    } else if (value instanceof ByteBuffer) {
      partitionValueBuilder.setBinaryValue(ByteString.copyFrom(((ByteBuffer) value).array()));
    } else {
      throw new UnsupportedOperationException(
          "Unexpected partition column value type: " + value.getClass());
    }
  }
}
