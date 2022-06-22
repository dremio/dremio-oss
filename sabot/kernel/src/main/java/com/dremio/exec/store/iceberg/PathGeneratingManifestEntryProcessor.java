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
import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;
import static com.dremio.exec.store.iceberg.IcebergUtils.getValueFromByteBuffer;
import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;
import static com.dremio.exec.store.iceberg.model.IcebergConstants.FILE_VERSION;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DremioManifestReaderUtils.ManifestEntryWrapper;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Manifest entry processor which handles both data and delete manifest scans.
 */
public class PathGeneratingManifestEntryProcessor implements ManifestEntryProcessor {

  private static final Set<String> BASE_OUTPUT_FIELDS = Stream.concat(
          SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA.getFields().stream(),
          SystemSchemas.ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA.getFields().stream())
      .map(f -> f.getName().toLowerCase())
      .collect(Collectors.toSet());

  private final BatchSchema outputSchema;
  private final Map<String, Field> nameToFieldMap;
  private final Set<String> invalidColumnsForPruning;
  private final HashMap<Field, ValueVector> columnStatsVectorMap = new HashMap<>();
  private final ManifestContent manifestContent;

  private Map<String, Integer> colToIDMap;
  private VarBinaryVector inputColIds;
  private VarCharVector outputFilePath;
  private BigIntVector outputFileSize;
  private IntVector outputFileType;
  private BigIntVector outputSequenceNumber;
  private IntVector outputSpecId;
  private VarBinaryVector outputPartitionKey;
  private VarBinaryVector outputPartitionInfo;
  private VarBinaryVector outputColIds;
  private PartitionSpec icebergPartitionSpec;
  private Map<String, Integer> partColToKeyMap;

  private boolean doneWithCurrentEntry;

  public PathGeneratingManifestEntryProcessor(ManifestScanTableFunctionContext functionContext) {
    outputSchema = functionContext.getFullSchema();
    BatchSchema tableSchema = functionContext.getTableSchema();
    nameToFieldMap = tableSchema.getFields().stream().collect(Collectors.toMap(f -> f.getName().toLowerCase(), f -> f));
    Map<Integer, PartitionSpec> partitionSpecMap = functionContext.getJsonPartitionSpecMap() != null ?
        IcebergSerDe.deserializeJsonPartitionSpecMap(
            deserializedJsonAsSchema(functionContext.getIcebergSchema()),
            functionContext.getJsonPartitionSpecMap().toByteArray()) :
        null;
    invalidColumnsForPruning = IcebergUtils.getInvalidColumnsForPruning(partitionSpecMap);
    manifestContent = functionContext.getManifestContent();
  }

  @Override
  public void setup(VectorAccessible incoming, VectorAccessible outgoing) {
    inputColIds = (VarBinaryVector) getVectorFromSchemaPath(incoming, SystemSchemas.COL_IDS);
    outputSequenceNumber = (BigIntVector) getVectorFromSchemaPath(outgoing, SystemSchemas.SEQUENCE_NUMBER);
    outputSpecId = (IntVector) getVectorFromSchemaPath(outgoing, SystemSchemas.PARTITION_SPEC_ID);
    outputPartitionKey = (VarBinaryVector) getVectorFromSchemaPath(outgoing, SystemSchemas.PARTITION_KEY);
    // output columns vary between data and delete manifest scans
    if (manifestContent == ManifestContent.DATA) {
      outputFilePath = (VarCharVector) getVectorFromSchemaPath(outgoing, SystemSchemas.DATAFILE_PATH);
      outputFileSize = (BigIntVector) getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_SIZE);
      outputPartitionInfo = (VarBinaryVector) getVectorFromSchemaPath(outgoing, SystemSchemas.PARTITION_INFO);
      outputColIds = (VarBinaryVector) getVectorFromSchemaPath(outgoing, SystemSchemas.COL_IDS);
    } else {
      outputFilePath = (VarCharVector) getVectorFromSchemaPath(outgoing, SystemSchemas.DELETEFILE_PATH);
      outputFileType = (IntVector) getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_CONTENT);
    }

    for (Field field : outputSchema.getFields()) {
      if (isColumnStatsOutputField(field)) {
        ValueVector vector = outgoing.getValueAccessorById(TypeHelper.getValueVectorClass(field),
            outgoing.getSchema().getFieldId(SchemaPath.getSimplePath(field.getName())).getFieldIds()).getValueVector();
        columnStatsVectorMap.put(field, vector);
      }
    }
  }

  @Override
  public void initialise(PartitionSpec partitionSpec) {
    icebergPartitionSpec = partitionSpec;
    colToIDMap = getColToIDMap();
    partColToKeyMap = new HashMap<>();

    for (int i = 0; i < icebergPartitionSpec.fields().size(); i++) {
      PartitionField partitionField = icebergPartitionSpec.fields().get(i);
      if (partitionField.transform().isIdentity()) {
        partColToKeyMap.put(icebergPartitionSpec.schema().findField(partitionField.sourceId()).name().toLowerCase(), i);
      }
    }
  }

  @Override
  public int processManifestEntry(ManifestEntryWrapper<?> manifestEntry, int startOutIndex,
      int maxOutputCount) throws IOException {
    if (!shouldProcessCurrentEntry(maxOutputCount)) {
      return 0;
    }
    String modifiedPath = manifestEntry.file().path().toString();
    byte[] path = modifiedPath.getBytes();
    outputFilePath.setSafe(startOutIndex, path);
    outputSequenceNumber.setSafe(startOutIndex, manifestEntry.sequenceNumber());
    outputSpecId.setSafe(startOutIndex, manifestEntry.file().specId());
    outputPartitionKey.setSafe(startOutIndex, serializePartitionKey(manifestEntry.file().partition()));

    long version = PathUtils.getQueryParam(manifestEntry.file().path().toString(), FILE_VERSION, 0L, Long::parseLong);
    if (manifestContent == ManifestContent.DATA) {
      outputFileSize.setSafe(startOutIndex, manifestEntry.file().fileSizeInBytes());
      outputColIds.setSafe(startOutIndex, inputColIds.get(0));
      PartitionProtobuf.NormalizedPartitionInfo partitionInfo = getDataFilePartitionInfo(manifestEntry.file(), version);
      outputPartitionInfo.setSafe(startOutIndex, IcebergSerDe.serializeToByteArray(partitionInfo));
    } else {
      // There are earlier checks to block queries if a table has equality deletes, checking here again just to
      // ensure we are only passing through position delete files.
      Preconditions.checkState(manifestEntry.file().content() == FileContent.POSITION_DELETES,
          "Equality deletes are not supported.");
      outputFileType.setSafe(startOutIndex, manifestEntry.file().content().id());
    }

    Map<String, Object> columnStats = getColumnStats(manifestEntry.file(), version);
    for (Field field : outputSchema.getFields()) {
      if (isColumnStatsOutputField(field)) {
        ValueVector vector = columnStatsVectorMap.get(field);
        writeToVector(vector, startOutIndex, columnStats.get(field.getName()));
      }
    }

    doneWithCurrentEntry = true;
    return 1;
  }

  @Override
  public void closeManifestEntry() {
    doneWithCurrentEntry = false;
  }

  @Override
  public void close() throws Exception {
    // Nothing to close
  }

  private boolean shouldProcessCurrentEntry(int maxOutputCount) {
    return !doneWithCurrentEntry && maxOutputCount > 0;
  }

  private PartitionProtobuf.NormalizedPartitionInfo getDataFilePartitionInfo(
      ContentFile<? extends ContentFile<?>> currentFile, long version) {
    PartitionProtobuf.NormalizedPartitionInfo.Builder partitionInfoBuilder = PartitionProtobuf.NormalizedPartitionInfo.newBuilder().setId(String.valueOf(1));

    Schema fileSchema = icebergPartitionSpec.schema();
    StructLike partitionStruct = currentFile.partition();
    for (int partColPos = 0; partColPos < partitionStruct.size(); ++partColPos) {
      PartitionField field = icebergPartitionSpec.fields().get(partColPos);
      /*
       * we can not send partition column value for 1. nonIdentity columns or 2. columns which was not partition but later added as partition columns.
       * because in case 1. information will be partial and scan will get incorrect values
       * in case2. initially when column was not partition we don't have value.
       */
      if (invalidColumnsForPruning != null && invalidColumnsForPruning.contains(fileSchema.findField(field.sourceId()).name())) {
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

  private Map<String, Integer> getColToIDMap() {
    if (colToIDMap == null) {
      Preconditions.checkArgument(inputColIds.getValueCount() > 0);
      IcebergProtobuf.IcebergDatasetXAttr icebergDatasetXAttr;
      try {
        icebergDatasetXAttr = LegacyProtobufSerializer.parseFrom(IcebergProtobuf.IcebergDatasetXAttr.PARSER, inputColIds.get(0));
      } catch (InvalidProtocolBufferException ie) {
        throw new RuntimeException("Could not deserialize Iceberg dataset info", ie);
      } catch (Exception e) {
        throw new RuntimeException("Unable to get colIDMap");
      }
      return icebergDatasetXAttr.getColumnIdsList().stream().collect(Collectors.toMap(c -> c.getSchemaPath().toLowerCase(), c -> c.getId()));
    } else {
      return colToIDMap;
    }
  }

  private void writePartitionValue(PartitionProtobuf.PartitionValue.Builder partitionValueBuilder, Object value, Field field) {
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

  private void addImplicitCols(PartitionProtobuf.NormalizedPartitionInfo.Builder partitionInfoBuilder, long version) {
    PartitionProtobuf.PartitionValue.Builder partitionValueBuilder = PartitionProtobuf.PartitionValue.newBuilder();
    partitionValueBuilder.setColumn(IncrementalUpdateUtils.UPDATE_COLUMN);
    partitionValueBuilder.setLongValue(version);
    partitionInfoBuilder.addValues(partitionValueBuilder.build());
  }

  private Map<String, Object> getColumnStats(ContentFile<? extends ContentFile<?>> currentFile, long version) {
    Map<String, Object> requiredStats = new HashMap<>();

    Schema fileSchema = icebergPartitionSpec.schema();
    for (Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      if (!isColumnStatsOutputField(field)) {
        continue;
      }
      Preconditions.checkArgument(fieldName.length() > 4);
      String colName = fieldName.substring(0, fieldName.length() - 4).toLowerCase();
      String suffix = fieldName.substring(fieldName.length() - 3).toLowerCase();

      if (colName.equals(IncrementalUpdateUtils.UPDATE_COLUMN)) {
        requiredStats.put(fieldName, version);
        continue;
      }

      Preconditions.checkArgument(colToIDMap.containsKey(colName));
      int key = colToIDMap.get(colName);
      Types.NestedField icebergField = fileSchema.findField(key);
      if (icebergField == null) {
        requiredStats.put(fieldName, null);
        continue;
      }
      Type fieldType = icebergField.type();

      Object value;

      switch (suffix) {
        case "min":
          ByteBuffer lowerBound = null;
          if (currentFile.lowerBounds() != null) {
            lowerBound = currentFile.lowerBounds().get(key);
          }
          value = getValueFromByteBuffer(lowerBound, fieldType);
          break;
        case "max":
          ByteBuffer upperBound = null;
          if (currentFile.upperBounds() != null) {
            upperBound = currentFile.upperBounds().get(key);
          }
          value = getValueFromByteBuffer(upperBound, fieldType);
          break;
        case "val":
          Preconditions.checkArgument(partColToKeyMap.containsKey(colName), "partition column not found");
          int partColPos = partColToKeyMap.get(colName);
          value = currentFile.partition().get(partColPos, getPartitionColumnClass(icebergPartitionSpec, partColPos));
          break;
        default:
          throw new RuntimeException("unexpected suffix for column: " + fieldName);
      }
      requiredStats.put(fieldName, value);
    }

    return requiredStats;
  }

  private boolean isColumnStatsOutputField(Field field) {
    return !BASE_OUTPUT_FIELDS.contains(field.getName().toLowerCase());
  }

  private byte[] serializePartitionKey(StructLike partition) throws IOException {
    Object[] vals = new Object[partition.size()];
    for (int i = 0; i < partition.size(); i++) {
      vals[i] = partition.get(i, Object.class);
    }

    return IcebergSerDe.serializeToByteArray(vals);
  }
}
