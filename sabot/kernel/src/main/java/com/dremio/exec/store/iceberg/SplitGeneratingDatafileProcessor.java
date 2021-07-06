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

import static com.dremio.exec.store.iceberg.IcebergUtils.getValueFromByteBuffer;
import static com.dremio.exec.store.iceberg.IcebergUtils.isNonAddOnField;
import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.cache.BlockLocationsCacheManager;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Datafile processor implementation which generates splits from data files
 */
public class SplitGeneratingDatafileProcessor implements DatafileProcessor {

  private final BatchSchema outputSchema;
  private final List<String> partitionCols;
  private final List<Field> partitionFields;
  private final Map<String, Integer> partColToKeyMap;
  private final BlockLocationsCacheManager cacheManager;
  private final BlockBasedSplitGenerator splitGenerator;
  private final HashMap<Field, ValueVector> valueVectorMap = new HashMap<>();

  private Schema fileSchema;
  private long currentDataFileOffset;
  private VarBinaryVector inputColIds;
  private VarBinaryVector outputSplitsIdentity;
  private VarBinaryVector outputColIds;
  private VarBinaryVector outputDataFileSplits;
  private Map<String, Integer> colToIDMap;
  private PartitionSpec icebergPartitionSpec;
  private Map<String, Object> dataFilePartitionAndStats;
  private PartitionProtobuf.BlockLocationsList dataFileBlockLocations;
  private PartitionProtobuf.NormalizedPartitionInfo dataFilePartitionInfo;

  public SplitGeneratingDatafileProcessor(BlockLocationsCacheManager cacheManager, OperatorContext context, TableFunctionContext functionContext, BlockBasedSplitGenerator.SplitCreator splitCreator) {
    this.cacheManager = cacheManager;
    splitGenerator = new BlockBasedSplitGenerator(splitCreator, context);
    partitionCols = functionContext.getPartitionColumns();
    outputSchema = functionContext.getFullSchema();
    partColToKeyMap = partitionCols != null ? IntStream.range(0, partitionCols.size())
      .boxed()
      .collect(Collectors.toMap(i -> partitionCols.get(i).toLowerCase(), i -> i)) : null;

    BatchSchema tableSchema = functionContext.getTableSchema();
    Map<String, Field> nameToFieldMap = tableSchema.getFields().stream().collect(Collectors.toMap(f -> f.getName().toLowerCase(), f -> f));
    partitionFields = partitionCols != null ? partitionCols.stream().map(c -> nameToFieldMap.get(c.toLowerCase())).collect(Collectors.toList()) : null;
  }

  @Override
  public void setup(VectorAccessible incoming, VectorAccessible outgoing) {
    inputColIds = (VarBinaryVector) getVectorFromSchemaPath(incoming, RecordReader.COL_IDS);
    outputSplitsIdentity = (VarBinaryVector) getVectorFromSchemaPath(outgoing, RecordReader.SPLIT_IDENTITY);
    outputColIds = (VarBinaryVector) getVectorFromSchemaPath(outgoing, RecordReader.COL_IDS);
    outputDataFileSplits = (VarBinaryVector) getVectorFromSchemaPath(outgoing, RecordReader.SPLIT_INFORMATION);
    for (Field field : outputSchema.getFields()) {
      if (isNonAddOnField(field.getName())) {
        continue;
      }

      ValueVector vector = outgoing.getValueAccessorById(TypeHelper.getValueVectorClass(field),
        outgoing.getSchema().getFieldId(SchemaPath.getSimplePath(field.getName())).getFieldIds()).getValueVector();
      valueVectorMap.put(field, vector);
    }
  }

  @Override
  public void initialise(PartitionSpec partitionSpec) {
    icebergPartitionSpec = partitionSpec;
    fileSchema = icebergPartitionSpec.schema();
    colToIDMap = getColToIDMap();
  }

  @Override
  public int processDatafile(DataFile currentDataFile, int startOutIndex, int maxOutputCount) throws IOException {
    if (isCurrentDatafileProcessed(currentDataFile)) {
      return 0;
    }

    initialiseDatafileInfo(currentDataFile);

    int currentOutputCount = 0;
    final List<SplitIdentity> splitsIdentity = new ArrayList<>();
    String dataFilePath = Path.getContainerSpecificRelativePath(Path.of(currentDataFile.path().toString()));
    List<SplitAndPartitionInfo> splits = splitGenerator.getSplitAndPartitionInfo(maxOutputCount, dataFilePartitionInfo, dataFilePath,
      currentDataFileOffset, currentDataFile.fileSizeInBytes(), 0, currentDataFile.format().toString(), splitsIdentity, dataFileBlockLocations);
    /* todo: set correct file modification time. Note: Currently Iceberg
            doesn't provide modification time at 'DataFile' level. setting it to 0 avoids
            setting unmodified_type property when making external calls to get objects
     */
    currentDataFileOffset = splitGenerator.getCurrentOffset();
    Preconditions.checkState(splits.size() == splitsIdentity.size(), "Splits count is not same as splits Identity count");
    Iterator<SplitAndPartitionInfo> splitsIterator = splits.iterator();
    Iterator<SplitIdentity> splitIdentityIterator = splitsIdentity.iterator();

    while (splitsIterator.hasNext()) {
      outputSplitsIdentity.setSafe(startOutIndex + currentOutputCount, IcebergSerDe.serializeToByteArray(splitIdentityIterator.next()));
      outputDataFileSplits.setSafe(startOutIndex + currentOutputCount, IcebergSerDe.serializeToByteArray(splitsIterator.next()));
      outputColIds.setSafe(startOutIndex + currentOutputCount, inputColIds.get(0));

      for (Field field : outputSchema.getFields()) {
        if (isNonAddOnField(field.getName())) {
          continue;
        }

        ValueVector vector = valueVectorMap.get(field);
        writeToVector(vector, startOutIndex + currentOutputCount, dataFilePartitionAndStats.get(field.getName()));
      }
      currentOutputCount++;
    }
    return currentOutputCount;
  }

  @Override
  public void closeDatafile() {
    currentDataFileOffset = 0;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(cacheManager);
  }

  @VisibleForTesting
  PartitionProtobuf.NormalizedPartitionInfo getDataFilePartitionInfo(DataFile currentDataFile) {
    PartitionProtobuf.NormalizedPartitionInfo.Builder partitionInfoBuilder = PartitionProtobuf.NormalizedPartitionInfo.newBuilder().setId(String.valueOf(1));

    // get table partition spec
    StructLike partitionStruct = currentDataFile.partition();
    for (int partColPos = 0; partColPos < partitionStruct.size(); ++partColPos) {
      PartitionProtobuf.PartitionValue.Builder partitionValueBuilder = PartitionProtobuf.PartitionValue.newBuilder();
      partitionValueBuilder.setColumn(partitionCols.get(partColPos));
      Object value = partitionStruct.get(partColPos, getPartitionColumnClass(partColPos));
      writePartitionValue(partitionValueBuilder, value, partitionFields.get(partColPos));
      partitionInfoBuilder.addValues(partitionValueBuilder.build());
    }
    return partitionInfoBuilder.build();
  }

  @VisibleForTesting
  Map<String, Object> getDataFileStats(DataFile currentDataFile) {
    Map<String, Object> requiredStats = new HashMap<>();

    for (Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      if (isNonAddOnField(fieldName)) {
        continue;
      }
      Preconditions.checkArgument(fieldName.length() > 4);
      String colName = fieldName.substring(0, fieldName.length() - 4).toLowerCase();
      String suffix = fieldName.substring(fieldName.length() - 3).toLowerCase();
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
          if (currentDataFile.lowerBounds() != null) {
            lowerBound = currentDataFile.lowerBounds().get(key);
          }
          value = getValueFromByteBuffer(lowerBound, fieldType);
          break;
        case "max":
          ByteBuffer upperBound = null;
          if (currentDataFile.upperBounds() != null) {
            upperBound = currentDataFile.upperBounds().get(key);
          }
          value = getValueFromByteBuffer(upperBound, fieldType);
          break;
        case "val":
          Preconditions.checkArgument(partColToKeyMap.containsKey(colName), "partition column not found");
          int partColPos = partColToKeyMap.get(colName);
          value = currentDataFile.partition().get(partColPos, getPartitionColumnClass(partColPos));
          break;
        default:
          throw new RuntimeException("unexpected suffix for column: " + fieldName);
      }
      requiredStats.put(fieldName, value);
    }
    return requiredStats;
  }

  private Class<?> getPartitionColumnClass(int partColPos) {
    return icebergPartitionSpec.javaClasses()[partColPos];
  }

  @VisibleForTesting
  Map<String, Integer> getColToIDMap() {
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
      partitionValueBuilder.setIntValue((Integer) value);
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

  private void initialiseDatafileInfo(DataFile dataFile) {
    if (currentDataFileOffset == 0) {
      dataFilePartitionAndStats = getDataFileStats(dataFile);
      dataFilePartitionInfo = getDataFilePartitionInfo(dataFile);
      if (cacheManager != null) {
        dataFileBlockLocations = cacheManager.createIfAbsent(dataFile.path().toString(), dataFile.fileSizeInBytes());
      }
    }
  }

  private boolean isCurrentDatafileProcessed(DataFile dataFile) {
    return currentDataFileOffset >= dataFile.fileSizeInBytes();
  }
}
