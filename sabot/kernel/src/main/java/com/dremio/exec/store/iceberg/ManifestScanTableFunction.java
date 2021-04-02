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
import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.protobuf.ByteString;

/**
 * Table function for Iceberg manifest file scan
 */
public class ManifestScanTableFunction extends AbstractTableFunction {
  private VarBinaryVector inputSplits;
  private VarBinaryVector outputSplits;
  private ManifestReader<DataFile> manifestReader;
  private CloseableIterator<DataFile> dataFileCloseableIterator;
  private final FileSystemPlugin<?> plugin;
  private final long targetSplitSize;
  private long currentOffset;
  private DataFile currentDataFile;
  private final List<String> partitionCols;
  private final PartitionSpec icebergPartitionSpec;
  private final BatchSchema outputSchema;
  private final BatchSchema tableSchema;

  private final Map<String, Integer> colNameToKeyMap;
  private final Map<String, Integer> partColToKeyMap;
  private final List<Field> partitionFields;

  public ManifestScanTableFunction(FragmentExecutionContext fragmentExecContext,
                                   OperatorContext context,
                                   TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    try {
      this.plugin = fragmentExecContext.getStoragePlugin(functionConfig.getFunctionContext().getPluginId());
    } catch (ExecutionSetupException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
    targetSplitSize = context.getOptions().getOption(ExecConstants.PARQUET_BLOCK_SIZE).getNumVal();
    partitionCols = functionConfig.getFunctionContext().getPartitionColumns();
    outputSchema = functionConfig.getFunctionContext().getFullSchema();
    tableSchema = functionConfig.getFunctionContext().getTableSchema();

    // todo: Current logic is assuming that the partition spec doesn't change. when we support partition schema evolution,
    // partition spec has to be read from each manifest file and coerce values from datafile entry to
    // target output type of the partition column
    icebergPartitionSpec = IcebergCatalog.getIcebergPartitionSpec(tableSchema, partitionCols);

    colNameToKeyMap = IntStream.range(0, tableSchema.getFields().size())
      .boxed()
      .collect(Collectors.toMap(i -> tableSchema.getFields().get(i).getName().toLowerCase(), i -> i + 1));
    partColToKeyMap = partitionCols != null ? IntStream.range(0, partitionCols.size())
      .boxed()
      .collect(Collectors.toMap(i -> partitionCols.get(i).toLowerCase(), i -> i)) : null;

    Map<String, Field> nameToFieldMap = tableSchema.getFields().stream().collect(Collectors.toMap(f -> f.getName().toLowerCase(), f -> f));
    partitionFields = partitionCols != null ? partitionCols.stream().map(c -> nameToFieldMap.get(c.toLowerCase())).collect(Collectors.toList()) : null;

  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);
    TypedFieldId typedFieldId = incoming.getSchema().getFieldId(SchemaPath.getSimplePath(RecordReader.SPLIT_INFORMATION));
    Field field = incoming.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
    inputSplits = (VarBinaryVector) incoming.getValueAccessorById(TypeHelper.getValueVectorClass(field), typedFieldId.getFieldIds()).getValueVector();

    typedFieldId = outgoing.getSchema().getFieldId(SchemaPath.getSimplePath(RecordReader.SPLIT_INFORMATION));
    field = outgoing.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
    outputSplits = (VarBinaryVector) outgoing.getValueAccessorById(TypeHelper.getValueVectorClass(field), typedFieldId.getFieldIds()).getValueVector();
    return outgoing;
  }

  private void resetCurrentDataFile() {
    currentOffset = 0;
    currentDataFile = null;
  }

  @Override
  public int processRow(int startOutIndex, int maxOutputCount) throws Exception {
    int currentOutputCount = 0;
    while ((currentDataFile != null || dataFileCloseableIterator.hasNext()) && currentOutputCount < maxOutputCount) {
      if (currentDataFile == null) {
        currentDataFile = dataFileCloseableIterator.next();
        if(currentDataFile != null) {
          context.getStats().addLongStat(TableFunctionOperator.Metric.NUM_DATA_FILE, 1);
        }
      }
      List<SplitAndPartitionInfo> splits = getSplitsFromDataFile(currentDataFile, maxOutputCount - currentOutputCount);

      // todo optimize for the case where processRow gets called for same currentDataFile multiple times
      Map<String, Object> partitionAndStatsValues = getRequiredStats(currentDataFile);

      for (SplitAndPartitionInfo split: splits) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
          out.writeObject(split);
          outputSplits.setSafe(startOutIndex + currentOutputCount, bos.toByteArray());

          for (Field field : outputSchema.getFields()) {
            if (field.getName().equals(RecordReader.SPLIT_INFORMATION)) {
              continue;
            }

            ValueVector vector = outgoing.getValueAccessorById(TypeHelper.getValueVectorClass(field),
              outgoing.getSchema().getFieldId(SchemaPath.getSimplePath(field.getName())).getFieldIds()).getValueVector();
            writeToVector(vector, startOutIndex + currentOutputCount, partitionAndStatsValues.get(field.getName()));
          }
          currentOutputCount++;
        }
      }
      if (currentOffset >= currentDataFile.fileSizeInBytes()) {
        resetCurrentDataFile();
      }
    }

    final int totalRecordCount = startOutIndex + currentOutputCount;
    outgoing.forEach(vw -> vw.getValueVector().setValueCount(totalRecordCount));
    outgoing.setRecordCount(totalRecordCount);
    return currentOutputCount;
  }

  @Override
  public void startRow(int row) throws Exception {
    resetCurrentDataFile();
    try (ByteArrayInputStream bis = new ByteArrayInputStream(inputSplits.get(row));
         ObjectInput in = new ObjectInputStream(bis)) {
      ManifestFile manifestFile = (ManifestFile) in.readObject();
      manifestReader = ManifestFiles.read(manifestFile, new HadoopFileIO(plugin.getFsConfCopy()));
      dataFileCloseableIterator = manifestReader.iterator();
    }
  }

  @Override
  public void closeRow() throws Exception {
    resetCurrentDataFile();
    AutoCloseables.close(dataFileCloseableIterator, manifestReader);
    dataFileCloseableIterator = null;
    manifestReader = null;
  }

  private void writePartitionValue(PartitionProtobuf.PartitionValue.Builder partitionValueBuilder, Object value, Field field) {
    if (value == null) {
      return;
    }
    if (value instanceof Long) {
      if (field.getType().equals(CompleteType.TIMESTAMP.getType())) {
        partitionValueBuilder.setLongValue((Long) value / 1_000);
      } else if (field.getType().equals(CompleteType.TIME.getType())) {
        partitionValueBuilder.setIntValue((int)((Long) value / 1_000));
      } else {
        partitionValueBuilder.setLongValue((Long) value);
      }
    } else if (value instanceof Integer) {
      partitionValueBuilder.setIntValue((Integer)value);
    } else if (value instanceof String) {
      partitionValueBuilder.setStringValue((String) value);
    } else if (value instanceof Double) {
      partitionValueBuilder.setDoubleValue((Double)value);
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

  private Class<?> getPartitionColumnClass(int partColPos) {
    return icebergPartitionSpec.javaClasses()[partColPos];
  }

  private List<SplitAndPartitionInfo> getSplitsFromDataFile(DataFile dataFile, int maxOutputCount) {
    PartitionProtobuf.NormalizedPartitionInfo.Builder partitionInfoBuilder = PartitionProtobuf.NormalizedPartitionInfo
            .newBuilder();

    partitionInfoBuilder.setId(String.valueOf(1));

    // get table partition spec
    StructLike partitionStruct = dataFile.partition();
    for (int partColPos = 0; partColPos < partitionStruct.size(); ++ partColPos) {
      PartitionProtobuf.PartitionValue.Builder partitionValueBuilder = PartitionProtobuf.PartitionValue.newBuilder();
      partitionValueBuilder.setColumn(partitionCols.get(partColPos));
      writePartitionValue(partitionValueBuilder, partitionStruct.get(partColPos,
              getPartitionColumnClass(partColPos)), partitionFields.get(partColPos));
      partitionInfoBuilder.addValues(partitionValueBuilder.build());
    }

    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = partitionInfoBuilder.build();
    List<SplitAndPartitionInfo> splits = new ArrayList<>();

    int splitCount = 0;
    while(splitCount < maxOutputCount && currentOffset < dataFile.fileSizeInBytes()) {
      long curBlockSize = Math.min(targetSplitSize, dataFile.fileSizeInBytes() - currentOffset);
      ParquetProtobuf.ParquetBlockBasedSplitXAttr splitExtended =
              ParquetProtobuf.ParquetBlockBasedSplitXAttr.newBuilder()
                      .setPath(dataFile.path().toString())
                      .setStart(currentOffset)
                      .setLength(curBlockSize)
                      .setFileLength(dataFile.fileSizeInBytes())
                      .setLastModificationTime(1) // todo: set correct file modification time
                      .build();
      currentOffset += curBlockSize;

      PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo
              .newBuilder()
              .setPartitionId(partitionInfo.getId())
              .setExtendedProperty(splitExtended.toByteString());

      splits.add(new SplitAndPartitionInfo(partitionInfo, splitInfo.build()));
      splitCount++;
    }

    return splits;
  }

  private Map<String, Object> getRequiredStats(DataFile dataFile) {
    Map<String, Object> requiredStats = new HashMap<>();

    for (Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      if (fieldName.equals(RecordReader.SPLIT_INFORMATION)) {
        continue;
      }
      Preconditions.checkArgument(fieldName.length() > 4);
      String colName = fieldName.substring(0, fieldName.length() - 4).toLowerCase();
      String suffix = fieldName.substring(fieldName.length() - 3).toLowerCase();
      Preconditions.checkArgument(colNameToKeyMap.containsKey(colName), "column not present in table schema");
      int key = colNameToKeyMap.get(colName);
      Object value;

      switch (suffix) {
        case "min":
          ByteBuffer lowerBound = null;
          if (dataFile.lowerBounds() != null) {
            lowerBound = dataFile.lowerBounds().get(key);
          }
          value = getValueFromByteBuffer(lowerBound, field);
          break;
        case "max":
          ByteBuffer upperBound = null;
          if (dataFile.upperBounds() != null) {
            upperBound = dataFile.upperBounds().get(key);
          }
          value = getValueFromByteBuffer(upperBound, field);
          break;
        case "val":
          Preconditions.checkArgument(partColToKeyMap.containsKey(colName), "partition column not found");
          int partColPos = partColToKeyMap.get(colName);
          value = dataFile.partition().get(partColPos, getPartitionColumnClass(partColPos));
          break;
        default:
          throw new RuntimeException("unexpected suffix for column: " + fieldName);
      }
      requiredStats.put(fieldName, value);
    }
    return requiredStats;
  }

  @Override
  public void close() throws Exception {
    super.close();
    AutoCloseables.close(dataFileCloseableIterator, manifestReader);
    dataFileCloseableIterator = null;
    manifestReader = null;
  }
}
