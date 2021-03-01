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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

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
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;

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

    // todo: Current logic is assuming that the partition spec doesn't change. when we support partition schema evolution,
    // partition spec has to be read from each manifest file and coerce values from datafile entry to
    // target output type of the partition column
    icebergPartitionSpec = IcebergCatalog.getIcebergPartitionSpec(functionConfig.getFunctionContext().getTableSchema(), partitionCols);
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
      }
      List<SplitAndPartitionInfo> splits = getSplitsFromDataFile(currentDataFile, maxOutputCount - currentOutputCount);
      for (SplitAndPartitionInfo split: splits) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
          out.writeObject(split);
          outputSplits.setSafe(startOutIndex + currentOutputCount, bos.toByteArray());
          currentOutputCount++;
        }
      }
      if (currentOffset >= currentDataFile.fileSizeInBytes()) {
        resetCurrentDataFile();
      }
    }
    outputSplits.setValueCount(startOutIndex + currentOutputCount);
    outgoing.setRecordCount(startOutIndex + currentOutputCount);
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

  private void writePartitionValue(PartitionProtobuf.PartitionValue.Builder partitionValueBuilder, Object value) {
    if (value == null) {
      return;
    }
    if (value instanceof Long) {
      partitionValueBuilder.setLongValue((Long) value);
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
              getPartitionColumnClass(partColPos)));
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

  @Override
  public void close() throws Exception {
    super.close();
    AutoCloseables.close(dataFileCloseableIterator, manifestReader);
    dataFileCloseableIterator = null;
    manifestReader = null;
  }
}
