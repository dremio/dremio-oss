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
package com.dremio.exec.store.dfs;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.deltalake.DeltaConstants;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.protobuf.ByteString;

/**
 * Table function converts input data file path, partition information and file size, and generates a VarBinary, which
 * contains serialised SplitAndPartitionInfo
 */
public class SplitGenTableFunction extends AbstractTableFunction {
  private static final Logger logger = LoggerFactory.getLogger(SplitGenTableFunction.class);

  private final List<String> partitionCols;
  private final Map<String, ValueVector> partitionColValues;
  private final long blockSize;

  private VarCharVector pathVector;
  private BigIntVector sizeVector;
  private BigIntVector modTimeVector;
  private VarBinaryVector outputSplits;

  private String currentPath;
  private long currentModTime;
  private long currentStart = 0L;
  private long fileSize;
  private long remainingSize;
  private int row;

  public SplitGenTableFunction(FragmentExecutionContext fec, OperatorContext context, TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    partitionCols = Optional.ofNullable(functionConfig.getFunctionContext().getPartitionColumns()).orElse(Collections.emptyList());
    partitionColValues = new HashMap<>(partitionCols.size());
    blockSize = context.getOptions().getOption(ExecConstants.PARQUET_BLOCK_SIZE).getNumVal();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);
    pathVector = (VarCharVector) getValueVector(incoming, DeltaConstants.SCHEMA_ADD_PATH);
    sizeVector = (BigIntVector) getValueVector(incoming, DeltaConstants.SCHEMA_ADD_SIZE);
    modTimeVector = (BigIntVector) getValueVector(incoming, DeltaConstants.SCHEMA_ADD_MODIFICATION_TIME);
    outputSplits = (VarBinaryVector) getValueVector(outgoing, RecordReader.SPLIT_INFORMATION);
    partitionCols.forEach(col -> partitionColValues.put(col, getValueVector(incoming, col)));
    return outgoing;
  }

  public static ValueVector getValueVector(VectorAccessible vectorWrappers, String fieldName) {
    TypedFieldId typedFieldId = vectorWrappers.getSchema().getFieldId(SchemaPath.getSimplePath(fieldName));
    Preconditions.checkNotNull(typedFieldId, "Column [%s] not found in incoming vectors.", fieldName);
    Field field = vectorWrappers.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
    return vectorWrappers.getValueAccessorById(TypeHelper.getValueVectorClass(field), typedFieldId.getFieldIds()).getValueVector();
  }

  @Override
  public void startRow(int row) throws Exception {
    final Path currentPathResolved = Path.of(functionConfig.getFunctionContext().getFormatSettings().getLocation())
            .resolve(new String(pathVector.get(row), StandardCharsets.UTF_8));
    currentPath = currentPathResolved.toString();
    currentPath = URLDecoder.decode(currentPath, "UTF-8");
    logger.debug("Reading data file {}", currentPath);
    // Generating splits with 0 as the mtime to signify that these Parquet files are immutable
    currentModTime = 0L;
    currentStart = 0L;
    fileSize = sizeVector.get(row);
    remainingSize = fileSize;
    this.row = row;
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    if (remainingSize <= 0) {
      return 0;
    }
    int splitOffset = 0;
    for (SplitAndPartitionInfo split : createSplits(currentPath, currentModTime, maxRecords)) {
      try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(split);
        outputSplits.setSafe(startOutIndex + (splitOffset++), bos.toByteArray());
      }
    }
    outputSplits.setValueCount(startOutIndex + splitOffset);
    outgoing.setRecordCount(startOutIndex + splitOffset);
    return splitOffset;
  }

  private List<SplitAndPartitionInfo> createSplits(String path, long mtime, int maxRecords) {
    PartitionProtobuf.NormalizedPartitionInfo.Builder partitionInfoBuilder = PartitionProtobuf.NormalizedPartitionInfo
            .newBuilder();
    partitionInfoBuilder.setId(String.valueOf(1));

    for (String partitionCol : partitionCols) {
      PartitionProtobuf.PartitionValue.Builder partitionValueBuilder = PartitionProtobuf.PartitionValue.newBuilder();
      partitionValueBuilder.setColumn(partitionCol);
      setPartitionValue(partitionCol, partitionValueBuilder);
      partitionInfoBuilder.addValues(partitionValueBuilder.build());
    }

    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = partitionInfoBuilder.build();
    final List<SplitAndPartitionInfo> splits = new ArrayList<>();
    while (remainingSize > 0 && splits.size() < maxRecords) {
      long splitSize = Math.min(blockSize, remainingSize);
      final ParquetProtobuf.ParquetBlockBasedSplitXAttr splitExtended =
              ParquetProtobuf.ParquetBlockBasedSplitXAttr.newBuilder()
                      .setStart(this.currentStart)
                      .setLength(splitSize)
                      .setPath(path)
                      .setFileLength(fileSize)
                      .setLastModificationTime(mtime)
                      .build();

      final PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo
              .newBuilder()
              .setPartitionId(partitionInfo.getId())
              .setExtendedProperty(splitExtended.toByteString());

      splits.add(new SplitAndPartitionInfo(partitionInfo, splitInfo.build()));
      remainingSize -= blockSize;
      this.currentStart += splitSize;
    }

    return splits;
  }

  private void setPartitionValue(String partitionColName, PartitionProtobuf.PartitionValue.Builder partitionVal) {
    final ValueVector partitionValVec = partitionColValues.get(partitionColName);

    if (partitionValVec.isNull(row)) {
      return;
    }
    if (partitionValVec instanceof VarCharVector) {
      partitionVal.setStringValue(new String(((VarCharVector) partitionValVec).get(row), StandardCharsets.UTF_8));
    } else if (partitionValVec instanceof IntVector) {
      partitionVal.setIntValue(((IntVector) partitionValVec).get(row));
    } else if (partitionValVec instanceof BigIntVector) {
      partitionVal.setLongValue(((BigIntVector) partitionValVec).getValueAsLong(row));
    } else if (partitionValVec instanceof Float4Vector) {
      partitionVal.setFloatValue(((Float4Vector) partitionValVec).get(row));
    } else if (partitionValVec instanceof Float8Vector) {
      partitionVal.setDoubleValue(((Float8Vector) partitionValVec).get(row));
    } else if (partitionValVec instanceof BitVector) {
      partitionVal.setBitValue(((BitVector) partitionValVec).getObject(row));
    } else if (partitionValVec instanceof TimeStampMilliVector) {
      partitionVal.setLongValue(((TimeStampMilliVector) partitionValVec).get(row));
    } else if (partitionValVec instanceof DateMilliVector) {
      partitionVal.setLongValue(((DateMilliVector) partitionValVec).get(row));
    } else if (partitionValVec instanceof DecimalVector) {
      BigDecimal decimal = ((DecimalVector) partitionValVec).getObject(row);
      partitionVal.setBinaryValue(ByteString.copyFrom(decimal.unscaledValue().toByteArray()));
    } else {
      // TODO: Handle other types
      throw new IllegalArgumentException("Partition column " + partitionColName + "'s type "
              + partitionValVec.getField().getType() + " is unsupported.");
    }
  }

  @Override
  public void closeRow() throws Exception {
  }
}
