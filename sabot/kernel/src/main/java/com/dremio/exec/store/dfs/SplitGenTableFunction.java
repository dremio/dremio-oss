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

import static com.dremio.exec.store.deltalake.DeltaConstants.PARTITION_NAME_SUFFIX;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.memory.ArrowBuf;
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
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.deltalake.DeltaConstants;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.util.VectorUtil;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.base.Preconditions;
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
  private StructVector outputSplitsIdentity;
  private VarBinaryVector outputSplits;

  private String currentPath;
  private long currentModTime;
  private long currentStart = 0L;
  private long fileSize;
  private long remainingSize;
  private int row;
  private ArrowBuf tmpBuf;

  public SplitGenTableFunction(FragmentExecutionContext fec, OperatorContext context, TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    partitionCols = Optional.ofNullable(functionConfig.getFunctionContext().getPartitionColumns()).orElse(Collections.emptyList());
    partitionColValues = new HashMap<>(partitionCols.size());
    blockSize = context.getOptions().getOption(ExecConstants.PARQUET_SPLIT_SIZE).getNumVal();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);
    pathVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(incoming, DeltaConstants.SCHEMA_ADD_PATH);
    sizeVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(incoming, DeltaConstants.SCHEMA_ADD_SIZE);
    modTimeVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(incoming, DeltaConstants.SCHEMA_ADD_MODIFICATION_TIME);
    outputSplits = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, RecordReader.SPLIT_INFORMATION);
    outputSplitsIdentity = (StructVector) VectorUtil.getVectorFromSchemaPath(outgoing, RecordReader.SPLIT_IDENTITY);
    partitionCols.forEach(col -> partitionColValues.put(col, VectorUtil.getVectorFromSchemaPath(incoming, col + PARTITION_NAME_SUFFIX)));
    tmpBuf = context.getAllocator().buffer(4096);
    return outgoing;
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
    List<SplitIdentity> splitsIdentity = new ArrayList<>();
    List<SplitAndPartitionInfo> splits = createSplits(currentPath, currentModTime, maxRecords, splitsIdentity);
    Preconditions.checkState(splits.size() == splitsIdentity.size(), "Splits count is not same as splits Identity count");
    Iterator<SplitAndPartitionInfo> splitsIterator = splits.iterator();
    Iterator<SplitIdentity> splitsIdentityIterator = splitsIdentity.iterator();

    NullableStructWriter splitIdentityWriter = outputSplitsIdentity.getWriter();

    while (splitsIterator.hasNext()) {
      IcebergUtils.writeSplitIdentity(splitIdentityWriter, startOutIndex + splitOffset, splitsIdentityIterator.next(), tmpBuf);
      outputSplits.setSafe(startOutIndex + splitOffset, serializeToByteArray(splitsIterator.next()));
      splitOffset++;
    }
    int recordCount = startOutIndex + splitOffset;
    outgoing.forEach(vw -> vw.getValueVector().setValueCount(recordCount));
    outgoing.setRecordCount(recordCount);
    return splitOffset;
  }

  private List<SplitAndPartitionInfo> createSplits(String path, long mtime, int maxRecords, List<SplitIdentity> splitsIdentity) {
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

      splitsIdentity.add(new SplitIdentity(splitExtended.getPath(), splitExtended.getStart(), splitExtended.getLength(), fileSize));

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

  private static byte[] serializeToByteArray(Object object) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(object);
      return bos.toByteArray();
    }
  }

  @Override
  public void closeRow() throws Exception {
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(super::close, tmpBuf);
  }
}
