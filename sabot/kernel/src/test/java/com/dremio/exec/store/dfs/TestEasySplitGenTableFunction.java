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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.SplitProducerTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.Test;

/** Tests for {@link SplitGenTableFunction} */
public class TestEasySplitGenTableFunction extends BaseTestQuery {
  private static final String FILE_1 = "file1.csv";
  private static final String FILE_2 = "file2.csv";
  private static final String FILE_3 = "file3.csv";
  private static final long FILE_1_LENGTH = 1000;
  private static final long FILE_2_LENGTH = 1032;
  private static final long FILE_3_LENGTH = 211L;

  private interface RowHandler {
    void accept(String path, long size, long mtime);
  }

  @Test
  public void testNoPartitionSplits() throws Exception {
    try (VarCharVector pathVector =
            new VarCharVector(
                MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH, allocator);
        BigIntVector sizeVector =
            new BigIntVector(
                MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_SIZE, allocator);
        BigIntVector mtimeVector =
            new BigIntVector(
                MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.MODIFICATION_TIME, allocator);
        AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable();
        VectorContainer incoming = new VectorContainer()) {
      List<ValueVector> incomingVectors = ImmutableList.of(pathVector, sizeVector, mtimeVector);
      incoming.addCollection(incomingVectors);
      incomingVectors.forEach(ValueVector::allocateNew);
      AtomicInteger counter = new AtomicInteger(0);
      RowHandler incomingRow =
          (path, size, mtime) -> {
            int idx = counter.getAndIncrement();
            pathVector.set(idx, path.getBytes(StandardCharsets.UTF_8));
            sizeVector.set(idx, size);
            mtimeVector.set(idx, mtime);
          };
      EasySplitGenTableFunction tableFunction =
          new EasySplitGenTableFunction(null, getOpCtx(), getConfig());
      long currentTime = System.currentTimeMillis();
      incomingRow.accept(FILE_1, FILE_1_LENGTH, currentTime);
      incomingRow.accept(FILE_2, FILE_2_LENGTH, currentTime);
      incomingRow.accept(FILE_3, FILE_3_LENGTH, currentTime);
      incoming.setAllCount(3);
      incoming.buildSchema();

      VectorAccessible outgoing = tableFunction.setup(incoming);
      closer.addAll(outgoing);

      VarBinaryVector outgoingSplits =
          (VarBinaryVector)
              VectorUtil.getVectorFromSchemaPath(outgoing, RecordReader.SPLIT_INFORMATION);

      tableFunction.startRow(0);
      assertThat(tableFunction.processRow(0, 2)).isEqualTo(1);
      assertThat(outgoingSplits.getValueCount()).isEqualTo(1);
      assertSplit(extractSplit(outgoingSplits, 0), FILE_1, 0L, FILE_1_LENGTH);
      tableFunction.startRow(1);
      assertThat(tableFunction.processRow(1, 5)).isEqualTo(1);
      assertThat(tableFunction.processRow(2, 2)).isEqualTo(0);
      assertThat(outgoingSplits.getValueCount()).isEqualTo(2);
      assertSplit(extractSplit(outgoingSplits, 1), FILE_2, 0L, FILE_2_LENGTH);
      tableFunction.close();
    }
  }

  @Test
  public void testMultiPartitionsPerSplit() {}

  private void assertSplit(SplitAndPartitionInfo split, String path, long start, long fileSize)
      throws InvalidProtocolBufferException {
    EasyProtobuf.EasyDatasetSplitXAttr xAttr = getXAttr(split);
    assertThat(xAttr.getPath()).isEqualTo(path);
    assertThat(xAttr.getStart()).isEqualTo(start);
    assertThat(xAttr.getLength()).isEqualTo(fileSize);
  }

  private EasyProtobuf.EasyDatasetSplitXAttr getXAttr(SplitAndPartitionInfo split)
      throws InvalidProtocolBufferException {
    return EasyProtobuf.EasyDatasetSplitXAttr.parseFrom(
        split.getDatasetSplitInfo().getExtendedProperty());
  }

  private SplitAndPartitionInfo extractSplit(VarBinaryVector splits, int idx)
      throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(splits.get(idx));
        ObjectInput in = new ObjectInputStream(bis)) {
      return (SplitAndPartitionInfo) in.readObject();
    }
  }

  private TableFunctionConfig getConfig() {
    SplitProducerTableFunctionContext functionContext =
        mock(SplitProducerTableFunctionContext.class);
    when(functionContext.getFullSchema())
        .thenReturn(RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA);
    List<SchemaPath> expectedColumns =
        Arrays.asList(
            SchemaPath.getSimplePath(RecordReader.SPLIT_IDENTITY),
            SchemaPath.getSimplePath(RecordReader.SPLIT_INFORMATION),
            SchemaPath.getSimplePath(RecordReader.COL_IDS));
    when(functionContext.getColumns()).thenReturn(expectedColumns);
    FileConfig fc = new FileConfig();
    fc.setLocation("/test");
    when(functionContext.getFormatSettings()).thenReturn(fc);

    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.SPLIT_GENERATION, false, functionContext);
  }

  private OperatorContext getOpCtx() {
    SabotContext sabotContext = getSabotContext();
    return new OperatorContextImpl(
        sabotContext.getConfig(),
        sabotContext.getDremioConfig(),
        getTestAllocator(),
        sabotContext.getOptionManager(),
        10,
        sabotContext.getExpressionSplitCache());
  }
}
