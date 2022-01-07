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

import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.DataProcessorType;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.extractSplit;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.getBatchSchema;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.getDatafile;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.getSplitVec;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.getTableFunctionContext;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.DataFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.parquet.ParquetSplitCreator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.google.common.collect.ImmutableList;

public class TestSplitGeneratingDatafileProcessor extends BaseTestQuery {
  private final int startIndex = 0;
  private final int maxOutputCount = 5;

  private long blockSize;
  private VectorContainer incoming;
  private VectorContainer outgoing;
  private VarBinaryVector splitVector;
  private SplitGeneratingDatafileProcessor dataFileProcessor;

  @Before
  public void initialiseSplitGenDatafileProcessor() throws Exception {
    OperatorContext operatorContext = getOperatorContext();
    blockSize = operatorContext.getOptions().getOption(ExecConstants.PARQUET_SPLIT_SIZE).getNumVal();
    TableFunctionContext tableFunctionContext = getTableFunctionContext(DataProcessorType.SPLIT_GEN);
    FileSystemPlugin plugin = mock(FileSystemPlugin.class);
    when(plugin.createSplitCreator(operatorContext,null, false)).thenReturn(new ParquetSplitCreator(operatorContext, true));
    dataFileProcessor = new SplitGeneratingDatafileProcessor(operatorContext, plugin, null, tableFunctionContext);
    dataFileProcessor = spy(dataFileProcessor);
    incoming = buildIncomingVector();
    outgoing = getOperatorContext().createOutputVectorContainer();
    outgoing.addSchema(getBatchSchema(DataProcessorType.SPLIT_GEN));
    outgoing.buildSchema();
    dataFileProcessor.setup(incoming, outgoing);
    splitVector = getSplitVec(outgoing);
  }

  @After
  public void close() throws Exception {
    AutoCloseables.close(incoming, outgoing, dataFileProcessor);
  }

  @Test
  public void testWithNoSplits() throws Exception {
    DataFile df1 = getDatafile("/path/to/data-1.parquet", blockSize - 1024);
    int splitGenerated, totalSplitGenerated = 0;
    splitGenerated = dataFileProcessor.processDatafile(df1, startIndex + totalSplitGenerated, maxOutputCount - totalSplitGenerated);
    totalSplitGenerated += splitGenerated;
    assertEquals(splitGenerated, 1);
    assertEquals(totalSplitGenerated, 1);
    dataFileProcessor.closeDatafile();

    SplitAndPartitionInfo splitAndPartitionInfo = extractSplit(splitVector, 0);
    ParquetProtobuf.ParquetBlockBasedSplitXAttr xAttr = ParquetProtobuf.ParquetBlockBasedSplitXAttr
      .parseFrom(splitAndPartitionInfo.getDatasetSplitInfo().getExtendedProperty());
    assertEquals(blockSize - 1024, xAttr.getFileLength());
  }

  @Test
  public void testWithFileSizeMoreThanSplitSize() throws Exception {
    DataFile df1 = getDatafile("/path/to/data-1.parquet", blockSize * 2 + 1024);
    int splitGenerated, totalSplitGenerated = 0;
    splitGenerated = dataFileProcessor.processDatafile(df1, startIndex + totalSplitGenerated, maxOutputCount - totalSplitGenerated);
    totalSplitGenerated += splitGenerated;
    assertEquals(splitGenerated, 3);

    splitGenerated = dataFileProcessor.processDatafile(df1, startIndex + totalSplitGenerated, maxOutputCount - totalSplitGenerated);
    totalSplitGenerated += splitGenerated;
    assertEquals(splitGenerated, 0);
    dataFileProcessor.closeDatafile();

    DataFile df2 = getDatafile("/path/to/data-2.parquet", blockSize * 1 + 1024);
    splitGenerated = dataFileProcessor.processDatafile(df2, startIndex + totalSplitGenerated, maxOutputCount - totalSplitGenerated);
    totalSplitGenerated += splitGenerated;
    assertEquals(splitGenerated, 2);

    splitGenerated = dataFileProcessor.processDatafile(df2, startIndex + totalSplitGenerated, maxOutputCount - totalSplitGenerated);
    totalSplitGenerated += splitGenerated;
    assertEquals(splitGenerated, 0);
    dataFileProcessor.closeDatafile();

    DataFile df3 = getDatafile("/path/to/data-3.parquet", blockSize * 1 + 1024);
    splitGenerated = dataFileProcessor.processDatafile(df3, startIndex + totalSplitGenerated, maxOutputCount - totalSplitGenerated);
    totalSplitGenerated += splitGenerated;
    assertEquals(splitGenerated, 0);
    dataFileProcessor.closeDatafile();

    //Test whether split no. 3 at index 2 is from data-1.parquet and its size is 1024
    // because sizeOf(Data-1.parquet)/blocksize = 1024
    SplitAndPartitionInfo splitAndPartitionInfo = extractSplit(splitVector, 2);
    ParquetProtobuf.ParquetBlockBasedSplitXAttr xAttr = ParquetProtobuf.ParquetBlockBasedSplitXAttr
      .parseFrom(splitAndPartitionInfo.getDatasetSplitInfo().getExtendedProperty());
    assertEquals("/path/to/data-1.parquet", xAttr.getPath());
    assertEquals(1024, xAttr.getLength());
    assertEquals(totalSplitGenerated, 5);
  }

  private OperatorContext getOperatorContext() {
    SabotContext sabotContext = getSabotContext();
    return new OperatorContextImpl(sabotContext.getConfig(), sabotContext.getDremioConfig(), getAllocator(), sabotContext.getOptionManager(), 10, sabotContext.getExpressionSplitCache());
  }

  private VectorContainer buildIncomingVector() {
    VarBinaryVector manifestVector = new VarBinaryVector(RecordReader.SPLIT_INFORMATION, allocator);
    VarBinaryVector colIdVector = new VarBinaryVector(RecordReader.COL_IDS, allocator);
    VectorContainer incoming = new VectorContainer();
    List<ValueVector> incomingVectors = ImmutableList.of(manifestVector, colIdVector);
    incoming.addCollection(incomingVectors);
    incomingVectors.stream().forEach(ValueVector::allocateNew);
    AtomicInteger counter = new AtomicInteger(0);
    BiConsumer<String, String> incomingRow = (ManifestFile, DummyColIDByte) -> {
      int idx = counter.getAndIncrement();
      manifestVector.set(idx, ManifestFile.getBytes(StandardCharsets.UTF_8));
      if (DummyColIDByte != null) {
        colIdVector.set(idx, DummyColIDByte.getBytes(StandardCharsets.UTF_8));
      }
    };
    incomingRow.accept("mfile1.avro", "coldID");
    incomingRow.accept("mfile2.avro", null);
    incomingRow.accept("mfile3.avro", null);
    incoming.setAllCount(3);
    incoming.buildSchema();
    return incoming;
  }
}
