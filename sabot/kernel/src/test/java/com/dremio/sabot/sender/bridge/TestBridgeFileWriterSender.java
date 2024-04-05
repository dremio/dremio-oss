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
package com.dremio.sabot.sender.bridge;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.cache.VectorAccessibleSerializable;
import com.dremio.exec.physical.config.BridgeFileWriterSender;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.FileExec;
import com.dremio.exec.record.ArrowRecordBatchLoader;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Generator;
import com.dremio.sabot.exec.cursors.FileCursorManagerFactory;
import com.dremio.sabot.exec.cursors.FileCursorManagerFactoryImpl;
import com.dremio.sabot.exec.rpc.AccountingFileTunnel;
import com.dremio.sabot.exec.rpc.FileStreamManager;
import com.dremio.sabot.exec.rpc.FileTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sender.BridgeFileWriterSenderOperator;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.dremio.sabot.threads.sharedres.SharedResource;
import io.airlift.tpch.GenerationDefinition;
import io.airlift.tpch.TpchGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Testing BridgeFileWriterSender */
public class TestBridgeFileWriterSender extends BaseTestOperator {
  @Test
  public void simple() throws Exception {
    check(GenerationDefinition.TpchTable.REGION, 0.1, DEFAULT_BATCH);
  }

  @Test
  public void multipleBatches() throws Exception {
    check(GenerationDefinition.TpchTable.CUSTOMER, 0.1, DEFAULT_BATCH);
  }

  @Test
  public void multipleFiles() throws Exception {
    try (AutoCloseable ac = with(BridgeFileWriterSenderOperator.NUM_BATCHES_PER_FILE, 10)) {
      check(GenerationDefinition.TpchTable.CUSTOMER, 0.1, 100);
    }
  }

  private void check(GenerationDefinition.TpchTable table, double scale, int batchSize)
      throws Exception {
    Fixtures.Table expected =
        TpchGenerator.singleGenerator(table, scale, getAllocator()).toTable(batchSize);
    try (Generator generator = TpchGenerator.singleGenerator(table, scale, getTestAllocator())) {
      check(expected, batchSize, generator);
    }
  }

  private void check(Fixtures.Table expected, int batchSize, Generator generator) throws Exception {
    BatchSchema schema = generator.getOutput().getSchema();
    BridgeFileWriterSender sender = new BridgeFileWriterSender(PROPS, schema, null, "set1");
    FileCursorManagerFactory fileCursorManagerFactory = new FileCursorManagerFactoryImpl();

    final SharedResource sharedResource = mock(SharedResource.class);
    when(sharedResource.isAvailable()).thenReturn(true);

    final TunnelProvider provider = mock(TunnelProvider.class);
    when(provider.getFileTunnel(any(FileStreamManager.class), anyInt()))
        .thenAnswer(
            new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                return new AccountingFileTunnel(
                    new FileTunnel((FileStreamManager) args[0], (int) args[1]),
                    fileCursorManagerFactory,
                    sharedResource);
              }
            });

    List<RecordBatchData> actual = null;
    try (BridgeFileWriterSenderOperator op =
        newOperator(
            BridgeFileWriterSenderOperator.class,
            sender,
            batchSize,
            new EndpointsIndex(),
            provider)) {
      int numRecords;
      op.setup(generator.getOutput());
      while ((numRecords = generator.next(batchSize)) != 0) {
        op.consumeData(numRecords);
      }
      op.noMoreToConsume();

      // read back batches from the file
      actual = readAllDataFromDir(op.getSpillManager(), schema);

      // verify that the read batches match the generated ones.
      expected.checkValid(actual);
      fileCursorManagerFactory.notifyAllRegistrationsDone();
    } finally {
      AutoCloseables.close(actual);
      generator.close();
    }
  }

  List<RecordBatchData> readAllDataFromDir(SpillManager spillManager, BatchSchema schema)
      throws IOException {
    List<RecordBatchData> allBatches = new ArrayList<>();

    // iterate over all files in the directory till end-of-stream marker.
    int currentIdx = 0;
    while (true) {
      String fileName = String.format("%08d.arrow", currentIdx);
      try (FSDataInputStream stream = spillManager.getSpillFile(fileName).open()) {
        FileReadResult readResult = readAllDataFromFile(stream, schema);
        allBatches.addAll(readResult.records);
        if (readResult.isStreamClosed) {
          break;
        }
      }
      ++currentIdx;
    }
    return allBatches;
  }

  private static class FileReadResult {
    List<RecordBatchData> records;
    boolean isStreamClosed;

    FileReadResult(List<RecordBatchData> records, boolean isStreamClosed) {
      this.records = records;
      this.isStreamClosed = isStreamClosed;
    }
  }
  ;

  FileReadResult readAllDataFromFile(FSDataInputStream input, BatchSchema schema)
      throws IOException {
    List<RecordBatchData> fileBatches = new ArrayList<>();
    boolean isStreamClosed = false;

    while (true) {
      FileExec.FileMessage fileMessage = FileExec.FileMessage.parseDelimitedFrom(input);
      if (fileMessage.hasStreamComplete()) {
        isStreamClosed = true;
        break;
      }
      if (!fileMessage.hasType()) {
        // indicates end-of-file
        break;
      }

      long bodySize = fileMessage.getBodySize();
      try (ArrowBuf body = getTestAllocator().buffer(bodySize)) {
        // read the body
        VectorAccessibleSerializable.readIntoArrowBuf(input, body, bodySize);

        // load container
        VectorContainer container = VectorContainer.create(getTestAllocator(), schema);
        ArrowRecordBatchLoader batchLoader = new ArrowRecordBatchLoader(container);
        batchLoader.load(fileMessage.getRecordBatch(), body);

        // add to list of record batches
        fileBatches.add(new RecordBatchData(container, getTestAllocator()));
      }
    }
    return new FileReadResult(fileBatches, isStreamClosed);
  }
}
