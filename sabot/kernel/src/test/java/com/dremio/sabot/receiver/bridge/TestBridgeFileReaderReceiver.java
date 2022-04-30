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
package com.dremio.sabot.receiver.bridge;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.BridgeFileReaderReceiver;
import com.dremio.exec.physical.config.BridgeFileWriterSender;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Generator;
import com.dremio.sabot.exec.cursors.FileCursorManagerFactory;
import com.dremio.sabot.exec.cursors.FileCursorManagerFactoryImpl;
import com.dremio.sabot.exec.rpc.AccountingFileTunnel;
import com.dremio.sabot.exec.rpc.FileStreamManager;
import com.dremio.sabot.exec.rpc.FileTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.receiver.BatchBufferFromFilesProvider;
import com.dremio.sabot.op.receiver.BridgeFileReaderReceiverOperator;
import com.dremio.sabot.op.receiver.RawFragmentBatchProvider;
import com.dremio.sabot.op.sender.BridgeFileWriterSenderOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceManager;

import io.airlift.tpch.GenerationDefinition;
import io.airlift.tpch.TpchGenerator;

/**
 * Testing BridgeFileReaderReceiver
 */
public class TestBridgeFileReaderReceiver extends BaseTestOperator {
  private final SharedResourceManager resourceManager = SharedResourceManager.newBuilder()
    .addGroup("test")
    .build();

  @Test
  public void simple() throws Exception {
    check(GenerationDefinition.TpchTable.REGION, 0.1, DEFAULT_BATCH, false);
  }

  @Test
  public void multipleBatches() throws Exception {
    check(GenerationDefinition.TpchTable.CUSTOMER, 0.1, DEFAULT_BATCH, false);
  }

  @Test
  public void multipleFiles() throws Exception {
    try (AutoCloseable ac = with(BridgeFileWriterSenderOperator.NUM_BATCHES_PER_FILE, 10)) {
      check(GenerationDefinition.TpchTable.CUSTOMER, 0.1, 100, false);
    }
  }

  @Test
  public void simpleInterleaved() throws Exception {
    check(GenerationDefinition.TpchTable.REGION, 0.1, DEFAULT_BATCH, true);
  }

  @Test
  public void multipleBatchesInterleaved() throws Exception {
    check(GenerationDefinition.TpchTable.CUSTOMER, 0.1, DEFAULT_BATCH, true);
  }

  @Test
  public void multipleFilesInterleaved() throws Exception {
    try (AutoCloseable ac = with(BridgeFileWriterSenderOperator.NUM_BATCHES_PER_FILE, 10)) {
      check(GenerationDefinition.TpchTable.CUSTOMER, 0.1, 100, true);
    }
  }

  private void check(GenerationDefinition.TpchTable table, double scale, int batchSize, boolean interleaved) throws Exception {
    Fixtures.Table expected = TpchGenerator.singleGenerator(table, scale, getAllocator()).toTable(batchSize);
    try (Generator generator = TpchGenerator.singleGenerator(table, scale, getTestAllocator())) {
      check(expected, batchSize, generator, interleaved);
    }
  }

  private void check(Fixtures.Table expected, int batchSize, Generator generator, boolean interleaved) throws Exception {
    FileCursorManagerFactory fileCursorManagerFactory = new FileCursorManagerFactoryImpl();

    final SharedResource sharedResource = mock(SharedResource.class);
    when(sharedResource.isAvailable()).thenReturn(true);

    final TunnelProvider provider = mock(TunnelProvider.class);
    when(provider.getFileTunnel(any(FileStreamManager.class), anyInt())).thenAnswer(
      new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          Object[] args = invocation.getArguments();
          return new AccountingFileTunnel(new FileTunnel((FileStreamManager) args[0], (int) args[1]),
            fileCursorManagerFactory, sharedResource);
        }
      }
    );
    if (interleaved) {
      checkInterleaved(expected, batchSize, generator, provider, fileCursorManagerFactory);
    } else {
      checkSerialized(expected, batchSize, generator, provider, fileCursorManagerFactory);
    }
  }

  private void checkSerialized(Fixtures.Table expected, int batchSize, Generator generator,
                               TunnelProvider provider, FileCursorManagerFactory fileCursorManagerFactory) throws Exception {
    BatchSchema schema = generator.getOutput().getSchema();
    BridgeFileWriterSender sender = new BridgeFileWriterSender(PROPS, schema, null, "set1");
    BridgeFileReaderReceiver receiver = new BridgeFileReaderReceiver(PROPS, schema, 0, "set1");

    List<RecordBatchData> actual = new ArrayList<>();
    try (BridgeFileWriterSenderOperator senderOp = newOperator(BridgeFileWriterSenderOperator.class, sender, batchSize, new EndpointsIndex(), provider)) {
      senderOp.setup(generator.getOutput());
      RawFragmentBatchProvider[] batchBufferProvider = { new BatchBufferFromFilesProvider(senderOp.getSpillManager().getId(), 0,
        resourceManager.getGroup("test"), getTestAllocator(), fileCursorManagerFactory) };
      try (BridgeFileReaderReceiverOperator receiverOp = newOperator(BridgeFileReaderReceiverOperator.class, receiver, batchSize, batchBufferProvider)) {
        int numRecords;
        fileCursorManagerFactory.notifyAllRegistrationsDone();

        // pump sender till it completes.
        while ((numRecords = generator.next(batchSize)) != 0) {
          senderOp.consumeData(numRecords);
        }
        senderOp.noMoreToConsume();

        // pump receiver till it completes.
        VectorAccessible output = receiverOp.setup();
        while (receiverOp.getState() != ProducerOperator.State.DONE) {
          numRecords = receiverOp.outputData();
          if (numRecords != 0) {
            actual.add(new RecordBatchData(output, getTestAllocator()));
          }
        }

        // verify that the batches at the receiver match the generated ones.
        expected.checkValid(actual);
      }
    } finally {
      AutoCloseables.close(actual);
      generator.close();
    }
  }

  private void checkInterleaved(Fixtures.Table expected, int batchSize, Generator generator,
                                TunnelProvider provider, FileCursorManagerFactory fileCursorManagerFactory) throws Exception {
    BatchSchema schema = generator.getOutput().getSchema();
    BridgeFileWriterSender sender = new BridgeFileWriterSender(PROPS, schema, null, "set1");
    BridgeFileReaderReceiver receiver = new BridgeFileReaderReceiver(PROPS, schema, 0, "set1");

    List<RecordBatchData> actual = new ArrayList<>();
    try (BridgeFileWriterSenderOperator senderOp = newOperator(BridgeFileWriterSenderOperator.class, sender, batchSize, new EndpointsIndex(), provider)) {
      senderOp.setup(generator.getOutput());
      RawFragmentBatchProvider[] batchBufferProvider = { new BatchBufferFromFilesProvider(senderOp.getSpillManager().getId(), 0,
        resourceManager.getGroup("test"), getTestAllocator(), fileCursorManagerFactory) };
      try (BridgeFileReaderReceiverOperator receiverOp = newOperator(BridgeFileReaderReceiverOperator.class, receiver, batchSize, batchBufferProvider)) {
        fileCursorManagerFactory.notifyAllRegistrationsDone();
        AtomicBoolean senderDone = new AtomicBoolean(false);
        AtomicBoolean receiverDone = new AtomicBoolean(false);

        Thread senderThread = new Thread(() -> {
          try {
            int numRecords;
            // pump sender till it completes.
            while ((numRecords = generator.next(batchSize)) != 0) {
              senderOp.consumeData(numRecords);
            }
            senderOp.noMoreToConsume();
            senderDone.set(true);

          } catch (Exception ex) {
            Assert.fail("unexpected exception in sender " + ex);
          }
        });

        Thread receiverThread = new Thread(() -> {
          try {
            // pump receiver till it completes.
            VectorAccessible output = receiverOp.setup();
            while (receiverOp.getState() != ProducerOperator.State.DONE) {
              int numRecords = receiverOp.outputData();
              if (numRecords != 0) {
                actual.add(new RecordBatchData(output, getTestAllocator()));
              }
            }
            receiverDone.set(true);
          } catch (Exception ex) {
            Assert.fail("unexpected exception in receiver " + ex);
          }
        });
        senderThread.start();
        receiverThread.start();
        senderThread.join();
        receiverThread.join();

        // verify that the batches at the receiver match the generated ones.
        if (senderDone.get() && receiverDone.get()) {
          expected.checkValid(actual);
        } else {
          Assert.fail(senderDone.get() ? "receiver failed" : "sender failed");
        }
      }
    } finally {
      AutoCloseables.close(actual);
      generator.close();
    }
  }
}
