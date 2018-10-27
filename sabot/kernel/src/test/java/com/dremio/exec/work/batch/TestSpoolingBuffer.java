/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.work.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.config.SabotConfig;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecTest;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentRecordBatch;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.sabot.exec.fragment.FragmentWorkQueue;
import com.dremio.sabot.exec.rpc.AckSender;
import com.dremio.sabot.op.receiver.RawFragmentBatch;
import com.dremio.sabot.op.receiver.SpoolingRawBatchBuffer;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.spill.DefaultSpillServiceOptions;
import com.dremio.service.spill.SpillService;
import com.dremio.service.spill.SpillServiceImpl;

import io.netty.buffer.ArrowBuf;

public class TestSpoolingBuffer extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSpoolingBuffer.class);

  @Test
  public void test() throws Exception {
    SharedResource resource = mock(SharedResource.class);
    QueryId queryId = ExternalIdHelper.toQueryId(ExternalIdHelper.generateExternalId());
    FragmentHandle handle = FragmentHandle.newBuilder().setMajorFragmentId(0).setMinorFragmentId(0).setQueryId(queryId).build();
    FragmentWorkQueue queue = mock(FragmentWorkQueue.class);
    final ExecutorService executorService = Executors.newFixedThreadPool(1);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        Runnable work = invocationOnMock.getArgumentAt(0, Runnable.class);
        executorService.submit(work);
        return null;
      }
    }).when(queue).put(any(Runnable.class));

    SabotConfig config = SabotConfig.create();
    final SchedulerService schedulerService = mock(SchedulerService.class);
    final SpillService spillService = new SpillServiceImpl(DremioConfig.create(null, config), new DefaultSpillServiceOptions(),
    new Provider<SchedulerService>() {
      @Override
      public SchedulerService get() {
        return schedulerService;
      }
    });

    try (BufferAllocator spoolingAllocator = new RootAllocator(Long.MAX_VALUE);
      SpoolingRawBatchBuffer buffer = new SpoolingRawBatchBuffer(resource, config, queue, handle, spillService, spoolingAllocator, 1, 0, 0)) {

      for (int i = 0; i < 100; i++) {
        try (RawFragmentBatch batch = newBatch(i)) {
          buffer.enqueue(batch);
        }
      }

      executorService.shutdown();
      if (!executorService.awaitTermination(45, TimeUnit.SECONDS)) {
        Assert.fail("Timed out while waiting for executor termination");
      }

      // checks that the batches have been written to disk and are no longer in memory
      assertEquals(6 * 1024, allocator.getAllocatedMemory());

      for (int i = 0; i < 100; i++) {
        RawFragmentBatch batch = buffer.getNext();
        assertEquals(1024, batch.getBody().capacity());
        assertEquals(i, batch.getBody().getInt(0));
        batch.close();
      }

      assertNull(buffer.getNext());
    }

  }

  private AckSender ackSender = mock(AckSender.class);

  private RawFragmentBatch newBatch(int index) {
    ArrowBuf buffer = allocator.buffer(1024);
    buffer.setInt(0, index);
    return new RawFragmentBatch(FragmentRecordBatch.getDefaultInstance(), buffer, ackSender);
  }
}
