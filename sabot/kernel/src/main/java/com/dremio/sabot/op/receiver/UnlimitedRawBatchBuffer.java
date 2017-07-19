/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.receiver;

import java.util.concurrent.LinkedBlockingDeque;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.google.common.collect.Queues;

public class UnlimitedRawBatchBuffer extends BaseRawBatchBuffer<RawFragmentBatch> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnlimitedRawBatchBuffer.class);

  private final int softlimit;
  private final int startlimit;

  public UnlimitedRawBatchBuffer(SharedResource resource, SabotConfig config, FragmentHandle handle, BufferAllocator allocator, int fragmentCount, int oppositeId) {
    super(resource, config, handle, allocator, fragmentCount);
    this.softlimit = bufferSizePerSocket * fragmentCount;
    this.startlimit = Math.max(softlimit/2, 1);
    logger.trace("softLimit: {}, startLimit: {}", softlimit, startlimit);
    this.bufferQueue = new UnlimitedBufferQueue();
  }

  private class UnlimitedBufferQueue implements BufferQueue<RawFragmentBatch> {
    private final LinkedBlockingDeque<RawFragmentBatch> buffer = Queues.newLinkedBlockingDeque();;

    @Override
    public RawFragmentBatch poll() {
      RawFragmentBatch batch = buffer.poll();
      if (batch != null) {
        batch.sendOk();
      }
      return batch;
    }

    @Override
    public int size() {
      return buffer.size();
    }

    @Override
    public boolean isEmpty() {
      return buffer.size() == 0;
    }

    @Override
    public void add(RawFragmentBatch batch) {
      buffer.add(batch);
    }
  }

  protected void enqueueInner(final RawFragmentBatch batch) {
    if (bufferQueue.size() < softlimit) {
      batch.sendOk();
    }
    bufferQueue.add(batch);
  }

  protected void upkeep(RawFragmentBatch batch) {
  }
}
