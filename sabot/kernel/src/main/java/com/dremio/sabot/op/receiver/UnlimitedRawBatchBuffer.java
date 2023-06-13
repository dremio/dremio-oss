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
package com.dremio.sabot.op.receiver;

import java.util.concurrent.LinkedBlockingDeque;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.options.OptionManager;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.google.common.collect.Queues;

public class UnlimitedRawBatchBuffer extends BaseRawBatchBuffer<RawFragmentBatch> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnlimitedRawBatchBuffer.class);

  private final int fragmentCount;
  private final int softlimit;

  public UnlimitedRawBatchBuffer(SharedResource resource, OptionManager options, FragmentHandle handle, BufferAllocator allocator, int fragmentCount, int oppositeId) {
    super(resource, options, handle, allocator, fragmentCount);
    this.fragmentCount = fragmentCount;
    this.softlimit = bufferSizePerSocket * fragmentCount;
    logger.trace("softLimit: {}", softlimit);
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
      return buffer.isEmpty();
    }

    @Override
    public void add(RawFragmentBatch batch) {
      buffer.add(batch);
    }

    @Override
    public void clear() {
      RawFragmentBatch batch;
      while (!buffer.isEmpty()) {
        batch = buffer.poll();
        if (batch.getBody() != null) {
          batch.getBody().close();
        }
      }
    }
  }

  @Override
  protected void enqueueInner(final RawFragmentBatch batch) {
    if (bufferQueue.size() < softlimit) {
      batch.sendOk();
    }
    bufferQueue.add(batch);
  }

  @Override
  protected void upkeep(RawFragmentBatch batch) {
  }
}
