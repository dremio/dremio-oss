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

import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.options.OptionManager;
import com.dremio.sabot.threads.sharedres.SharedResource;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.BufferAllocator;

public abstract class BaseRawBatchBuffer<T> implements RawBatchBuffer {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BaseRawBatchBuffer.class);

  private enum BufferState {
    RUN,
    CLOSED
  }

  protected interface BufferQueue<T> {
    public RawFragmentBatch poll();

    public int size();

    public boolean isEmpty();

    public void add(T obj);

    public void clear();
  }

  private AtomicLong queueMonitor = new AtomicLong(0);
  protected final OptionManager options;
  protected final BufferAllocator allocator;
  protected final FragmentHandle handle;
  protected BufferQueue<T> bufferQueue;
  private volatile BufferState state = BufferState.RUN;
  protected final int bufferSizePerSocket;
  private AtomicLong remainingStreams;
  private final int fragmentCount;
  private final SharedResource resource;

  public BaseRawBatchBuffer(
      SharedResource resource,
      OptionManager options,
      FragmentHandle handle,
      BufferAllocator allocator,
      final int fragmentCount) {
    bufferSizePerSocket = (int) options.getOption(ExecConstants.INCOMING_BUFFER_SIZE);
    this.options = options;
    this.fragmentCount = fragmentCount;
    this.remainingStreams = new AtomicLong(fragmentCount);
    this.allocator = allocator;
    this.handle = handle;
    this.resource = resource;
  }

  /**
   * Return the fragment count from construction time.
   *
   * @return the fragment count
   */
  protected int getFragmentCount() {
    return fragmentCount;
  }

  @Override
  public void streamComplete() {
    synchronized (resource) {
      decrementStreamCounter();
      resource.markAvailable();
    }
  }

  @Override
  public void enqueue(final RawFragmentBatch batch) {

    synchronized (resource) {
      if (state == BufferState.CLOSED) {
        // do not even enqueue just release and send ack back
        batch.close();
        batch.sendOk();
        return;
      }

      enqueueInner(batch);
      if (queueMonitor.incrementAndGet() == 1) {
        resource.markAvailable();
      }
    }
  }

  /**
   * implementation specific method to enqueue batch
   *
   * @param batch
   * @throws IOException
   */
  protected abstract void enqueueInner(final RawFragmentBatch batch);

  //  ## Add assertion that all acks have been sent. TODO
  @Override
  public void close() throws Exception {
    if (!bufferQueue.isEmpty()) {
      clearBufferWithBody();
    }
    resource.markAvailable();
  }

  /**
   * Helper method to clear buffer with request bodies release also flushes ack queue - in case
   * there are still responses pending
   */
  private void clearBufferWithBody() {
    while (!bufferQueue.isEmpty()) {
      RawFragmentBatch batch = bufferQueue.poll();
      assertAckSent(batch);
      if (batch.getBody() != null) {
        batch.getBody().close();
      }
    }
  }

  @Override
  public synchronized RawFragmentBatch getNext() {
    RawFragmentBatch b;

    synchronized (resource) {
      while ((b = bufferQueue.poll()) == null) {
        // keep polling as long as the queueMonitor states we have messages as there is a race
        // condition between poll() and get().
        if (queueMonitor.get() == 0) {
          // poll didn't return anything and the queue monitor states we have no messages.

          if (state == BufferState.RUN) {
            // if we're still running, mark this resource as unavailable.
            resource.markBlocked();
          }

          return null;
        }
      }

      // we actually removed an item.
      queueMonitor.decrementAndGet();
      upkeep(b);

      assertAckSent(b);
      return b;
    }
  }

  @Override
  public boolean isStreamDone() {
    return remainingStreams.get() == 0;
  }

  private void assertAckSent(RawFragmentBatch batch) {
    assert batch == null || batch.isAckSent() : "Ack not sent for batch";
  }

  // note this is called under synchronized.
  private void decrementStreamCounter() {
    if (remainingStreams.decrementAndGet() == 0) {
      logger.debug("Streams finished");
      resource.markAvailable();
      state = BufferState.CLOSED;
    }
  }

  /** Handle miscellaneous tasks after batch retrieval */
  protected abstract void upkeep(RawFragmentBatch batch);
}
