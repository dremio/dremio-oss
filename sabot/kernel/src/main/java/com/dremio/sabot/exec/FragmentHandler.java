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
package com.dremio.sabot.exec;

import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.dremio.sabot.exec.rpc.IncomingDataBatch;
import com.google.common.base.Preconditions;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A FragmentHandler ensures proper handling of cancellation/early termination messages even when their corresponding
 * FragmentExecutor didn't start yet. Implementation must handle concurrent accesses from both the FragmentExecutor and
 * the fabric thread reporting the messages.<br>
 * If the corresponding fragment doesn't start after the eviction delay, handler will be marked as expired and removed
 * from the cache. Also once a fragment is done and after the eviction delay the handler will also be marked as expired.
 */
public class FragmentHandler implements EventProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentHandler.class);

  private class FragmentEvent {
    final FragmentHandle handle;
    final long time;

    FragmentEvent(FragmentHandle handle) {
      this.handle = handle;
      this.time = System.currentTimeMillis();
    }
  }

  private final FragmentHandle handle;
  private final long evictionDelayMillis;

  private volatile FragmentExecutor executor;
  private volatile boolean fragmentStarted;

  private final AtomicBoolean canceled = new AtomicBoolean();
  private volatile long cancellationTime;

  private final Queue<FragmentEvent> finishedReceivers = new ConcurrentLinkedQueue<>();

  private volatile long expirationTime;

  FragmentHandler(FragmentHandle handle, long evictionDelayMillis) {
    this.handle = handle;
    this.evictionDelayMillis = evictionDelayMillis;
    expirationTime = System.currentTimeMillis() + evictionDelayMillis;
  }

  public void cancel() {
    // If an executor exists, we must ensure that it is informed.
    // Setting cancellation before the executor check ensures that either that any executor that comes in after
    // the cancelled flag is set will consume the flag so we don't have to worry about informing its cancellation
    if (canceled.compareAndSet(false,true)) {
      cancellationTime = System.currentTimeMillis();
      final FragmentExecutor executor = this.executor; // this is important, another thread could set this.executor to null
      if (executor != null) {
        executor.getListener().cancel();
      }
    }
  }

  private String formatError(String identifier, int sendingMajorFragmentId, int sendingMinorFragmentId) {
    return String.format("Received %s for %s from %d:%d before fragment executor started",
      identifier, QueryIdHelper.getQueryIdentifier(handle), sendingMajorFragmentId, sendingMinorFragmentId);
  }

  public void handle(FragmentStreamComplete completion) {
    Preconditions.checkState(fragmentStarted, formatError("stream completion",
      completion.getSendingMajorFragmentId(), completion.getSendingMinorFragmentId()));
    if (executor != null) {
      executor.getListener().handle(completion);
    }
    // A missing executor means it already terminated. We can simply drop this message.
  }

  public void handle(IncomingDataBatch batch) throws FragmentSetupException, IOException {
    Preconditions.checkState(fragmentStarted, formatError("data batch",
      batch.getHeader().getSendingMajorFragmentId(), batch.getHeader().getSendingMinorFragmentId()));
    if (executor != null) {
      executor.getListener().handle(batch);
    }
    // A missing executor means it already terminated. We can simply drop this message.
  }

  void receiverFinished(FragmentHandle receiver) {
    finishedReceivers.add(new FragmentEvent(receiver));
  }

  @Override
  public boolean isCancelled() {
    return canceled.get();
  }

  @Override
  public FragmentHandle pollFinishedReceiver() {
    final FragmentEvent event = finishedReceivers.poll();
    return event != null ? event.handle : null;
  }

  boolean isExpired() {
    return executor == null && System.currentTimeMillis() > expirationTime;
  }

  public FragmentHandle getHandle() {
    return handle;
  }

  public void setExecutor(FragmentExecutor executor) {
    this.executor = Preconditions.checkNotNull(executor, "fragment executor must be provided");
    fragmentStarted = true;
  }

  public FragmentExecutor getExecutor() {
    return executor;
  }

  public boolean isRunning() {
    return executor != null;
  }

  boolean hasStarted() {
    return fragmentStarted;
  }

  void checkStateAndLogIfNecessary() {
    if (!fragmentStarted) {
      final DateTimeFormatter formatter = ISODateTimeFormat.dateTime();
      if (isCancelled()) {
        logger.warn("Received cancel request at {} for fragment {} that was never started",
          formatter.print(cancellationTime),
          QueryIdHelper.getQueryIdentifier(handle));
      }

      FragmentEvent event;
      while ((event = finishedReceivers.poll()) != null) {
        logger.warn("Received early fragment termination at {} for path {} {} -> {} for a fragment that was never started",
          formatter.print(event.time),
          QueryIdHelper.getQueryId(handle.getQueryId()),
          QueryIdHelper.getFragmentId(event.handle),
          QueryIdHelper.getFragmentId(handle)
        );
      }
    }
  }

  void invalidate() {
    this.executor = null;
    expirationTime = System.currentTimeMillis() + evictionDelayMillis;
    checkStateAndLogIfNecessary();
  }
}
