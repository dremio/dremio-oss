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
package com.dremio.sabot.exec;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicStampedReference;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.dremio.common.util.MayExpire;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.exec.rpc.IncomingDataBatch;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A FragmentHandler ensures proper handling of cancellation/early termination messages even when their corresponding
 * FragmentExecutor didn't start yet. Implementation must handle concurrent accesses from both the FragmentExecutor and
 * the fabric thread reporting the messages.<br>
 * If the corresponding fragment doesn't start after the eviction delay, handler will be marked as expired and removed
 * from the cache. Also once a fragment is done and after the eviction delay the handler will also be marked as expired.
 */
public class FragmentHandler implements EventProvider, MayExpire {
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

  private volatile boolean fragmentStarted;

  //holds a reference to the executor object
  private AtomicStampedReference<FragmentExecutor> execReference;

  //stamp state at the beginning, i.e. when FragmentHandler is created & executor is null
  private static final int defaultExecStamp = 0;

  //stamp value once the proper executor is set inside 'setExecutor'
  private static final int validExecStamp = 1;

  //stamp value once the executor is set back to null as part of 'invalidate'
  private static final int invalidExecStamp = 2;

  private final AtomicBoolean canceled = new AtomicBoolean();
  private volatile long cancellationTime;

  private final Queue<FragmentEvent> finishedReceivers = new ConcurrentLinkedQueue<>();

  private volatile long expirationTime;
  private Throwable failedReason;

  FragmentHandler(FragmentHandle handle, long evictionDelayMillis) {
    this.handle = handle;
    this.evictionDelayMillis = evictionDelayMillis;
    expirationTime = System.currentTimeMillis() + evictionDelayMillis;
    execReference = new AtomicStampedReference<FragmentExecutor>(null, defaultExecStamp);
  }

  public void activate() {
    final FragmentExecutor executor = execReference.getReference();
    if (executor != null) {
      executor.getListener().activate();
    }
  }

  public void cancel() {
    // If an executor exists, we must ensure that it is informed.
    // Setting cancellation before the executor check ensures that either that any executor that comes in after
    // the cancelled flag is set will consume the flag so we don't have to worry about informing its cancellation
    if (canceled.compareAndSet(false,true)) {
      cancellationTime = System.currentTimeMillis();
      final FragmentExecutor executor = execReference.getReference(); // this is important, another thread could set this.executor to null
      if (executor != null) {
        executor.getListener().cancel();
      }
    }
  }

  void fail(Throwable failedReason) {
    synchronized (this) {
      if (this.failedReason == null) {
        this.failedReason = failedReason;
      }
    }
    cancel();
  }

  private String formatError(String identifier, int sendingMajorFragmentId, int sendingMinorFragmentId) {
    return String.format("Received %s for %s from %d:%d before fragment executor started",
      identifier, QueryIdHelper.getQueryIdentifier(handle), sendingMajorFragmentId, sendingMinorFragmentId);
  }

  public void handle(OutOfBandMessage message) {
    // A missing executor means it already terminated. We can simply drop this message.
    final FragmentExecutor executor = execReference.getReference();
    if (executor != null) {
      executor.getListener().handle(message);
    }
  }

  public void handle(FragmentStreamComplete completion) {
    Preconditions.checkState(fragmentStarted, formatError("stream completion",
      completion.getSendingMajorFragmentId(), completion.getSendingMinorFragmentId()));

    // A missing executor means it already terminated. We can simply drop this message.
    final FragmentExecutor executor = execReference.getReference();
    if (executor != null) {
      executor.getListener().handle(completion);
    }
  }

  public void handle(IncomingDataBatch batch) {
    Preconditions.checkState(fragmentStarted, formatError("data batch",
      batch.getHeader().getSendingMajorFragmentId(), batch.getHeader().getSendingMinorFragmentId()));

    // A missing executor means it already terminated. We can simply drop this message.
    final FragmentExecutor executor = execReference.getReference();
    if (executor != null) {
      executor.getListener().handle(batch);
    }
  }

  void receiverFinished(FragmentHandle receiver) {
    finishedReceivers.add(new FragmentEvent(receiver));
  }

  @Override
  public synchronized Optional<Throwable> getFailedReason() {
    return Optional.ofNullable(failedReason);
  }

  @Override
  public FragmentHandle pollFinishedReceiver() {
    final FragmentEvent event = finishedReceivers.poll();
    return event != null ? event.handle : null;
  }

  @Override
  public boolean isExpired() {
    return execReference.getReference() == null && System.currentTimeMillis() > expirationTime;
  }

  public FragmentHandle getHandle() {
    return handle;
  }

  public void setExecutor(FragmentExecutor executor) {
    Preconditions.checkNotNull(executor);
    Preconditions.checkState(execReference.getStamp() == defaultExecStamp);

    execReference.set(executor, validExecStamp);
    fragmentStarted = true;
  }

  public FragmentExecutor getExecutor() {
    return execReference.getReference();
  }

  public boolean isRunning() { return execReference.getReference() != null; }

  boolean hasStarted() {
    return fragmentStarted;
  }

  void checkStateAndLogIfNecessary() {
    if (!fragmentStarted) {
      final DateTimeFormatter formatter = ISODateTimeFormat.dateTime();
      if (canceled.get()) {
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
    expirationTime = System.currentTimeMillis() + evictionDelayMillis;

    if (execReference.getReference() != null) {
      Preconditions.checkState(execReference.getStamp() == validExecStamp);
    }
    execReference.set(null, invalidExecStamp);
    checkStateAndLogIfNecessary();
  }

  // Testing purposes only: mark this fragment handler as expired
  @VisibleForTesting
  void testExpireNow() {
    expirationTime = System.currentTimeMillis() - 1;
  }

}
