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
package com.dremio.sabot.op.receiver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.DeferredException;
import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.proto.CoordExecRPC.Collector;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.sabot.exec.fragment.FragmentWorkQueue;
import com.dremio.sabot.exec.rpc.IncomingDataBatch;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.spi.BatchStreamProvider;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;
import com.dremio.service.spill.SpillService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * Determines when a particular fragment has enough data for each of its receiving exchanges to commence execution.  Also monitors whether we've collected all incoming data.
 */
public class IncomingBuffers implements BatchStreamProvider, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IncomingBuffers.class);

  private volatile boolean closed = false;
  private final Map<Integer, DataCollector> collectorMap;
//  private final FragmentContext context;

  /**
   * Lock used to manage close and data acceptance. We should only create a local reference to incoming data in the case
   * that the incoming buffers are !closed. As such, we need to make sure that we aren't in the process of closing the
   * incoming buffers when data is arriving. The read lock can be shared by many incoming batches but the write lock
   * must be exclusive to the close method.
   */
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final AutoCloseableLock sharedIncomingBatchLock = new AutoCloseableLock(lock.readLock());
  private final AutoCloseableLock exclusiveCloseLock = new AutoCloseableLock(lock.writeLock());
  private final BufferAllocator allocator;
  private final SharedResourceGroup resourceGroup;
  private final DeferredException deferredException;

  public IncomingBuffers(
      DeferredException exception,
      SharedResourceGroup resourceGroup,
      FragmentWorkQueue workQueue,
      TunnelProvider tunnelProvider,
      PlanFragment fragment,
      BufferAllocator incomingAllocator,
      SabotConfig config,
      SpillService spillService
      ) {
    this.deferredException = exception;
    this.resourceGroup = resourceGroup;

    final String allocatorName = String.format("op:%s:incoming",
        QueryIdHelper.getFragmentId(fragment.getHandle()));
    this.allocator = incomingAllocator.newChildAllocator(allocatorName, 0, Long.MAX_VALUE);


    final Map<Integer, DataCollector> collectors = Maps.newHashMap();
    for(int i =0; i < fragment.getCollectorCount(); i++){
      Collector collector = fragment.getCollector(i);

      DataCollector newCollector = collector.getSupportsOutOfOrder() ?
          new MergingCollector(resourceGroup, collector, allocator, config, fragment.getHandle(), workQueue, tunnelProvider, spillService) :
          new PartitionedCollector(resourceGroup, collector, allocator, config, fragment.getHandle(), workQueue, tunnelProvider, spillService);
      collectors.put(collector.getOppositeMajorFragmentId(), newCollector);
    }

    collectorMap = ImmutableMap.copyOf(collectors);
  }

  public void completionArrived(final FragmentStreamComplete completion) {
    try (AutoCloseableLock lock = sharedIncomingBatchLock.open()) {
      if (closed) {
        return;
      }

      final DataCollector collector = collector(completion.getSendingMajorFragmentId());

      synchronized (collector) {
        collector.streamCompleted(completion.getSendingMinorFragmentId());
      }
    }
  }

  private DataCollector collector(int sendMajorFragmentId){
    DataCollector collector = collectorMap.get(sendMajorFragmentId);
    Preconditions.checkNotNull(collector, "We received a major fragment id that we were not expecting.  The id was %d. %s", sendMajorFragmentId, Arrays.toString(collectorMap.values().toArray()));
    return collector;
  }

  public void batchArrived(final IncomingDataBatch incomingBatch) throws FragmentSetupException, IOException {
    if(!incomingBatch.checkAcceptance(allocator.getHeadroom())){
      deferredException.addException(UserException.memoryError()
          .message("Out of memory while receiving incoming message. Message size: %d, Current thread allocation: %d, thread limit: %d.", incomingBatch.size(), allocator.getAllocatedMemory(), allocator.getLimit())
          .build(logger));
      return;
    }

    // we want to make sure that we only generate local record batch reference in the case that we're not closed.
    // Otherwise we would leak memory.
    try (AutoCloseableLock lock = sharedIncomingBatchLock.open()) {
      if (closed) {
        return;
      }

      final DataCollector collector = collector(incomingBatch.getHeader().getSendingMajorFragmentId());

      synchronized (collector) {
        try(final RawFragmentBatch newRawFragmentBatch = incomingBatch.newRawFragmentBatch(allocator)){
          collector.batchArrived(incomingBatch.getHeader().getSendingMinorFragmentId(), newRawFragmentBatch);
        }
      }
    }

  }


  @Override
  public boolean isPotentiallyBlocked() {
    return !resourceGroup.isAvailable();
  }

  public RawFragmentBatchProvider[] getBuffers(int senderMajorFragmentId) {
    DataCollector collector = collectorMap.get(senderMajorFragmentId);
    Preconditions.checkNotNull(collector, "Invalid major fragment id %d. Expected a value in %s", senderMajorFragmentId, collectorMap.values().toString());
    return collector.getBuffers();
  }

  @Override
  public void close() throws Exception {
    try (AutoCloseableLock lock = exclusiveCloseLock.open()) {
      closed = true;
      AutoCloseables.close(Iterables.concat(
          collectorMap.values(),
          Collections.singleton(allocator)
          ));
    }
  }

}
