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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC.Collector;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FinishedReceiver;
import com.dremio.exec.util.ArrayWrappedIntIntMap;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.fragment.FragmentWorkQueue;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import com.dremio.service.spill.SpillService;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public abstract class AbstractDataCollector implements DataCollector {

  private final int oppositeMajorFragmentId;
  private final int incomingStreams;
  protected final RawBatchBuffer[] buffers;
  protected final ArrayWrappedIntIntMap fragmentMap;
  private final CompletionMessageSender[] completionMessages;
  private final TunnelProvider tunnelProvider;
  private final FragmentHandle handle;
  private final Collector config;
  private volatile boolean closed = false;

  /**
   * @param parentAccounter
   * @param receiver
   * @param numBuffers Number of RawBatchBuffer inputs required to store the incoming data
   * @param bufferCapacity Capacity of each RawBatchBuffer.
   * @param context
   */
  public AbstractDataCollector(
    SharedResourceGroup resourceGroup,
    boolean isDiscrete,
    Collector collector,
    final int bufferCapacity,
    BufferAllocator allocator,
    SabotConfig config,
    OptionManager options,
    FragmentHandle handle,
    FragmentWorkQueue workQueue,
    TunnelProvider tunnelProvider,
    SpillService spillService,
    EndpointsIndex endpointsIndex) {
    Preconditions.checkNotNull(collector);
    Preconditions.checkNotNull(endpointsIndex);
    this.config = collector;
    this.handle = handle;
    this.tunnelProvider = tunnelProvider;
    this.incomingStreams = collector.getIncomingMinorFragmentIndexCount();
    this.oppositeMajorFragmentId = collector.getOppositeMajorFragmentId();
    this.completionMessages = new CompletionMessageSender[incomingStreams];

    // Create fragmentId to index that is within the range [0, incoming.size()-1]
    // We use this mapping to find objects belonging to the fragment in buffers and remainders arrays.
    fragmentMap = new ArrayWrappedIntIntMap();
    int index = 0;
    for (MinorFragmentIndexEndpoint fragment: collector.getIncomingMinorFragmentIndexList()) {
      fragmentMap.put(fragment.getMinorFragmentId(), index);

      // save completion messages for later.
      NodeEndpoint endpoint = endpointsIndex.getNodeEndpoint(fragment.getEndpointIndex());
      completionMessages[index] = new CompletionMessageSender(endpoint, fragment.getMinorFragmentId());
      index++;
    }

    final boolean spooling = collector.getIsSpooling();

    if (isDiscrete) {
      buffers = new RawBatchBuffer[collector.getIncomingMinorFragmentIndexCount()];
      List<MinorFragmentIndexEndpoint> fragments = collector.getIncomingMinorFragmentIndexList();
      try (AutoCloseables.RollbackCloseable rollbackCloseable = new AutoCloseables.RollbackCloseable()){
        for (MinorFragmentIndexEndpoint fragment : fragments) {
          NodeEndpoint endpoint = endpointsIndex.getNodeEndpoint(fragment.getEndpointIndex());
          final String name = String.format("nway-recv-%s-%s:%d:%d", spooling ? "spool" : "mem", endpoint.getAddress(), collector.getOppositeMajorFragmentId(), fragment.getMinorFragmentId());
          final SharedResource resource = resourceGroup.createResource(name, spooling ? SharedResourceType.NWAY_RECV_SPOOL_BUFFER : SharedResourceType.NWAY_RECV_MEM_BUFFER);
          final RawBatchBuffer buffer;
          if (spooling) {
            buffer = new SpoolingRawBatchBuffer(resource, config, options, workQueue, handle, spillService, allocator, bufferCapacity, collector.getOppositeMajorFragmentId(), fragment.getMinorFragmentId());
          } else {
            buffer = new UnlimitedRawBatchBuffer(resource, options, handle, allocator, bufferCapacity, collector.getOppositeMajorFragmentId());
          }
          rollbackCloseable.add(buffer);
          buffer.init();
          buffers[fragment.getMinorFragmentId()] = buffer;
        }
        rollbackCloseable.commit();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    } else {
      buffers = new RawBatchBuffer[1];
      final String name = String.format("unordered-spooling-recv-%s-%d:*", spooling ? "spool" : "mem", collector.getOppositeMajorFragmentId());
      final SharedResource resource = resourceGroup.createResource(name, spooling ? SharedResourceType.UNORDERED_RECV_SPOOL_BUFFER : SharedResourceType.UNORDERED_RECV_MEM_BUFFER);
      final RawBatchBuffer buffer;
      if (spooling) {
        buffer = new SpoolingRawBatchBuffer(resource, config, options, workQueue, handle, spillService, allocator, bufferCapacity, collector.getOppositeMajorFragmentId(), 0);
      } else {
        buffer = new UnlimitedRawBatchBuffer(resource, options, handle, allocator, bufferCapacity, collector.getOppositeMajorFragmentId());
      }
      buffers[0] = buffer;
      buffers[0].init();
    }
  }

  @Override
  public int getOppositeMajorFragmentId() {
    return oppositeMajorFragmentId;
  }

  @Override
  public RawBatchBuffer[] getBuffers(){
    return buffers;
  }


  @Override
  public void streamCompleted(int minorFragmentId) {
    final int workIndex = fragmentMap.get(minorFragmentId);
    completionMessages[workIndex].markDone();
    getBuffer(minorFragmentId).streamComplete();
  }

  @Override
  public void batchArrived(int minorFragmentId, RawFragmentBatch batch) {
    getBuffer(minorFragmentId).enqueue(batch);
  }


  @Override
  public int getTotalIncomingFragments() {
    return incomingStreams;
  }

  protected abstract RawBatchBuffer getBuffer(int minorFragmentId);

  @Override
  public synchronized void close() throws Exception {
    if(!closed){

      final List<AutoCloseable> closeables = new ArrayList<>();
      closeables.addAll(Arrays.asList(buffers));

      closeables.add(new AutoCloseable() {

        @Override
        public void close() throws Exception {
          // we may close before we received the last message. In this
          // situation, we need to send everybody a finished message.
          // We can optimize this to only do so in the situation where the query
          // isn't currently failing. In those cases, the Foreman will already
          // be cleaning up.
          for (int i = 0; i < completionMessages.length; i++) {
            completionMessages[i].informUpstreamIfNecessary();
          }
        }

      });

      closeables.add(new AutoCloseable() {
        @Override
        public void close() throws Exception {
          closed = true;
        }
      });

      AutoCloseables.close(closeables);
    }
  }

  private class CompletionMessageSender {
    private volatile boolean done = false;

    private NodeEndpoint sendingNode;
    private int sendingMinorFragmentId;

    public CompletionMessageSender(NodeEndpoint sendingNode, int sendingMinorFragmentId) {
      super();
      this.sendingNode = sendingNode;
      this.sendingMinorFragmentId = sendingMinorFragmentId;
    }

    public void informUpstreamIfNecessary(){
      if(!done){
        final FinishedReceiver message = FinishedReceiver.newBuilder()
          .setReceiver(handle)
          .setSender(FragmentHandle.newBuilder()
                       .setQueryId(handle.getQueryId())
                       .setMajorFragmentId(config.getOppositeMajorFragmentId())
                       .setMinorFragmentId(sendingMinorFragmentId))
          .build();
        tunnelProvider.getExecTunnel(sendingNode).informReceiverFinished(message);
        done = true;
      }
    }

    public void markDone(){
      done = true;
    }

  }
}
