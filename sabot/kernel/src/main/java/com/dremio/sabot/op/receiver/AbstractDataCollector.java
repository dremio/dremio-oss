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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordExecRPC.Collector;
import com.dremio.exec.proto.CoordExecRPC.IncomingMinorFragment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FinishedReceiver;
import com.dremio.exec.util.ArrayWrappedIntIntMap;
import com.dremio.sabot.exec.fragment.FragmentWorkQueue;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import com.google.common.base.Preconditions;

public abstract class AbstractDataCollector implements DataCollector {

  // private final List<MinorFragmentEndpoint> incoming;
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
      FragmentHandle handle,
      FragmentWorkQueue workQueue,
      TunnelProvider tunnelProvider) {
    Preconditions.checkNotNull(collector);
    this.config = collector;
    this.handle = handle;
    this.tunnelProvider = tunnelProvider;
    this.incomingStreams = collector.getIncomingMinorFragmentCount();
    this.oppositeMajorFragmentId = collector.getOppositeMajorFragmentId();
    this.completionMessages = new CompletionMessageSender[incomingStreams];

    // Create fragmentId to index that is within the range [0, incoming.size()-1]
    // We use this mapping to find objects belonging to the fragment in buffers and remainders arrays.
    fragmentMap = new ArrayWrappedIntIntMap();
    int index = 0;
    for (IncomingMinorFragment fragment: collector.getIncomingMinorFragmentList()) {
      fragmentMap.put(fragment.getMinorFragment(), index);

      // save completion messages for later.
      completionMessages[index] = new CompletionMessageSender(fragment.getEndpoint(), fragment.getMinorFragment());
      index++;
    }

    final boolean spooling = collector.getIsSpooling();

    if (isDiscrete) {
      buffers = new RawBatchBuffer[collector.getIncomingMinorFragmentCount()];
      List<IncomingMinorFragment> fragments = collector.getIncomingMinorFragmentList();
      for (IncomingMinorFragment fragment : fragments) {
        final String name = String.format("nway-recv-%s-%s:%d:%d", spooling ? "spool" : "mem", fragment.getEndpoint().getAddress(), collector.getOppositeMajorFragmentId(), fragment.getMinorFragment());
        final SharedResource resource = resourceGroup.createResource(name, spooling ? SharedResourceType.NWAY_RECV_SPOOL_BUFFER : SharedResourceType.NWAY_RECV_MEM_BUFFER);
        if (spooling) {
          buffers[fragment.getMinorFragment()] = new SpoolingRawBatchBuffer(resource, config, workQueue, handle, allocator, bufferCapacity, collector.getOppositeMajorFragmentId(), fragment.getMinorFragment());
        } else {
          buffers[fragment.getMinorFragment()] = new UnlimitedRawBatchBuffer(resource, config, handle, allocator, bufferCapacity, collector.getOppositeMajorFragmentId());
        }
      }
    } else {
      buffers = new RawBatchBuffer[1];
      final String name = String.format("unordered-spooling-recv-%s-%d:*", spooling ? "spool" : "mem", collector.getOppositeMajorFragmentId());
      final SharedResource resource = resourceGroup.createResource(name, spooling ? SharedResourceType.UNORDERED_RECV_SPOOL_BUFFER : SharedResourceType.UNORDERED_RECV_MEM_BUFFER);
      if (spooling) {
        buffers[0] = new SpoolingRawBatchBuffer(resource, config, workQueue, handle, allocator, bufferCapacity, collector.getOppositeMajorFragmentId(), 0);
      } else {
        buffers[0] = new UnlimitedRawBatchBuffer(resource, config, handle, allocator, bufferCapacity, collector.getOppositeMajorFragmentId());
      }
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
