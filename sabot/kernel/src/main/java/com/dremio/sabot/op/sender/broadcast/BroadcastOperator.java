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
package com.dremio.sabot.op.sender.broadcast;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.BroadcastSender;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sender.BaseSender;
import com.dremio.sabot.op.spi.TerminalOperator;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.primitives.Ints;

/**
 * Broadcast Sender broadcasts incoming batches to all receivers (one or more).
 * This is useful in cases such as broadcast join where sending the entire table to join
 * to all nodes is cheaper than merging and computing all the joins in the same node.
 */
public class BroadcastOperator extends BaseSender {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BroadcastOperator.class);

  private State state = State.NEEDS_SETUP;

  private final BroadcastSender config;
  private final int[][] receivingMinorFragments;
  private final AccountingExecTunnel[] tunnels;
  private final ExecProtos.FragmentHandle handle;
  private final OperatorStats stats;
  private final OperatorContext context;

  private VectorAccessible incoming;

  public enum Metric implements MetricDef {
    N_RECEIVERS,
    BYTES_SENT;
    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public BroadcastOperator(TunnelProvider tunnelProvider, OperatorContext context, BroadcastSender config) throws OutOfMemoryException {
    super(config);
    this.config = config;
    this.context = context;
    this.handle = context.getFragmentHandle();
    this.stats = context.getStats();

    final List<MinorFragmentEndpoint> destinations = config.getDestinations(context.getEndpointsIndex());
    final ArrayListMultimap<NodeEndpoint, Integer> dests = ArrayListMultimap.create();

    for(MinorFragmentEndpoint destination : destinations) {
      dests.put(destination.getEndpoint(), destination.getMinorFragmentId());
    }

    int destCount = dests.keySet().size();
    int i = 0;

    this.tunnels = new AccountingExecTunnel[destCount];
    this.receivingMinorFragments = new int[destCount][];
    for(final NodeEndpoint ep : dests.keySet()){
      List<Integer> minorsList= dests.get(ep);
      int[] minorsArray = new int[minorsList.size()];
      int x = 0;
      for(Integer m : minorsList){
        minorsArray[x++] = m;
      }
      receivingMinorFragments[i] = minorsArray;
      tunnels[i] = tunnelProvider.getExecTunnel(ep);
      i++;
    }
  }

  @Override
  public void setup(VectorAccessible incoming) {
    state.is(State.NEEDS_SETUP);
    this.incoming = incoming;
    checkSchema(incoming.getSchema());
    state = State.CAN_CONSUME;
  }

  private void updateStats(FragmentWritableBatch writableBatch) {
    stats.setLongStat(Metric.N_RECEIVERS, tunnels.length);
    stats.addLongStat(Metric.BYTES_SENT, writableBatch.getByteCount());
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void noMoreToConsume() {
    for (int i = 0; i < tunnels.length; ++i) {
      final FragmentStreamComplete completion = FragmentStreamComplete.newBuilder()
          .setQueryId(handle.getQueryId())
          .setSendingMajorFragmentId(handle.getMajorFragmentId())
          .setSendingMinorFragmentId(handle.getMinorFragmentId())
          .setReceivingMajorFragmentId(config.getReceiverMajorFragmentId())
          .addAllReceivingMinorFragmentId(Ints.asList(receivingMinorFragments[i]))
          .build();
      tunnels[i].sendStreamComplete(completion);
    }
    state = State.DONE;
  }


  @Override
  public void receivingFragmentFinished(FragmentHandle handle) {
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitTerminalOperator(this, value);
  }

  @Override
  public void consumeData(int records) {

    ArrowRecordBatch arrowRecordBatch = FragmentWritableBatch.getArrowRecordBatch(incoming);

    List<ArrowBuf> buffers = arrowRecordBatch.getBuffers();

    buffers = FluentIterable.from(buffers)
      .transform(new Function<ArrowBuf, ArrowBuf>() {
        @Nullable
        @Override
        public ArrowBuf apply(@Nullable ArrowBuf buf) {
          long writerIndex = buf.writerIndex();
          ArrowBuf newBuf = buf.getReferenceManager().transferOwnership(buf, context.getAllocator())
            .getTransferredBuffer();
          newBuf.writerIndex(writerIndex);
          buf.release();
          return newBuf;
        }
      }).toList();

    if (tunnels.length > 1) {
      for (ArrowBuf buf : buffers) {
        buf.retain(tunnels.length - 1);
      }
    }

    for (int i = 0; i < tunnels.length; ++i) {
      FragmentWritableBatch batch = new FragmentWritableBatch(
          handle.getQueryId(),
          handle.getMajorFragmentId(),
          handle.getMinorFragmentId(),
          config.getReceiverMajorFragmentId(),
          new ArrowRecordBatch(arrowRecordBatch.getLength(), arrowRecordBatch.getNodes(), buffers, NoCompressionCodec.DEFAULT_BODY_COMPRESSION, false),
          receivingMinorFragments[i]);
      updateStats(batch);
      tunnels[i].sendRecordBatch(batch);
      for (ArrowBuf buf : buffers) {
        buf.release();
      }
    }
  }

  public static class Creator implements TerminalOperator.Creator<BroadcastSender> {
    @Override
    public TerminalOperator create(TunnelProvider tunnelProvider, OperatorContext context, BroadcastSender operator)
        throws ExecutionSetupException {
      return new BroadcastOperator(tunnelProvider, context, operator);
    }
  }

}
