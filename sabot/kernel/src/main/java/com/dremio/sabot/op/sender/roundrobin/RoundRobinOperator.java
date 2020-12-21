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
package com.dremio.sabot.op.sender.roundrobin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nullable;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.physical.config.RoundRobinSender;
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

/**
 * Round Robin Sender broadcasts incoming batches to receivers in a round robin fashion.
 */
public class RoundRobinOperator extends BaseSender {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RoundRobinOperator.class);

  private State state = State.NEEDS_SETUP;

  private final RoundRobinSender config;
  private final ExecProtos.FragmentHandle handle;
  private final OperatorStats stats;
  private final BufferAllocator allocator;

  private final List<AccountingExecTunnel> tunnels;
  private final List<List<Integer>> minorFragments;
  private int currentTunnelsIndex;
  private int currentMinorFragmentsIndex;

  private VectorAccessible incoming;

  public enum Metric implements MetricDef {
    N_RECEIVERS,
    BYTES_SENT;
    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public RoundRobinOperator(TunnelProvider tunnelProvider, OperatorContext context, RoundRobinSender config) throws OutOfMemoryException {
    super(config);
    this.config = config;
    this.allocator = context.getAllocator();
    this.handle = context.getFragmentHandle();
    this.stats = context.getStats();

    List<MinorFragmentEndpoint> destinations = config.getDestinations(context.getEndpointsIndex());
    final ArrayListMultimap<NodeEndpoint, Integer> dests = ArrayListMultimap.create();
    for(MinorFragmentEndpoint destination : destinations) {
      dests.put(destination.getEndpoint(), destination.getMinorFragmentId());
    }

    this.tunnels = new ArrayList<>();
    this.minorFragments = new ArrayList<>();
    for(final NodeEndpoint ep : dests.keySet()){
      List<Integer> minorsList= dests.get(ep);
      minorFragments.add(minorsList);
      tunnels.add(tunnelProvider.getExecTunnel(ep));
    }

    int destCount = dests.keySet().size();
    this.currentTunnelsIndex = ThreadLocalRandom.current().nextInt(destCount);
    this.currentMinorFragmentsIndex = ThreadLocalRandom.current().nextInt(minorFragments.get(currentTunnelsIndex).size());
  }

  @Override
  public void setup(VectorAccessible incoming) {
    state.is(State.NEEDS_SETUP);
    this.incoming = incoming;
    checkSchema(incoming.getSchema());
    state = State.CAN_CONSUME;
  }

  private void updateStats(FragmentWritableBatch writableBatch) {
    stats.setLongStat(Metric.N_RECEIVERS, tunnels.size());
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
    for (int i = 0; i < tunnels.size(); ++i) {
      final FragmentStreamComplete completion = FragmentStreamComplete.newBuilder()
        .setQueryId(handle.getQueryId())
        .setSendingMajorFragmentId(handle.getMajorFragmentId())
        .setSendingMinorFragmentId(handle.getMinorFragmentId())
        .setReceivingMajorFragmentId(config.getReceiverMajorFragmentId())
        .addAllReceivingMinorFragmentId(minorFragments.get(i))
        .build();
      tunnels.get(i).sendStreamComplete(completion);
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
          ArrowBuf newBuf = buf.getReferenceManager().transferOwnership(buf, allocator).getTransferredBuffer();
          newBuf.writerIndex(writerIndex);
          buf.release();
          return newBuf;
        }
      }).toList();

    FragmentWritableBatch batch = new FragmentWritableBatch(
      handle.getQueryId(),
      handle.getMajorFragmentId(),
      handle.getMinorFragmentId(),
      config.getReceiverMajorFragmentId(),
      new ArrowRecordBatch(arrowRecordBatch.getLength(), arrowRecordBatch.getNodes(), buffers, NoCompressionCodec.DEFAULT_BODY_COMPRESSION, false),
      minorFragments.get(currentTunnelsIndex).get(currentMinorFragmentsIndex)
    );
    updateStats(batch);
    tunnels.get(currentTunnelsIndex).sendRecordBatch(batch);

    currentMinorFragmentsIndex++;
    if (currentMinorFragmentsIndex >= minorFragments.get(currentTunnelsIndex).size()) {
      currentTunnelsIndex = (currentTunnelsIndex + 1) % tunnels.size();
      currentMinorFragmentsIndex = 0;
    }
    for (ArrowBuf buf : buffers) {
      buf.release();
    }
  }

  public static class Creator implements TerminalOperator.Creator<RoundRobinSender> {
    @Override
    public TerminalOperator create(TunnelProvider tunnelProvider, OperatorContext context, RoundRobinSender operator)
      throws ExecutionSetupException {
      return new RoundRobinOperator(tunnelProvider, context, operator);
    }
  }

}
