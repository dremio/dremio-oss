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
package com.dremio.sabot.op.screen;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.physical.config.Screen;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.WritableBatch;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.rpc.AccountingExecToCoordTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.spi.TerminalOperator;
import org.apache.arrow.memory.OutOfMemoryException;

public class ScreenOperator implements TerminalOperator {
  private static final ControlsInjector injector =
      ControlsInjectorFactory.getInjector(ScreenOperator.class);
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ScreenOperator.class);

  private final OperatorContext context;
  private final OperatorStats stats;
  private final AccountingExecToCoordTunnel execToCoord;
  private final Screen config;

  private State state = State.NEEDS_SETUP;
  private VectorAccessible incoming;
  private RecordMaterializer materializer;
  private long batchesSent = 0;

  public static enum Metric implements MetricDef {
    BYTES_SENT;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public ScreenOperator(TunnelProvider tunnelProvider, OperatorContext context, Screen operator)
      throws OutOfMemoryException {
    this.execToCoord = tunnelProvider.getCoordTunnel();
    this.context = context;
    this.stats = context.getStats();
    this.config = operator;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void setup(VectorAccessible incoming) {
    this.incoming = incoming;
    this.materializer = new VectorRecordMaterializer(context, incoming);
    state = State.CAN_CONSUME;
  }

  @Override
  public void noMoreToConsume() {
    state = State.DONE;
    // make sure we send a schema batch.
    if (batchesSent == 0) {

      WritableBatch writable = WritableBatch.getBatchNoHVWrap(0, incoming, false);

      final QueryData header =
          QueryData.newBuilder()
              .setQueryId(context.getFragmentHandle().getQueryId())
              .setRowCount(0)
              .setDef(writable.getDef())
              .build();
      writable.close();
      final QueryWritableBatch batch = new QueryWritableBatch(header);

      stats.startWait();
      try {
        execToCoord.sendData(batch);
      } finally {
        stats.stopWait();
      }
    }
  }

  @Override
  public void consumeData(int records) {
    state.is(State.CAN_CONSUME);
    final QueryWritableBatch batch = materializer.convertNext(records);
    stats.addLongStat(Metric.BYTES_SENT, batch.getByteCount());
    stats.startWait();
    try {
      execToCoord.sendData(batch);
    } finally {
      stats.stopWait();
    }

    batchesSent++;
  }

  @Override
  public void receivingFragmentFinished(FragmentHandle handle) {
    throw new IllegalStateException("No operators should exist downstream of Screen.");
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(
      OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitTerminalOperator(this, value);
  }

  @Override
  public void close() throws Exception {}

  public static class Creator implements TerminalOperator.Creator<Screen> {
    @Override
    public TerminalOperator create(
        TunnelProvider tunnelProvider, OperatorContext context, Screen operator)
        throws ExecutionSetupException {
      if (operator.getSilent()) {
        return new SilentScreenOperator();
      }
      return new ScreenOperator(tunnelProvider, context, operator);
    }
  }
}
