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
package com.dremio.sabot.op.sender;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.BridgeFileWriterSender;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.ExecRPC;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.rpc.AccountingFileTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.dremio.sabot.op.spi.TerminalOperator;
import com.dremio.service.spill.SpillService;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Impl for sender operator that writes to a file, instead of a socket. */
@Options
public class BridgeFileWriterSenderOperator extends BaseSender {
  private static final Logger logger =
      LoggerFactory.getLogger(BridgeFileWriterSenderOperator.class);
  public static final PositiveLongValidator NUM_BATCHES_PER_FILE =
      new PositiveLongValidator("exec.op.sender.batches_per_file", 100_000, 2_000);
  private final BridgeFileWriterSender config;
  private final ExecProtos.FragmentHandle handle;
  private final OperatorStats stats;
  private final BufferAllocator allocator;
  private final TunnelProvider tunnelProvider;
  private final String uniqueId;
  private final OptionManager options;
  private final SpillService spillService;
  private final SabotConfig sabotConfig;

  private State state = State.NEEDS_SETUP;
  private SpillManager spillManager;
  private AccountingFileTunnel tunnel;
  private VectorAccessible incoming;

  public enum Metric implements MetricDef {
    BYTES_SENT;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public BridgeFileWriterSenderOperator(
      TunnelProvider tunnelProvider, OperatorContext context, BridgeFileWriterSender config) {
    super(config);
    this.tunnelProvider = tunnelProvider;
    this.config = config;
    this.allocator = context.getAllocator();
    this.handle = context.getFragmentHandle();
    this.stats = context.getStats();
    this.uniqueId =
        BridgeFileWriterSender.computeUniqueId(
            handle.getQueryId(), config.getBridgeSetId(), handle.getMinorFragmentId());
    this.options = context.getOptions();
    this.spillService = context.getSpillService();
    this.sabotConfig = context.getConfig();

    logger.debug("uniqueId {}", uniqueId);
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(
      OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitTerminalOperator(this, value);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void setup(VectorAccessible incoming) throws Exception {
    state.is(State.NEEDS_SETUP);
    this.incoming = incoming;
    checkSchema(incoming.getSchema());

    this.spillManager =
        new SpillManager(sabotConfig, options, uniqueId, null, spillService, "cte bridge", null);
    this.tunnel =
        tunnelProvider.getFileTunnel(
            new FileStreamManagerImpl(spillManager), (int) options.getOption(NUM_BATCHES_PER_FILE));
    state = State.CAN_CONSUME;
  }

  @Override
  public void consumeData(int records) throws Exception {
    ArrowRecordBatch arrowRecordBatch = FragmentWritableBatch.getArrowRecordBatch(incoming);
    // transfer buffers to this operator's allocator.
    List<ArrowBuf> buffers = arrowRecordBatch.getBuffers();
    buffers =
        buffers.stream()
            .map(
                buf -> {
                  long writerIndex = buf.writerIndex();
                  ArrowBuf newBuf =
                      buf.getReferenceManager()
                          .transferOwnership(buf, allocator)
                          .getTransferredBuffer();
                  newBuf.writerIndex(writerIndex);
                  buf.getReferenceManager().release();
                  return newBuf;
                })
            .collect(Collectors.toList());

    FragmentWritableBatch batch =
        new FragmentWritableBatch(
            handle.getQueryId(),
            handle.getMajorFragmentId(),
            handle.getMinorFragmentId(),
            config.getReceiverMajorFragmentId(),
            new ArrowRecordBatch(
                arrowRecordBatch.getLength(),
                arrowRecordBatch.getNodes(),
                buffers,
                NoCompressionCodec.DEFAULT_BODY_COMPRESSION,
                false));

    // write batch to the file tunnel.
    try (AutoCloseable ac = OperatorStats.getWaitRecorder(stats)) {
      tunnel.sendRecordBatch(batch);
    }
    updateStats(batch);
    logger.debug("wrote batch {} records", records);

    // release buffers
    for (ArrowBuf buf : buffers) {
      buf.getReferenceManager().release();
    }
    if (tunnel.isAllReceiversDone()) {
      // if all receivers have finished, no point in continuing to send data.
      state = State.DONE;
      logger.debug("switching to DONE state because all receivers are finished");
    }
  }

  private void updateStats(FragmentWritableBatch writableBatch) {
    stats.addLongStat(
        BridgeFileWriterSenderOperator.Metric.BYTES_SENT, writableBatch.getByteCount());
  }

  @Override
  public void receivingFragmentFinished(ExecProtos.FragmentHandle handle) throws Exception {
    throw new UnsupportedOperationException(
        "receivingFragmentFinished() not expected in this operator");
  }

  @Override
  public void noMoreToConsume() throws Exception {
    // write a "StreamComplete" msg to the file tunnel.
    final ExecRPC.FragmentStreamComplete completion =
        ExecRPC.FragmentStreamComplete.newBuilder()
            .setQueryId(handle.getQueryId())
            .setSendingMajorFragmentId(handle.getMajorFragmentId())
            .setSendingMinorFragmentId(handle.getMinorFragmentId())
            .build();
    tunnel.sendStreamComplete(completion);
    state = State.DONE;
    logger.debug("switching to DONE state on invocation of noMoreToConsume()");
  }

  // used for testing only
  public SpillManager getSpillManager() {
    return spillManager;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(tunnel);
  }

  public static class Creator implements TerminalOperator.Creator<BridgeFileWriterSender> {
    @Override
    public TerminalOperator create(
        TunnelProvider tunnelProvider, OperatorContext context, BridgeFileWriterSender operator)
        throws ExecutionSetupException {
      return new BridgeFileWriterSenderOperator(tunnelProvider, context, operator);
    }
  }
}
