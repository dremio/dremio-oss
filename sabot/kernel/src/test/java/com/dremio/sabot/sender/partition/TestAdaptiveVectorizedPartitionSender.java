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
package com.dremio.sabot.sender.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.options.OptionValue;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.CustomGenerator;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sender.partition.vectorized.AdaptiveVectorizedPartitionSenderOperator;
import com.dremio.sabot.op.sender.partition.vectorized.VectorizedPartitionSenderOperator;
import com.dremio.sabot.op.spi.TerminalOperator;
import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;

/**
 *
 */
public class TestAdaptiveVectorizedPartitionSender extends BaseTestOperator {
  static final int NUM_FRAGMENTS = 3;
  CustomGenerator generator;
  @BeforeClass
  public static void setUp() throws Exception {
    testContext.getOptions().setOption(OptionValue.createBoolean(OptionValue.OptionType.SYSTEM,
      ExecConstants.ADAPTIVE_HASH.getOptionName(),
      true));
  }

  @AfterClass
  public static void shutdown() throws Exception {
    testContext.getOptions().setOption(OptionValue.createBoolean(OptionValue.OptionType.SYSTEM,
      ExecConstants.ADAPTIVE_HASH.getOptionName(),
      ExecConstants.ADAPTIVE_HASH.getDefault().getBoolVal()));
  }

  @After
  public void cleanupGenerator() throws Exception {
    AutoCloseables.close(generator);
  }

  @Test
  public void testSinglePartitionNoOOBMessage() throws Exception {
    test(1, 2000, false);
  }

  @Test
  public void testSinglePartitionWithOOBMessageSingleBatch() throws Exception {
    int expectedRows = 2000;
    int partitionCount = 1;
    test(partitionCount, expectedRows, true);
  }

  @Test
  public void testSinglePartitionWithOOBMessageMultipleBatches() throws Exception {
    int expectedRows = 20000;
    int partitionCount = 1;
    test(partitionCount, expectedRows, true);
  }

  @Test
  public void testTwoPartitionsNoOOBMessage() throws Exception {
    test(2, 2001, false);
  }

  @Test
  public void testTwoPartitionsWithOOBMessageSingleBatch() throws Exception {
    int expectedRows = 2001;
    int partitionCount = 2;
    test(partitionCount, expectedRows, true, false);
  }

  @Test
  public void testTwoPartitionsWithOOBMessageMultipleBatches() throws Exception {
    int expectedRows = 20000;
    int partitionCount = 2;
    test(partitionCount, expectedRows, true, false);
  }

  @Test
  public void testThreePartitionsWithOOBMessageSingleBatch() throws Exception {
    int expectedRows = 2001;
    int partitionCount = 3;
    test(partitionCount, expectedRows, true);
  }

  @Test
  public void testThreePartitionsWithOOBMessageMultipleBatches() throws Exception {
    int expectedRows = 20000;
    int partitionCount = 3;
    test(partitionCount, expectedRows, true);
  }

  @Test
  public void testManyPartitionsWithOOBMessageNoRRDistributionSingleBatch() throws Exception {
    int expectedRows = 2001;
    int partitionCount = 30;
    test(partitionCount, expectedRows, true, false);
  }

  @Test
  public void testManyPartitionsWithOOBMessageNoRRDistributionMultipleBatches() throws Exception {
    int expectedRows = 20000;
    int partitionCount = 30;
    test(partitionCount, expectedRows, true, false);
  }

  private void test(int partitionCount, int expectedRows, boolean createMsg) throws Exception {
    test(partitionCount, expectedRows, createMsg, createMsg);
  }

  private void test(int partitionCount, int expectedRows, boolean createMsg, boolean expectRRDistribution) throws Exception {
    generator = new CustomGenerator(expectedRows, getTestAllocator(), partitionCount);

    HashPartitionSender sender = new HashPartitionSender(PROPS, generator.getSchema(), null,
      1, getIndexEndpoints(), f(CustomGenerator.ID.getName()), true);

    final int[] rowCountPerFragment = new int[NUM_FRAGMENTS];

    final AccountingExecTunnel tunnel = mock(AccountingExecTunnel.class);
    doAnswer(new Answer<Void>(){
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final FragmentWritableBatch batch = (FragmentWritableBatch) invocation.getArguments()[0];
        for (int fragId : batch.getHeader().getReceivingMinorFragmentIdList()) {
          rowCountPerFragment[fragId] += batch.getRecordCount();
        }
        for(ByteBuf b : batch.getBuffers()){
          b.release();
        }
        return null;
      }}).when(tunnel).sendRecordBatch(any(FragmentWritableBatch.class), any());

    final TunnelProvider provider = mock(TunnelProvider.class);
    when(provider.getExecTunnel(any(NodeEndpoint.class))).thenReturn(tunnel);

    AdaptiveVectorizedPartitionSenderOperator op = newOperator(AdaptiveVectorizedPartitionSenderOperator.class, sender, DEFAULT_BATCH,
      new EndpointsIndex(getEndpoints()), provider);

    // force to do dop adjustment
    op.setForceAdjustDop(true);
    op.setup(generator.getOutput());
    op.getOperatorContext().getStats().startProcessing();

    int count;
    boolean msgProcessed = false;
    OutOfBandMessage msg = null;
    if (createMsg) {
      msg = createPartitionCountsMessage(expectedRows, partitionCount);
    }
    while(op.getState() != TerminalOperator.State.DONE && (count = generator.next(DEFAULT_BATCH)) != 0){
      assertState(op, TerminalOperator.State.CAN_CONSUME);
      // process oob message after the batch that exceeds the threshold count
      if (msg != null && !msgProcessed) {
        op.workOnOOB(msg);
        msgProcessed = true;
      }
      op.consumeData(count);
    }

    op.noMoreToConsume();

    int sentRowCount = 0;
    assertEquals(8, VectorizedPartitionSenderOperator.PARTITION_MULTIPLE ); // Min/Max computed for 8 partitions. Higher multiples have tighter bounds, and vice versa
    for (int f = 0; f < NUM_FRAGMENTS; f++) {
      if (msg != null && expectRRDistribution) {
        // rows are evenly distributed to all receivers (in Round-Robin fashion)
        assertTrue(Math.abs(rowCountPerFragment[f] - expectedRows / NUM_FRAGMENTS) < 3);
      } else {
        if (f < partitionCount) {
          int slices = partitionCount < NUM_FRAGMENTS ? partitionCount : NUM_FRAGMENTS;
          boolean shouldPlusOne = f < (expectedRows % slices);
          // only receivers whose index is smaller than partitionCount get rows
          assertEquals(expectedRows / slices + (shouldPlusOne ? 1 : 0),
            rowCountPerFragment[f]);
        } else {
          // only receivers whose index is smaller than partitionCount get rows
          assertEquals(0, rowCountPerFragment[f]);
        }
      }
      sentRowCount += rowCountPerFragment[f];
    }
    assertEquals(expectedRows, sentRowCount);
  }

  public List<MinorFragmentIndexEndpoint> getIndexEndpoints() {
    List<MinorFragmentIndexEndpoint> l = new ArrayList<>();
    for (int i = 0; i < NUM_FRAGMENTS; i++) {
      l.add(MinorFragmentIndexEndpoint.newBuilder().setMinorFragmentId(i).setEndpointIndex(0).build());
    }
    return l;
  }

  public List<NodeEndpoint> getEndpoints() {
    List<NodeEndpoint> l = new ArrayList<>();
    for (int i = 0; i < NUM_FRAGMENTS; i++) {
      l.add(NodeEndpoint.newBuilder().setAddress(String.format("a_%d", i)).setFabricPort(1).build());
    }
    return l;
  }

  private OutOfBandMessage createPartitionCountsMessage(int totalRows, int partitions) {
    List<Pair<Integer, Long>> partitionCounts = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      boolean shouldPlusOne = (i < (totalRows % partitions));
      partitionCounts.add(new Pair<>(i, (long)totalRows / partitions + (shouldPlusOne ? 1 : 0)));
    }

    ExecProtos.HashDistributionValueCounts.Builder partitionValueCountsBuilder = ExecProtos.HashDistributionValueCounts.newBuilder();
    long totalCount = 0;
    for (Pair<Integer, Long> partitionCount : partitionCounts) {
      ExecProtos.HashDistributionValueCount partitionValueCount = ExecProtos.HashDistributionValueCount.newBuilder()
        .setHashDistributionKey(partitionCount.first)
        .setCount(partitionCount.second)
        .build();
      partitionValueCountsBuilder.addHashDistributionValueCounts(partitionValueCount);
      totalCount += partitionCount.second;
    }

    partitionValueCountsBuilder.setTotalSeenRecords(totalCount);
    partitionValueCountsBuilder.setUniqueValueCount(partitions);
    OutOfBandMessage.Payload payload = new OutOfBandMessage.Payload(partitionValueCountsBuilder.build());
    return new OutOfBandMessage(
      null,
      0,
      ImmutableList.of(0),
      0,
      0,
      payload,
      false);
  }
}
