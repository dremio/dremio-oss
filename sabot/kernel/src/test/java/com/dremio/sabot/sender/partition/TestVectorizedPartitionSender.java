/*
 * Copyright (C) 2017 Dremio Corporation
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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.MinorFragmentEndpoint;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.CustomGenerator;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sender.partition.vectorized.VectorizedPartitionSenderOperator;

import io.netty.buffer.ByteBuf;

/**
 *
 */
public class TestVectorizedPartitionSender extends BaseTestOperator {
  final static int NUM_FRAGMENTS = 3;
  final static int NUM_ROWS = 200;
  final static int MIN_NUM_PER_FRAGMENT = 62;
  final static int MAX_NUM_PER_FRAGMENT = 69;
  CustomGenerator generator;

  @Before
  public void setupGenerator() {
    generator = new CustomGenerator(NUM_ROWS, getTestAllocator());
  }

  @After
  public void cleanupGenerator() throws Exception {
    AutoCloseables.close(generator);
  }

  @Test
  public void testNumPartitions() throws Exception {
    HashPartitionSender sender = new HashPartitionSender(1, null, f(CustomGenerator.ID.getName()),
      getEndpoints(),
      generator.getSchema()
    );

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
      }}).when(tunnel).sendRecordBatch(any(FragmentWritableBatch.class));

    final TunnelProvider provider = mock(TunnelProvider.class);
    when(provider.getExecTunnel(any(NodeEndpoint.class))).thenReturn(tunnel);

    VectorizedPartitionSenderOperator op = newOperator(VectorizedPartitionSenderOperator.class, sender, DEFAULT_BATCH, provider);
    op.setup(generator.getOutput());
    op.getOperatorContext().getStats().startProcessing();
    op.consumeData(generator.next(DEFAULT_BATCH));
    op.noMoreToConsume();
    int sum = 0;
    assertEquals(8, VectorizedPartitionSenderOperator.PARTITION_MULTIPLE ); // Min/Max computed for 8 partitions. Higher multiples have tighter bounds, and vice versa
    for (int i = 0; i < NUM_FRAGMENTS; i++) {
      assert (rowCountPerFragment[i] >= MIN_NUM_PER_FRAGMENT);
      assert (rowCountPerFragment[i] <= MAX_NUM_PER_FRAGMENT);
      sum += rowCountPerFragment[i];
    }
    assertEquals(NUM_ROWS, sum);
  }

  public List<MinorFragmentEndpoint> getEndpoints() {
    List<MinorFragmentEndpoint> l = new ArrayList<MinorFragmentEndpoint>();
    for (int i = 0; i < NUM_FRAGMENTS; i++) {
      l.add(new MinorFragmentEndpoint(i, NodeEndpoint.newBuilder().setAddress(String.format("a_%d", i)).setFabricPort(1).build()));
    }
    return l;
  }

}
