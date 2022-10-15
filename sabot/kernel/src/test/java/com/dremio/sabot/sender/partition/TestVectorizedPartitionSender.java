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
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
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
  static final int NUM_FRAGMENTS = 3;
  static final int NUM_ROWS = 200;
  static final int MIN_NUM_PER_FRAGMENT = 62;
  static final int MAX_NUM_PER_FRAGMENT = 69;
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
    HashPartitionSender sender = new HashPartitionSender(PROPS, generator.getSchema(), null, 1, getIndexEndpoints(), f(CustomGenerator.ID.getName()));

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

    VectorizedPartitionSenderOperator op = newOperator(VectorizedPartitionSenderOperator.class, sender, DEFAULT_BATCH,
      new EndpointsIndex(getEndpoints()), provider);
    op.setup(generator.getOutput());
    op.getOperatorContext().getStats().startProcessing();
    op.consumeData(generator.next(DEFAULT_BATCH));
    op.noMoreToConsume();
    int sum = 0;
    assertEquals(8, VectorizedPartitionSenderOperator.PARTITION_MULTIPLE ); // Min/Max computed for 8 partitions. Higher multiples have tighter bounds, and vice versa
    for (int i = 0; i < NUM_FRAGMENTS; i++) {
      assertTrue(rowCountPerFragment[i] >= MIN_NUM_PER_FRAGMENT);
      assertTrue(rowCountPerFragment[i] <= MAX_NUM_PER_FRAGMENT);
      sum += rowCountPerFragment[i];
    }
    assertEquals(NUM_ROWS, sum);
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

}
