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
package com.dremio.sabot.sender.broadcast;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.exec.physical.config.BroadcastSender;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sender.broadcast.BroadcastOperator;

import io.airlift.tpch.GenerationDefinition.TpchTable;
import io.airlift.tpch.TpchGenerator;
import io.netty.buffer.ByteBuf;

public class TestBroadcastSender extends BaseTestOperator {

  @Test
  public void checkMemoryLeak() throws Exception {
    EndpointsIndex endpointsIndex = new EndpointsIndex(
      Arrays.asList(
        NodeEndpoint.newBuilder().setAddress("a").setFabricPort(1).build(),
        NodeEndpoint.newBuilder().setAddress("b").setFabricPort(2).build()
      )
    );

    BroadcastSender sender = new BroadcastSender(PROPS, getSchema(), null, 1,
        Arrays.asList(
          MinorFragmentIndexEndpoint.newBuilder().setMinorFragmentId(1).setEndpointIndex(0).build(),
          MinorFragmentIndexEndpoint.newBuilder().setMinorFragmentId(2).setEndpointIndex(1).build()
        )
    );

    final AccountingExecTunnel tunnel = mock(AccountingExecTunnel.class);
    doAnswer(new Answer<Void>(){

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final FragmentWritableBatch batch = (FragmentWritableBatch) invocation.getArguments()[0];
        for(ByteBuf b : batch.getBuffers()){
          b.release();
        }
        return null;
      }}).when(tunnel).sendRecordBatch(any(FragmentWritableBatch.class));

    final TunnelProvider provider = mock(TunnelProvider.class);
    when(provider.getExecTunnel(any(NodeEndpoint.class))).thenReturn(tunnel);


    try(BroadcastOperator op = newOperator(BroadcastOperator.class, sender, DEFAULT_BATCH, endpointsIndex, provider);
        TpchGenerator g = TpchGenerator.singleGenerator(TpchTable.NATION, 0.1, getTestAllocator());){
      op.setup(g.getOutput());
      g.next(DEFAULT_BATCH);
      op.consumeData(g.next(DEFAULT_BATCH));
      op.noMoreToConsume();

    }
  }

  public BatchSchema getSchema() {
    SchemaBuilder builder = BatchSchema.newBuilder()
      .addField(new Field("n_nationKey", new FieldType(true, MinorType.BIGINT.getType(), null), null))
      .addField(new Field("n_name", new FieldType(true, MinorType.VARCHAR.getType(), null), null))
      .addField(new Field("n_regionKey", new FieldType(true, MinorType.BIGINT.getType(), null), null))
      .addField(new Field("n_comment", new FieldType(true, MinorType.VARCHAR.getType(), null), null));
    return builder.build();
  }
}
