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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.config.SabotConfig;
import com.dremio.config.DremioConfig;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.sabot.exec.fragment.FragmentWorkQueue;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.spill.DefaultSpillServiceOptions;
import com.dremio.service.spill.SpillService;
import com.dremio.service.spill.SpillServiceImpl;

public class TestAbstractDataCollector {

  @Test
  public void testReserveMemory() {
    SharedResourceGroup resourceGroup = mock(SharedResourceGroup.class);
    SabotConfig config = mock(SabotConfig.class);
    FragmentWorkQueue workQueue = mock(FragmentWorkQueue.class);
    TunnelProvider tunnelProvider = mock(TunnelProvider.class);

    EndpointsIndex endpointsIndex = new EndpointsIndex(
      Arrays.asList(
        NodeEndpoint.newBuilder().setAddress("localhost").setFabricPort(12345).build(),
        NodeEndpoint.newBuilder().setAddress("localhost").setFabricPort(12345).build()
      )
    );
    List<CoordExecRPC.MinorFragmentIndexEndpoint> list =
      Arrays.asList(
        MinorFragmentIndexEndpoint.newBuilder().setEndpointIndex(0).setMinorFragmentId(0).build(),
        MinorFragmentIndexEndpoint.newBuilder().setEndpointIndex(0).setMinorFragmentId(0).build()
      );

    CoordExecRPC.Collector collector = CoordExecRPC.Collector.newBuilder()
      .setIsSpooling(true)
      .setOppositeMajorFragmentId(3)
      .setSupportsOutOfOrder(true)
      .addAllIncomingMinorFragmentIndex(list)
      .build();
    ExecProtos.FragmentHandle handle = ExecProtos.FragmentHandle.newBuilder().setMajorFragmentId(2323).setMinorFragmentId(234234).build();
    BufferAllocator allocator = new RootAllocator(2000000);
    boolean outOfMemory = false;
    final SchedulerService schedulerService = Mockito.mock(SchedulerService.class);
    final SpillService spillService = new SpillServiceImpl(DremioConfig.create(null, config), new DefaultSpillServiceOptions(),
                                                           new Provider<SchedulerService>() {
                                                             @Override
                                                             public SchedulerService get() {
                                                               return schedulerService;
                                                             }
                                                           });
    try {
      AbstractDataCollector dataCollector = new AbstractDataCollector(resourceGroup, true,
        collector, 10240, allocator, config, handle, workQueue, tunnelProvider, spillService, endpointsIndex) {
        @Override
        protected RawBatchBuffer getBuffer(int minorFragmentId) {
          return null;
        }
      };
    } catch (OutOfMemoryException e) {
      /* Each minor fragment will reserve an arrow buffer with 1024*1024 size. 2*1024*1024 memory is required
       * because there are two minor fragments. Allocator is limited to 2000000, so OutOfMemoryException is
       * expected when it tries to allocate the second arrow buffer, but it should not cause memory leak when
       * allocator is closed.
       */
      // The first allocation should succeed
      assertEquals(allocator.getPeakMemoryAllocation(), 1024*1024);

      outOfMemory = true;
    }

    // Verify that it runs out of memory for second allocation.
    assertTrue(outOfMemory);
    /* We are verifying that the first allocated arrow buffer should be released if the second allocation fail,
     * so no memory leak report is expected.
     */
    allocator.close();
  }
}
