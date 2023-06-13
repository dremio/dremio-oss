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
package com.dremio.sabot.exec.fragment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;

import com.dremio.common.DeferredException;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.fragment.CachedFragmentReader;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.fragment.PlanFragmentsIndex;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionManager;
import com.dremio.sabot.driver.OperatorCreatorRegistry;
import com.dremio.sabot.driver.Pipeline;
import com.dremio.sabot.exec.EventProvider;
import com.dremio.sabot.exec.FragmentTicket;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.FragmentStats;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.task.AsyncTask;
import com.dremio.sabot.threads.sharedres.SharedResourceManager;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.spill.SpillService;
import com.google.common.collect.Lists;

/**
 * Tests for {@link FragmentExecutor}
 */
public class TestFragmentExecutor {
    private BufferAllocator allocator = new RootAllocator();

    @Test
    public void testWorkOnOOBAfterSetup() throws Exception {
        try(ArrowBuf messageBuf = allocator.buffer(10L)) {
            FragmentExecutor exec = spy(getTestFragmentExecutor());
            doNothing().when(exec).setupExecution();
            ArrowBuf[] bufs = new ArrowBuf[]{messageBuf};
            ExecProtos.RuntimeFilter filter = ExecProtos.RuntimeFilter.newBuilder().setProbeScanOperatorId(101).setProbeScanMajorFragmentId(1)
                    .setPartitionColumnFilter(ExecProtos.CompositeColumnFilter.newBuilder().setSizeBytes(64).addAllColumns(Lists.newArrayList("col1")).build())
                    .build();
            OutOfBandMessage oobm = new OutOfBandMessage(UserBitShared.QueryId.newBuilder().build(), 1, Collections.EMPTY_LIST, 101, 1, 1,
                    101, new OutOfBandMessage.Payload(filter), bufs, false);

            FragmentExecutor.FragmentExecutorListener listener = exec.getListener();
            listener.handle(oobm);
            Pipeline pipeline = mock(Pipeline.class);
            listener.overridePipeline(pipeline);
            AsyncTask asyncTask = exec.asAsyncTask();
            asyncTask.run();
            verify(exec).setupExecution();
            exec.transitionToRunning();
            exec.overrideIsSetup(true);
            listener.overrideIsSetup(true);
            asyncTask.run();
            verify(pipeline).workOnOOB(any(OutOfBandMessage.class));
        }
    }

    private FragmentExecutor getTestFragmentExecutor() {
        FragmentStatusReporter statusReporter = mock(FragmentStatusReporter.class);
        SabotConfig config = SabotConfig.create();
        ExecutionControls executionControls = mock(ExecutionControls.class);
        CoordExecRPC.PlanFragmentMajor major = CoordExecRPC.PlanFragmentMajor.newBuilder().build();
        CoordExecRPC.PlanFragmentMinor minor = CoordExecRPC.PlanFragmentMinor.newBuilder().build();
        PlanFragmentFull fragment = new PlanFragmentFull(major, minor);
        ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        EndpointsIndex endpointsIndex = mock(EndpointsIndex.class);
        CachedFragmentReader reader = mock(CachedFragmentReader.class);
        PlanFragmentsIndex planFragmentsIndex = mock(PlanFragmentsIndex.class);
        when(planFragmentsIndex.getEndpointsIndex()).thenReturn(endpointsIndex);
        when(reader.getPlanFragmentsIndex()).thenReturn(planFragmentsIndex);
        SharedResourceManager sharedResources = SharedResourceManager.newBuilder().addGroup("pipeline").addGroup("work-queue").build();
        OperatorCreatorRegistry opCreator = mock(OperatorCreatorRegistry.class);
        ContextInformation contextInfo = mock(ContextInformation.class);
        OperatorContextCreator contextCreator = mock(OperatorContextCreator.class);
        FunctionLookupContext functionLookupContext = mock(FunctionLookupContext.class);
        FunctionLookupContext decimalFunctionLookupContext = mock(FunctionLookupContext.class);
        TunnelProvider tunnelProvider = mock(TunnelProvider.class);
        FlushableSendingAccountor flushable = mock(FlushableSendingAccountor.class);
        OptionManager fragmentOptions = mock(OptionManager.class);
        FragmentStats stats = mock(FragmentStats.class);
        final FragmentTicket ticket = mock(FragmentTicket.class);
        final CatalogService sources = mock(CatalogService.class);
        DeferredException exception = mock(DeferredException.class);
        EventProvider eventProvider = mock(EventProvider.class);
        SpillService spillService = mock(SpillService.class);

        return new FragmentExecutor(
                statusReporter,
                config,
                executionControls,
                fragment,
                1,
                null,
                clusterCoordinator,
                reader,
                sharedResources,
                opCreator,
                allocator,
                contextInfo,
                contextCreator,
                functionLookupContext,
                decimalFunctionLookupContext,
                null,
                tunnelProvider,
                flushable,
                fragmentOptions,
                stats,
                ticket,
                sources,
                exception,
                eventProvider,
                spillService
                );
    }
}
