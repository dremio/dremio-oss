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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.ExpressionSplitCache;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Sender;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.FragmentAssignment;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.server.NodeDebugContextProvider;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.FragmentStats;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.heap.HeapLowMemController;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.spill.SpillService;
import com.google.common.base.Preconditions;

class OperatorContextCreator implements OperatorContext.Creator, AutoCloseable {

  private final List<AutoCloseable> operatorContexts = new ArrayList<>();
  private final FragmentStats stats;
  private final BufferAllocator allocator;
  private BufferAllocator fragmentOutputAllocator;
  private final CodeCompiler compiler;
  private final SabotConfig config;
  private final DremioConfig dremioConfig;
  private final FragmentHandle handle;
  private final ExecutionControls executionControls;
  private final FunctionLookupContext funcRegistry;
  private final FunctionLookupContext decimalFuncRegistry;
  private final NamespaceService namespaceService;
  private final OptionManager options;
  private final FragmentExecutorBuilder fragmentExecutorBuilder;
  private final ExecutorService executor;
  private final SpillService spillService;
  private final ContextInformation contextInformation;
  private final NodeDebugContextProvider nodeDebugContextProvider;
  private final TunnelProvider tunnelProvider;
  private final List<FragmentAssignment> assignments;
  private final EndpointsIndex endpointsIndex;
  private Provider<CoordinationProtos.NodeEndpoint> nodeEndpointProvider;
  private final List<CoordExecRPC.MajorFragmentAssignment> extFragmentAssignments;
  private List<MinorFragmentEndpoint> minorFragmentEndpoints;
  private final ExpressionSplitCache expressionSplitCache;
  private final HeapLowMemController heapLowMemController;

  public OperatorContextCreator(FragmentStats stats, BufferAllocator allocator, CodeCompiler compiler,
                                SabotConfig config, DremioConfig dremioConfig, FragmentHandle handle, ExecutionControls executionControls,
                                FunctionLookupContext funcRegistry, FunctionLookupContext decimalFuncRegistry,
                                NamespaceService namespaceService, OptionManager options, FragmentExecutorBuilder fragmentExecutorBuilder,
                                ExecutorService executor, SpillService spillService, ContextInformation contextInformation,
                                NodeDebugContextProvider nodeDebugContextProvider, TunnelProvider tunnelProvider,
                                List<FragmentAssignment> assignments, EndpointsIndex endpointsIndex,
                                Provider<CoordinationProtos.NodeEndpoint> nodeEndpointProvider,
                                List<CoordExecRPC.MajorFragmentAssignment> extFragmentAssignments, ExpressionSplitCache expressionSplitCache,
                                HeapLowMemController heapLowMemController) {
    super();
    this.stats = stats;
    this.allocator = allocator;
    this.dremioConfig = dremioConfig;
    this.nodeEndpointProvider = nodeEndpointProvider;
    this.fragmentOutputAllocator = null;
    this.compiler = compiler;
    this.config = config;
    this.handle = handle;
    this.executionControls = executionControls;
    this.funcRegistry = funcRegistry;
    this.decimalFuncRegistry = decimalFuncRegistry;
    this.namespaceService = namespaceService;
    this.options = options;
    this.fragmentExecutorBuilder = fragmentExecutorBuilder;
    this.executor = executor;
    this.spillService = spillService;
    this.contextInformation = contextInformation;
    this.nodeDebugContextProvider = nodeDebugContextProvider;
    this.tunnelProvider = tunnelProvider;
    this.assignments = assignments;
    this.endpointsIndex = endpointsIndex;
    this.extFragmentAssignments = extFragmentAssignments;
    this.expressionSplitCache = expressionSplitCache;
    this.heapLowMemController = heapLowMemController;
  }

  public void setFragmentOutputAllocator(BufferAllocator fragmentOutputAllocator) {
    Preconditions.checkState(this.fragmentOutputAllocator == null);
    this.fragmentOutputAllocator = fragmentOutputAllocator;
  }

  public void setMinorFragmentEndpointsFromRootSender(PhysicalOperator root) {
    if (root instanceof Sender) {
      this.minorFragmentEndpoints = ((Sender)root).getDestinations(this.endpointsIndex);
    }
  }

  @Override
  public OperatorContext newOperatorContext(PhysicalOperator popConfig) throws Exception {
    Preconditions.checkState(this.fragmentOutputAllocator != null);
    final String allocatorName = String.format("op:%s:%d:%s",
      QueryIdHelper.getFragmentId(handle),
      popConfig.getProps().getLocalOperatorId(),
      popConfig.getClass().getSimpleName());

    long memReserve = popConfig.getProps().getMemReserve();
    long memLimit = popConfig.getProps().getMemLimit();
    if (options.getOption(ExecConstants.ENABLE_SPILLABLE_OPERATORS)) {
      memReserve = 0;
      memLimit = Long.MAX_VALUE;
    }

    final BufferAllocator operatorAllocator =
      allocator.newChildAllocator(allocatorName, memReserve, memLimit);
    try (RollbackCloseable closeable = AutoCloseables.rollbackable(operatorAllocator)) {
      final OpProfileDef def = new OpProfileDef(popConfig.getProps().getLocalOperatorId(), popConfig.getOperatorType(), OperatorContext.getChildCount(popConfig), popConfig.getOperatorSubType());
      final OperatorStats stats = this.stats.newOperatorStats(def, operatorAllocator);
      FunctionLookupContext functionLookupContext = funcRegistry;
      if (options.getOption(PlannerSettings.ENABLE_DECIMAL_V2)) {
        functionLookupContext = decimalFuncRegistry;
      }
      OperatorContextImpl context = new OperatorContextImpl(
        config,
        dremioConfig,
        handle,
        popConfig,
        operatorAllocator,
        fragmentOutputAllocator,
        compiler,
        stats,
        executionControls,
        fragmentExecutorBuilder,
        executor,
        functionLookupContext,
        contextInformation,
        options,
        spillService,
        nodeDebugContextProvider,
        popConfig.getProps().getTargetBatchSize(),
        tunnelProvider,
        assignments,
        extFragmentAssignments,
        nodeEndpointProvider,
        endpointsIndex,
        minorFragmentEndpoints,
        expressionSplitCache,
        heapLowMemController);
      operatorContexts.add(context);
      closeable.commit();
      return context;
    }
  }

  @Override
  public void close() throws Exception {
    Collections.reverse(operatorContexts);
    AutoCloseables.close(operatorContexts);
  }
}
