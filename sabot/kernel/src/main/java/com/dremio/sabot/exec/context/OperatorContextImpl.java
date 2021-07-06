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
package com.dremio.sabot.exec.context;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.ClassProducerImpl;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.proto.CoordExecRPC.FragmentAssignment;
import com.dremio.exec.proto.CoordExecRPC.MajorFragmentAssignment;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.server.NodeDebugContextProvider;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.fragment.FragmentExecutorBuilder;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.filter.VectorContainerWithSV;
import com.dremio.service.spill.SpillService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

@VisibleForTesting
public class OperatorContextImpl extends OperatorContext implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorContextImpl.class);

  private final SabotConfig config;
  private final FragmentHandle handle;
  private final BufferAllocator allocator;
  private final BufferAllocator fragmentOutputAllocator;
  private final ExecutionControls executionControls;
  private boolean closed = false;
  private final PhysicalOperator popConfig;
  private final OperatorStats stats;
  private final BufferManager manager;
  private final FragmentExecutorBuilder fragmentExecutorBuilder;
  private final ExecutorService executor;
  private final TunnelProvider tunnelProvider;
  private final List<FragmentAssignment> assignments;

  private final ClassProducer producer;
  private final OptionManager optionManager;
  private final int targetBatchSize;
  private final NodeDebugContextProvider nodeDebugContextProvider;
  private final Provider<CoordinationProtos.NodeEndpoint> nodeEndpointProvider;
  private final SpillService spillService;
  private final EndpointsIndex endpointsIndex;
  private final Map<Integer, MajorFragmentAssignment> majorFragmentAssignments;
  private final List<MinorFragmentEndpoint> minorFragmentEndpoints;

  public OperatorContextImpl(
    SabotConfig config,
    FragmentHandle handle,
    PhysicalOperator popConfig,
    BufferAllocator allocator,
    BufferAllocator fragmentOutputAllocator,
    CodeCompiler compiler,
    OperatorStats stats,
    ExecutionControls executionControls,
    FragmentExecutorBuilder fragmentExecutorBuilder,
    ExecutorService executor,
    FunctionLookupContext functions,
    ContextInformation contextInformation,
    final OptionManager optionManager,
    SpillService spillService,
    NodeDebugContextProvider nodeDebugContextProvider,
    int targetBatchSize,
    TunnelProvider tunnelProvider,
    List<FragmentAssignment> assignments,
    List<MajorFragmentAssignment> majorFragmentAssignments,
    Provider<CoordinationProtos.NodeEndpoint> nodeEndpointProvider,
    EndpointsIndex endpointsIndex,
    List<MinorFragmentEndpoint> minorFragmentEndpoints) throws OutOfMemoryException {
    this.config = config;
    this.handle = handle;
    this.allocator = allocator;
    this.fragmentOutputAllocator = fragmentOutputAllocator;
    this.popConfig = popConfig;
    this.nodeEndpointProvider = nodeEndpointProvider;

    //some unit test cases pass null optionManager
    final int bufCapacity = (optionManager != null) ?
      (int)optionManager.getOption(ExecConstants.BUF_MANAGER_CAPACITY) : (1 << 16);
    this.manager = new BufferManagerImpl(allocator, bufCapacity);

    this.stats = stats;
    this.executionControls = executionControls;
    this.fragmentExecutorBuilder = fragmentExecutorBuilder;
    this.executor = executor;
    this.optionManager = optionManager;
    this.targetBatchSize = targetBatchSize;
    this.nodeDebugContextProvider = nodeDebugContextProvider;
    this.producer = new ClassProducerImpl(new CompilationOptions(optionManager), compiler, functions, contextInformation, manager, minorFragmentEndpoints);
    this.spillService = spillService;
    this.tunnelProvider = tunnelProvider;
    this.assignments = assignments;
    this.endpointsIndex = endpointsIndex;
    this.majorFragmentAssignments = Optional.ofNullable(majorFragmentAssignments)
            .map(f -> f.stream().collect(Collectors.toMap(MajorFragmentAssignment::getMajorFragmentId, v -> v)))
            .orElse(Collections.emptyMap());
    this.minorFragmentEndpoints = minorFragmentEndpoints;
  }

  public OperatorContextImpl(
      SabotConfig config,
      BufferAllocator allocator,
      OptionManager optionManager,
      int targetBatchSize
      ) {

    this(config, null, null, allocator, allocator, null, null, null, null, null, null, null,
      optionManager, null, NodeDebugContextProvider.NOOP, targetBatchSize, null, ImmutableList.of(), ImmutableList.of(), null, null, null);
  }

  @Override
  public SabotConfig getConfig(){
    return config;
  }

  @Override
  public ArrowBuf replace(ArrowBuf old, int newSize) {
    return manager.replace(old, newSize);
  }

  @Override
  public ArrowBuf getManagedBuffer() {
    return manager.getManagedBuffer();
  }

  @Override
  public ArrowBuf getManagedBuffer(int size) {
    return manager.getManagedBuffer(size);
  }

  @Override
  public ExecutionControls getExecutionControls() {
    return executionControls;
  }

  @Override
  public ExecutorService getExecutor() {
    if (executor == null) {
      throw new UnsupportedOperationException("Operator context does not have an executor");
    }
    return executor;
  }

  @Override
  public QueryId getQueryIdForLocalQuery() {
    if (fragmentExecutorBuilder == null) {
      throw new UnsupportedOperationException("Operator context does not support generating QueryId");
    }
    return fragmentExecutorBuilder.getFragmentExecutors().getQueryIdForLocalQuery();
  }

  public LogicalPlanPersistence getLpPersistence() {
    Preconditions.checkNotNull(fragmentExecutorBuilder, "Cannot get LogicalPlanPersistence without initializing FragmentExecutorBuilder");
    return fragmentExecutorBuilder.getPlanReader().getLpPersistance();
  }

  @Override
  public CoordinationProtos.NodeEndpoint getNodeEndPoint() {
    Preconditions.checkNotNull(fragmentExecutorBuilder, "Cannot get NodeEndpoint without initializing FragmentExecutorBuilder");
    return fragmentExecutorBuilder.getNodeEndpoint();
  }

  @Override
  public void startFragmentOnLocal(PlanFragmentFull planFragmentFull) {
    if (fragmentExecutorBuilder == null) {
      throw new UnsupportedOperationException("Operator context does not support starting fragments");
    }
    fragmentExecutorBuilder.getFragmentExecutors().startFragmentOnLocal(planFragmentFull, fragmentExecutorBuilder);
  }

  @Override
  public BufferAllocator getAllocator() {
    if (allocator == null) {
      throw new UnsupportedOperationException("Operator context does not have an allocator");
    }
    return allocator;
  }

  public TunnelProvider getTunnelProvider() {
    return tunnelProvider;
  }

  public List<FragmentAssignment> getAssignments() {
    return assignments;
  }

  public EndpointsIndex getEndpointsIndex() {
    return endpointsIndex;
  }

  @Override
  public MajorFragmentAssignment getExtMajorFragmentAssignments(int extMajorFragment) {
    return majorFragmentAssignments.get(extMajorFragment);
  }

  @Override
  public Provider<CoordinationProtos.NodeEndpoint> getNodeEndpointProvider() {
    return nodeEndpointProvider;
  }

  @Override
  public VectorContainer createOutputVectorContainer() {
    return new VectorContainer(fragmentOutputAllocator);
  }

  @Override
  public VectorContainer createOutputVectorContainer(Schema schema) {
    return VectorContainer.create(fragmentOutputAllocator, schema);
  }

  @Override
  public VectorContainerWithSV createOutputVectorContainerWithSV() {
    return new VectorContainerWithSV(fragmentOutputAllocator, new SelectionVector2(fragmentOutputAllocator));
  }

  @Override
  public VectorContainerWithSV createOutputVectorContainerWithSV(SelectionVector2 incomingSv) {
    return new VectorContainerWithSV(fragmentOutputAllocator, incomingSv.clone());
  }

  @Override
  public BufferAllocator getFragmentOutputAllocator() {
    return fragmentOutputAllocator;
  }

  @Override
  public BufferManager getBufferManager() { return  manager; }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public int getTargetBatchSize() {
    return targetBatchSize;
  }

  @Override
  public void close() throws Exception {
    if (closed) {
      logger.warn("Attempted to close Operator context for {}, but context is already closed", popConfig != null ? popConfig.getClass().getName() : "<unknown>");
      return;
    }

    try{
      AutoCloseables.close(manager, allocator);
    }finally{
      closed = true;
    }
  }

  @Override
  public OperatorStats getStats() {
    return stats;
  }

  @Override
  public OptionManager getOptions() {
    return optionManager;
  }

  @Override
  public FragmentHandle getFragmentHandle() {
    return handle;
  }

  @Override
  public FunctionContext getFunctionContext() {
    return producer.getFunctionContext();
  }

  @Override
  public ClassProducer getClassProducer(){
    return producer;
  }


  @Override
  public  NodeDebugContextProvider getNodeDebugContextProvider() {
    return nodeDebugContextProvider;
  }

  @Override
  public SpillService getSpillService() {
    return spillService;
  }

  @Override
  public List<MinorFragmentEndpoint> getMinorFragmentEndpoints() {
    return minorFragmentEndpoints;
  }
}
