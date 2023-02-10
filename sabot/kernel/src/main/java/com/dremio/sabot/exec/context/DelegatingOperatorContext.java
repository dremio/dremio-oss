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

import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.inject.Provider;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.config.DremioConfig;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.ExpressionSplitCache;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.server.NodeDebugContextProvider;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.heap.HeapLowMemController;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.filter.VectorContainerWithSV;
import com.dremio.service.spill.SpillService;
import com.google.common.base.Preconditions;

public class DelegatingOperatorContext extends OperatorContext {

  private final OperatorContext delegate;

  public DelegatingOperatorContext(OperatorContext delegate) {
    this.delegate = Preconditions.checkNotNull(delegate);
  }

  @Override
  public SabotConfig getConfig() {
    return delegate.getConfig();
  }

  @Override
  public DremioConfig getDremioConfig() {
    return delegate.getDremioConfig();
  }

  @Override
  public ArrowBuf replace(ArrowBuf old, int newSize) {
    return delegate.replace(old, newSize);
  }

  @Override
  public ArrowBuf getManagedBuffer() {
    return delegate.getManagedBuffer();
  }

  @Override
  public ArrowBuf getManagedBuffer(int size) {
    return delegate.getManagedBuffer(size);
  }

  @Override
  public BufferAllocator getAllocator() {
    return delegate.getAllocator();
  }

  @Override
  public BufferAllocator getFragmentOutputAllocator() {
    return delegate.getFragmentOutputAllocator();
  }

  @Override
  public BufferManager getBufferManager() {
    return delegate.getBufferManager();
  }

  @Override
  public VectorContainer createOutputVectorContainer() {
    return delegate.createOutputVectorContainer();
  }

  @Override
  public VectorContainer createOutputVectorContainer(Schema schema) {
    return delegate.createOutputVectorContainer(schema);
  }

  @Override
  public VectorContainerWithSV createOutputVectorContainerWithSV() {
    return delegate.createOutputVectorContainerWithSV();
  }

  @Override
  public VectorContainerWithSV createOutputVectorContainerWithSV(SelectionVector2 incomingSv) {
    return delegate.createOutputVectorContainerWithSV(incomingSv);
  }

  @Override
  public OperatorStats getStats() {
    return delegate.getStats();
  }

  @Override
  public ExecutionControls getExecutionControls() {
    return delegate.getExecutionControls();
  }

  @Override
  public OptionManager getOptions() {
    return delegate.getOptions();
  }

  @Override
  public int getTargetBatchSize() {
    return delegate.getTargetBatchSize();
  }

  @Override
  public HeapLowMemController getHeapLowMemController() {
    return delegate.getHeapLowMemController();
  }

  @Override
  public ClassProducer getClassProducer() {
    return delegate.getClassProducer();
  }

  @Override
  public FunctionContext getFunctionContext() {
    return delegate.getFunctionContext();
  }

  @Override
  public ExecProtos.FragmentHandle getFragmentHandle() {
    return delegate.getFragmentHandle();
  }

  @Override
  public ExecutorService getExecutor() {
    return delegate.getExecutor();
  }

  @Override
  public UserBitShared.QueryId getQueryIdForLocalQuery() {
    return delegate.getQueryIdForLocalQuery();
  }

  @Override
  public LogicalPlanPersistence getLpPersistence() {
    return delegate.getLpPersistence();
  }

  @Override
  public CoordinationProtos.NodeEndpoint getNodeEndPoint() {
    return delegate.getNodeEndPoint();
  }

  @Override
  public void startFragmentOnLocal(PlanFragmentFull planFragmentFull) {
    delegate.startFragmentOnLocal(planFragmentFull);
  }

  @Override
  public NodeDebugContextProvider getNodeDebugContextProvider() {
    return delegate.getNodeDebugContextProvider();
  }

  @Override
  public SpillService getSpillService() {
    return delegate.getSpillService();
  }

  @Override
  public TunnelProvider getTunnelProvider() {
    return delegate.getTunnelProvider();
  }

  @Override
  public List<CoordExecRPC.FragmentAssignment> getAssignments() {
    return delegate.getAssignments();
  }

  @Override
  public EndpointsIndex getEndpointsIndex() {
    return delegate.getEndpointsIndex();
  }

  @Override
  public CoordExecRPC.MajorFragmentAssignment getExtMajorFragmentAssignments(int extMajorFragment) {
    return delegate.getExtMajorFragmentAssignments(extMajorFragment);
  }

  @Override
  public List<MinorFragmentEndpoint> getMinorFragmentEndpoints() {
    return delegate.getMinorFragmentEndpoints();
  }

  @Override
  public ExpressionSplitCache getExpressionSplitCache() {
    return delegate.getExpressionSplitCache();
  }

  @Override
  public Provider<CoordinationProtos.NodeEndpoint> getNodeEndpointProvider() {
    return delegate.getNodeEndpointProvider();
  }
}
