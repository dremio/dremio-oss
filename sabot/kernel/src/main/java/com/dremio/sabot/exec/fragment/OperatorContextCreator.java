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
package com.dremio.sabot.exec.fragment;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.common.util.Numbers;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.NodeDebugContextProvider;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.FragmentStats;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.namespace.NamespaceService;

class OperatorContextCreator implements OperatorContext.Creator, AutoCloseable {

  private final List<AutoCloseable> operatorContexts = new ArrayList<>();
  private final FragmentStats stats;
  private final BufferAllocator allocator;
  private final CodeCompiler compiler;
  private final SabotConfig config;
  private final FragmentHandle handle;
  private final ExecutionControls executionControls;
  private final FunctionLookupContext funcRegistry;
  private final NamespaceService namespaceService;
  private final OptionManager options;
  private final ExecutorService executor;
  private final ContextInformation contextInformation;
  private final NodeDebugContextProvider nodeDebugContextProvider;

  public OperatorContextCreator(FragmentStats stats, BufferAllocator allocator, CodeCompiler compiler,
                                SabotConfig config, FragmentHandle handle, ExecutionControls executionControls,
                                FunctionLookupContext funcRegistry, NamespaceService namespaceService, OptionManager options,
                                ExecutorService executor, ContextInformation contextInformation,
                                NodeDebugContextProvider nodeDebugContextProvider) {
    super();
    this.stats = stats;
    this.allocator = allocator;
    this.compiler = compiler;
    this.config = config;
    this.handle = handle;
    this.executionControls = executionControls;
    this.funcRegistry = funcRegistry;
    this.namespaceService = namespaceService;
    this.options = options;
    this.executor = executor;
    this.contextInformation = contextInformation;
    this.nodeDebugContextProvider = nodeDebugContextProvider;
  }

  @Override
  public OperatorContext newOperatorContext(PhysicalOperator popConfig) throws Exception {

    final String allocatorName = String.format("op:%s:%d:%s",
      QueryIdHelper.getFragmentId(handle),
      popConfig.getOperatorId(),
      popConfig.getClass().getSimpleName());

    final BufferAllocator operatorAllocator =
      allocator.newChildAllocator(allocatorName, popConfig.getInitialAllocation(), popConfig.getMaxAllocation());
    try (RollbackCloseable closeable = AutoCloseables.rollbackable(operatorAllocator)) {
      final OpProfileDef def = new OpProfileDef(popConfig.getOperatorId(), popConfig.getOperatorType(), OperatorContext.getChildCount(popConfig));
      final OperatorStats stats = this.stats.newOperatorStats(def, operatorAllocator);
      OperatorContextImpl context = new OperatorContextImpl(
        config,
        handle,
        popConfig,
        operatorAllocator,
        compiler,
        stats,
        executionControls,
        executor,
        funcRegistry,
        contextInformation,
        options,
        namespaceService,
        nodeDebugContextProvider,
        calculateTargetRecordSize(popConfig));
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

  private int calculateTargetRecordSize(PhysicalOperator popConfig) {
    final BatchSchema schema = checkNotNull(popConfig.getSchema(funcRegistry), "An operator cannot have null schema.");
    final int listSizeEstimate = (int) options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
    final int varFieldSizeEstimate = (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
    final int estimatedRecordSize = schema.estimateRecordSize(listSizeEstimate, varFieldSizeEstimate);

    if (estimatedRecordSize == 0) {
      // If the estimated row size is zero (possible when schema is not known initially for queries containing
      // convert_from), fall back to max size
      return (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);
    }

    final int minTargetBatchCount = (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MIN);
    final int maxTargetBatchCount = (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);

    final int maxBatchSizeBytes = (int) options.getOption(ExecConstants.TARGET_BATCH_SIZE_BYTES);

    final int targetBatchSize = max(minTargetBatchCount,
      min(maxTargetBatchCount, maxBatchSizeBytes / estimatedRecordSize));

    // TODO: may be we should get the closest 2^x - 1
    return Math.max(1, Numbers.nextPowerOfTwo(targetBatchSize) - 1);
  }

}
