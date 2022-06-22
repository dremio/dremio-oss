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
package com.dremio.exec.planner.fragment;

import static com.dremio.exec.ExecConstants.TABLE_FUNCTION_WIDTH_EXPAND_UPTO_ENGINE_SIZE;
import static com.dremio.exec.ExecConstants.TABLE_FUNCTION_WIDTH_USE_ENGINE_SIZE;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.dremio.exec.physical.base.Exchange;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Store;
import com.dremio.exec.physical.config.AbstractTableFunctionPOP;
import com.dremio.exec.planner.AbstractOpWrapperVisitor;
import com.dremio.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.options.OptionManager;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Visitor to collect stats such as cost and parallelization info of operators within a fragment.
 *
 * All operators have cost associated with them, but only few type of operators such as scan,
 * store and exchanges (both sending and receiving) have parallelization info associated with them.
 */
public class StatsCollector extends AbstractOpWrapperVisitor<Void, RuntimeException> {
  private final PlanningSet planningSet;
  private final ExecutionNodeMap executionNodeMap;
  private final OptionManager options;

  public StatsCollector(PlanningSet planningSet, ExecutionNodeMap executionMap,
                        OptionManager options) {
    this.planningSet = planningSet;
    this.executionNodeMap = executionMap;
    this.options = options;
  }

  @Override
  public Void visitSendingExchange(Exchange exchange, Wrapper wrapper) throws RuntimeException {
    // Handle the sending side exchange
    final Wrapper receivingFragment = planningSet.get(wrapper.getNode().getSendingExchangePair().getNode());

    Supplier<Collection<NodeEndpoint>> receiverEndpointsSupplier = () -> {
      // List to contain the endpoints where the fragment that receive data to this fragment are running.
      List<NodeEndpoint> receiverEndpoints;
      if (receivingFragment.isEndpointsAssignmentDone()) {
        receiverEndpoints = receivingFragment.getAssignedEndpoints();
      } else {
        receiverEndpoints = Collections.emptyList();
      }
      return receiverEndpoints;
    };

    wrapper.getStats().addWidthConstraint(exchange.getSenderParallelizationWidthConstraint());
    wrapper.getStats().addEndpointAffinity(exchange.getSenderEndpointffinity(receiverEndpointsSupplier));
    return visitOp(exchange, wrapper);
  }

  @Override
  public Void visitReceivingExchange(Exchange exchange, Wrapper wrapper) throws RuntimeException {
    // Handle the receiving side Exchange

    final List<ExchangeFragmentPair> receivingExchangePairs = wrapper.getNode().getReceivingExchangePairs();

    Supplier<Collection<NodeEndpoint>> supplierEndpointsSupplier = () -> {
      // List to contain the endpoints where the fragment that send dat to this fragment are running.
      final List<NodeEndpoint> sendingEndpoints = Lists.newArrayList();

      for (ExchangeFragmentPair pair : receivingExchangePairs) {
        if (pair.getExchange() == exchange) {
          Wrapper sendingFragment = planningSet.get(pair.getNode());
          if (sendingFragment.isEndpointsAssignmentDone()) {
            sendingEndpoints.addAll(sendingFragment.getAssignedEndpoints());
          }
        }
      }
      return sendingEndpoints;
    };

    wrapper.getStats().addWidthConstraint(exchange.getReceiverParallelizationWidthConstraint());
    wrapper.getStats().addEndpointAffinity(exchange.getReceiverEndpointAffinity(supplierEndpointsSupplier));
    // no traversal since it would cross current fragment boundary.
    return null;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public Void visitGroupScan(GroupScan groupScan, Wrapper wrapper) {
    final Stats stats = wrapper.getStats();
    stats.addMaxWidth(groupScan.getMaxParallelizationWidth());
    stats.addMinWidth(groupScan.getMinParallelizationWidth());

    ImmutableList<CompleteWork> work = ImmutableList.<CompleteWork>copyOf(groupScan.getSplits(executionNodeMap));
    stats.addSplits(groupScan, work);

    return super.visitGroupScan(groupScan, wrapper);
  }

  @Override
  public Void visitStore(Store store, Wrapper wrapper) {
    wrapper.getStats().addMaxWidth(1);
    return super.visitStore(store, wrapper);
  }

  @Override
  public Void visitOp(PhysicalOperator op, Wrapper wrapper) {
    final Stats stats = wrapper.getStats();
    stats.addCost(op.getProps().getCost());
    if (wrapper.getNode().getSendingExchange() != op && op.getProps().isSingleStream()) {
      stats.addMaxWidth(1);
    }
    for (PhysicalOperator child : op) {
      child.accept(this, wrapper);
    }
    return null;
  }

  @Override
  public Void visitTableFunction(AbstractTableFunctionPOP op, Wrapper wrapper) {
    final Stats stats = wrapper.getStats();
    int finalMinWidth = 0, finalMaxWidth = 0;
    if(op.getMaxParallelizationWidth() > 0) {
      finalMaxWidth = (int)op.getMaxParallelizationWidth();
    }
    if(op.getMinParallelizationWidth() > 0) {
      finalMinWidth = (int)op.getMinParallelizationWidth();
    }
    stats.addCost(op.getProps().getCost());
    if (options.getOption(TABLE_FUNCTION_WIDTH_USE_ENGINE_SIZE)) {
      int tableFunctionMaxParallelism = (int) options.getOption(TABLE_FUNCTION_WIDTH_EXPAND_UPTO_ENGINE_SIZE);
      int minWidth = executionNodeMap.getExecutors().size() <= tableFunctionMaxParallelism
        ? executionNodeMap.getExecutors().size() : tableFunctionMaxParallelism;
      int existingMinWidth = finalMinWidth > 0 ? finalMinWidth : stats.getMinWidth();
      finalMinWidth = Math.max(minWidth, existingMinWidth);
      int existingMaxWidth = finalMaxWidth > 0 ? finalMaxWidth : stats.getMaxWidth();
      finalMaxWidth = Math.max(finalMinWidth, existingMaxWidth);
    }
    if (finalMinWidth > 0) {
      stats.addMinWidth(finalMinWidth);
    }
    if (finalMaxWidth > 0) {
      stats.addMaxWidth(finalMaxWidth);
    }
    for (PhysicalOperator child : op) {
      if (child != null) {
        child.accept(this, wrapper);
      }
    }
    return null;
  }
}
