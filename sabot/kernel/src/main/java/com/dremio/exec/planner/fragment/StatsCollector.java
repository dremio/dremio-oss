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
package com.dremio.exec.planner.fragment;

import java.util.Collections;
import java.util.List;

import com.dremio.exec.physical.base.Exchange;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Store;
import com.dremio.exec.planner.AbstractOpWrapperVisitor;
import com.dremio.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.schedule.CompleteWork;
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

  public StatsCollector(final PlanningSet planningSet, ExecutionNodeMap executionNodeMap) {
    this.planningSet = planningSet;
    this.executionNodeMap = executionNodeMap;
  }

  @Override
  public Void visitSendingExchange(Exchange exchange, Wrapper wrapper) throws RuntimeException {
    // Handle the sending side exchange
    Wrapper receivingFragment = planningSet.get(wrapper.getNode().getSendingExchangePair().getNode());

    // List to contain the endpoints where the fragment that receive data to this fragment are running.
    List<NodeEndpoint> receiverEndpoints;
    if (receivingFragment.isEndpointsAssignmentDone()) {
      receiverEndpoints = receivingFragment.getAssignedEndpoints();
    } else {
      receiverEndpoints = Collections.emptyList();
    }

    wrapper.getStats().addParallelizationInfo(exchange.getSenderParallelizationInfo(receiverEndpoints));
    return visitOp(exchange, wrapper);
  }

  @Override
  public Void visitReceivingExchange(Exchange exchange, Wrapper wrapper) throws RuntimeException {
    // Handle the receiving side Exchange

    final List<ExchangeFragmentPair> receivingExchangePairs = wrapper.getNode().getReceivingExchangePairs();

    // List to contain the endpoints where the fragment that send dat to this fragment are running.
    final List<NodeEndpoint> sendingEndpoints = Lists.newArrayList();

    for(ExchangeFragmentPair pair : receivingExchangePairs) {
      if (pair.getExchange() == exchange) {
        Wrapper sendingFragment = planningSet.get(pair.getNode());
        if (sendingFragment.isEndpointsAssignmentDone()) {
          sendingEndpoints.addAll(sendingFragment.getAssignedEndpoints());
        }
      }
    }

    wrapper.getStats().addParallelizationInfo(exchange.getReceiverParallelizationInfo(sendingEndpoints));
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
    stats.addEndpointAffinities(work.iterator());
    stats.setDistributionAffinity(groupScan.getDistributionAffinity());

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
    stats.addCost(op.getCost());
    if (wrapper.getNode().getSendingExchange() != op && op.isSingle()) {
      stats.addMaxWidth(1);
    }
    for (PhysicalOperator child : op) {
      child.accept(this, wrapper);
    }
    return null;
  }
}
