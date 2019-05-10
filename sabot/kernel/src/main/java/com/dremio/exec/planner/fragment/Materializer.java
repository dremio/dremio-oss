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
package com.dremio.exec.planner.fragment;

import java.util.List;
import java.util.Map;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.Exchange;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.schedule.CompleteWork;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

public class Materializer extends AbstractPhysicalVisitor<PhysicalOperator, Materializer.IndexedFragmentNode, ExecutionSetupException>{
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Materializer.class);

  private final Map<GroupScan, ListMultimap<Integer, CompleteWork>> splitSets;
  private EndpointsIndex.Builder indexBuilder;

  Materializer(Map<GroupScan, ListMultimap<Integer, CompleteWork>> splitSets, EndpointsIndex.Builder indexBuilder) {
    this.splitSets = splitSets;
    this.indexBuilder = indexBuilder;
  }

  @Override
  public PhysicalOperator visitExchange(Exchange exchange, IndexedFragmentNode iNode) throws ExecutionSetupException {
    iNode.addAllocation(exchange);
    if(exchange == iNode.getNode().getSendingExchange()){

      // this is a sending exchange.
      PhysicalOperator child = exchange.getChild().accept(this, iNode);
      PhysicalOperator materializedSender = exchange.getSender(iNode.getMinorFragmentId(), child, indexBuilder);
      return materializedSender;

    }else{
      // receiving exchange.
      PhysicalOperator materializedReceiver = exchange.getReceiver(iNode.getMinorFragmentId(), indexBuilder);
      return materializedReceiver;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public PhysicalOperator visitGroupScan(@SuppressWarnings("rawtypes") GroupScan groupScan, IndexedFragmentNode iNode) throws ExecutionSetupException {
    ListMultimap<Integer, CompleteWork> splits = splitSets.get(groupScan);
    Preconditions.checkNotNull(splits);
    List<CompleteWork> work = splits.get(iNode.getMinorFragmentId());
    Preconditions.checkNotNull(work);
    PhysicalOperator child = groupScan.getSpecificScan(work);
    iNode.addAllocation(groupScan);
    return child;
  }

  @Override
  public PhysicalOperator visitSubScan(SubScan subScan, IndexedFragmentNode value) throws ExecutionSetupException {
    value.addAllocation(subScan);
    // TODO - implement this
    return super.visitOp(subScan, value);
  }

  @Override
  public PhysicalOperator visitOp(PhysicalOperator op, IndexedFragmentNode iNode) throws ExecutionSetupException {
    iNode.addAllocation(op);
//    logger.debug("Visiting catch all: {}", op);
    List<PhysicalOperator> children = Lists.newArrayList();
    for(PhysicalOperator child : op){
      children.add(child.accept(this, iNode));
    }
    PhysicalOperator newOp = op.getNewWithChildren(children);
    return newOp;
  }

  public static class IndexedFragmentNode{
    final Wrapper info;
    final int minorFragmentId;

    public IndexedFragmentNode(int minorFragmentId, Wrapper info) {
      super();
      this.info = info;
      this.minorFragmentId = minorFragmentId;
    }

    public Fragment getNode() {
      return info.getNode();
    }

    public int getMinorFragmentId() {
      return minorFragmentId;
    }

    public Wrapper getInfo() {
      return info;
    }

    public void addAllocation(PhysicalOperator pop) {
      info.addAllocation(pop);
    }

  }

}
