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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.Exchange;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.schedule.CompleteWork;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

public class Materializer extends AbstractPhysicalVisitor<PhysicalOperator, Materializer.IndexedFragmentNode, ExecutionSetupException>{

  private final Map<GroupScan, ListMultimap<Integer, CompleteWork>> splitSets;
  private EndpointsIndex.Builder indexBuilder;

  public Set<Integer> getExtCommunicableMajorFragments() {
    return extCommunicableMajorFragments;
  }

  private Set<Integer> extCommunicableMajorFragments = new HashSet<>();

  Materializer(Map<GroupScan, ListMultimap<Integer, CompleteWork>> splitSets, EndpointsIndex.Builder indexBuilder) {
    this.splitSets = splitSets;
    this.indexBuilder = indexBuilder;
  }

  @Override
  public PhysicalOperator visitExchange(Exchange exchange, IndexedFragmentNode iNode) throws ExecutionSetupException {
    iNode.addAllocation(exchange);
    extCommunicableMajorFragments.addAll(exchange.getExtCommunicableMajorFragments());

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
    extCommunicableMajorFragments.addAll(groupScan.getExtCommunicableMajorFragments());

    return child;
  }

  @Override
  public PhysicalOperator visitSubScan(SubScan subScan, IndexedFragmentNode value) throws ExecutionSetupException {
    value.addAllocation(subScan);
    extCommunicableMajorFragments.addAll(subScan.getExtCommunicableMajorFragments());
    // TODO - implement this
    return super.visitOp(subScan, value);
  }

  @Override
  public PhysicalOperator visitOp(PhysicalOperator op, IndexedFragmentNode iNode) throws ExecutionSetupException {
    iNode.addAllocation(op);
//    logger.debug("Visiting catch all: {}", op);
    extCommunicableMajorFragments.addAll(op.getExtCommunicableMajorFragments());
    List<PhysicalOperator> children = Lists.newArrayList();
    for(PhysicalOperator child : op){
      children.add(child.accept(this, iNode));
    }
    PhysicalOperator newOp = op.getNewWithChildren(children);
    return newOp;
  }

  @Override
  public PhysicalOperator visitWriter(Writer op, IndexedFragmentNode iNode) throws ExecutionSetupException {
    if (AbstractWriter.class.isAssignableFrom(op.getClass())) {
      setTruncatedRecordLimit((AbstractWriter) op,
                              iNode.getInfo().getWidth());
    }
    return visitOp(op, iNode);
  }

  /**
   * If output records needs to be truncated,
   * this method computes the number of records to be written per minor fragment
   * and sets the computed number in WriterOptions in the AbstractWriter.
   *
   * @param abstractWriter
   * @param numMinorFragments
   */
  private void setTruncatedRecordLimit(AbstractWriter abstractWriter, int numMinorFragments) {
    WriterOptions wo = abstractWriter.getOptions();
    if (wo.isOutputLimitEnabled()) {
      wo.setRecordLimit(Math.max(PlannerSettings.MIN_RECORDS_PER_FRAGMENT,
                                 wo.getOutputLimitSize() / numMinorFragments));
    }
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
