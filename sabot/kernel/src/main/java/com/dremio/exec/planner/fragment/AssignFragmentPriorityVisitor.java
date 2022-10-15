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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.Exchange;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.physical.config.NestedLoopJoinPOP;
import com.dremio.exec.physical.config.UnionAll;
import com.dremio.exec.work.foreman.ForemanSetupException;

/**
 * Assigns priorities to fragments to provide hint(s) to the executor on which fragment to execute first in
 * case there is a resource contention.
 * <p>
 * Uses a simple algorithm where priority increases by one when any Y joint is encountered based on specific
 * rules for the operator at the joint. For example, number increases by 1 when moving to the probe side of a join,
 * which indicates that build side fragments and fragments above has a higher priority than the build side.
 * </p>
 * <p>
 * Note that at the end number is reversed so that higher number indicates higher priority. This is done to make it
 * easier for the executor to assign weights (which works more like a max heap).
 * </p>
 */
public class AssignFragmentPriorityVisitor extends AbstractPhysicalVisitor<Void, Void, ForemanSetupException> {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(AssignFragmentPriorityVisitor.class);
  private final Map<Integer, Integer> majorFragmentToPriorityMap;

  private int currentPriority;
  private int maxPrioritySeenAtExchange;

  public AssignFragmentPriorityVisitor() {
    this.currentPriority = 1;
    this.maxPrioritySeenAtExchange = Integer.MIN_VALUE;
    this.majorFragmentToPriorityMap = new HashMap<>();
  }

  @Override
  public Void visitExchange(Exchange exchange, Void value) throws ForemanSetupException {
    // put the current priority and then move down
    majorFragmentToPriorityMap.putIfAbsent(exchange.getChild().getProps().getMajorFragmentId(), currentPriority);
    majorFragmentToPriorityMap.putIfAbsent(exchange.getProps().getMajorFragmentId(), currentPriority);
    this.maxPrioritySeenAtExchange = Math.max(currentPriority, maxPrioritySeenAtExchange);
    exchange.getChild().accept(this, null);
    if (this.currentPriority > this.maxPrioritySeenAtExchange) {
      // remove holes due to full Y fragments when we leave an exchange
      this.currentPriority = this.maxPrioritySeenAtExchange;
    }
    return value;
  }

  @Override
  public Void visitHashJoin(HashJoinPOP op, Void value) throws ForemanSetupException {
    // first visit the right child
    op.getRight().accept(this, null);
    // increase the priority and visit the left child
    currentPriority++;
    op.getLeft().accept(this, null);
    return null;
  }

  @Override
  public Void visitNestedLoopJoin(NestedLoopJoinPOP op, Void value) throws ForemanSetupException {
    // first visit the build side
    op.getBuild().accept(this, null);
    // increase the priority and visit the probe side
    currentPriority++;
    op.getProbe().accept(this, null);
    return null;
  }

  @Override
  public Void visitUnion(UnionAll op, Void value) throws ForemanSetupException {
    boolean first = true;
    for (PhysicalOperator child : op) {
      if (!first) {
        // if we are branching due to multi-child (e.g union) increase priority before next branch
        currentPriority++;
      }
      child.accept(this, null);
      first = false;
    }
    return null;
  }

  @Override
  public Void visitOp(PhysicalOperator op, Void value)  throws ForemanSetupException {
    for (PhysicalOperator child : op) {
      child.accept(this, null);
    }
    return null;
  }

  /**
   * Gets fragment weight.
   * <p>
   * Priority is converted to weight which is inverse of priority
   * </p>
   *
   * @param majorFragmentId major fragment id
   * @return fragment weight, higher number denoting higher priority
   */
  public int getFragmentWeight(int majorFragmentId) {
    final int maxAssignPriority = Math.max(maxPrioritySeenAtExchange, 1);
    Integer prio = majorFragmentToPriorityMap.get(majorFragmentId);
    if (prio == null) {
      // this should not happen, but let us not make it fatal if it does
      logger.warn("Assigned Priority not found for major fragment {}. Defaulting to {}", majorFragmentId,
        maxAssignPriority);
      return maxAssignPriority;
    } else {
      return maxAssignPriority - prio + 1;
    }
  }
}
