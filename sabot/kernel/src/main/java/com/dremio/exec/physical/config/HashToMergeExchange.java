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
package com.dremio.exec.physical.config;

import java.util.List;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractExchange;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalOperatorUtil;
import com.dremio.exec.physical.base.Receiver;
import com.dremio.exec.physical.base.Sender;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("hash-to-merge-exchange")
public class HashToMergeExchange extends AbstractExchange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashToMergeExchange.class);

  private final LogicalExpression distExpr;
  private final List<Ordering> orderExprs;

  @JsonCreator
  public HashToMergeExchange(@JsonProperty("child") PhysicalOperator child,
      @JsonProperty("expr") LogicalExpression expr,
      @JsonProperty("orderings") List<Ordering> orderExprs) {
    super(child);
    this.distExpr = expr;
    this.orderExprs = orderExprs;
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child, FunctionLookupContext context) {
    return new HashPartitionSender(receiverMajorFragmentId, child, distExpr,
        PhysicalOperatorUtil.getIndexOrderedEndpoints(receiverLocations), getSchema(context));
  }

  @Override
  public Receiver getReceiver(int minorFragmentId, FunctionLookupContext context) {
    return new MergingReceiverPOP(senderMajorFragmentId, PhysicalOperatorUtil.getIndexOrderedEndpoints(senderLocations), orderExprs, true, getSchema(context));
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new HashToMergeExchange(child, distExpr, orderExprs);
  }

  @JsonProperty("orderExpr")
  public List<Ordering> getOrderExpressions(){
    return orderExprs;
  }
}
