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
package com.dremio.exec.physical.config;

import java.util.List;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.physical.base.AbstractExchange;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalOperatorUtil;
import com.dremio.exec.physical.base.Receiver;
import com.dremio.exec.physical.base.Sender;
import com.dremio.exec.physical.config.HashSenderCalculator.BucketOptions;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HashToRandomExchange extends AbstractExchange {

  private static final boolean HASH_EXCHANGE_SPOOLING;

  static {
    HASH_EXCHANGE_SPOOLING = "true".equals(System.getProperty("dremio.hash_exchange_spooling", "false"));
  }

  private final BucketOptions options;
  private final LogicalExpression expr;
  private final OptionManager optionManager;
  private final boolean adaptiveHash;

  public HashToRandomExchange(
      OpProps props,
      OpProps senderProps,
      OpProps receiverProps,
      BucketOptions options,
      BatchSchema schema,
      PhysicalOperator child,
      LogicalExpression expr,
      OptionManager optionManager,
      boolean adaptiveHash) {
    super(props, senderProps, receiverProps, schema, child, optionManager);
    this.options = options;
    this.expr = expr;
    this.optionManager = optionManager;
    this.adaptiveHash = adaptiveHash;
  }

  public HashToRandomExchange(
    OpProps props,
    OpProps senderProps,
    OpProps receiverProps,
    BucketOptions options,
    BatchSchema schema,
    PhysicalOperator child,
    LogicalExpression expr,
    OptionManager optionManager) {
    this(props, senderProps, receiverProps, options, schema, child, expr, optionManager, false);
  }

  @Override
  public long getMemReserve() {
   return computeMemReserve();
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child, EndpointsIndex.Builder indexBuilder) {
    final List<MinorFragmentIndexEndpoint> dest = PhysicalOperatorUtil.getIndexOrderedEndpoints(receiverLocations, indexBuilder);
    return new HashPartitionSender(options.getResult(senderProps, dest.size()), schema, child, receiverMajorFragmentId, dest, expr, adaptiveHash);
  }

  @Override
  public Receiver getReceiver(int minorFragmentId, EndpointsIndex.Builder indexBuilder) {
    return new UnorderedReceiver(receiverProps, schema, senderMajorFragmentId, PhysicalOperatorUtil.getIndexOrderedEndpoints(senderLocations, indexBuilder), HASH_EXCHANGE_SPOOLING);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new HashToRandomExchange(props, senderProps, receiverProps, options, schema, child, expr, optionManager, adaptiveHash);
  }

  @JsonProperty("expr")
  public LogicalExpression getExpression(){
    return expr;
  }
}
