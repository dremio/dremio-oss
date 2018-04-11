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
package com.dremio.exec.physical.base;


import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.MinorFragmentEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

public abstract class AbstractSender extends AbstractSingle implements Sender {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSender.class);

  protected final int oppositeMajorFragmentId;
  //
  protected final List<MinorFragmentEndpoint> destinations;

  @JsonProperty("schema")
  protected BatchSchema schema;

  /**
   * @param oppositeMajorFragmentId MajorFragmentId of fragments that are receiving data sent by this sender.
   * @param child Child PhysicalOperator which is providing data to this Sender.
   * @param destinations List of receiver MinorFragmentEndpoints each containing MinorFragmentId and SabotNode endpoint
   *                     where it is running.
   */
  public AbstractSender(int oppositeMajorFragmentId,
                        PhysicalOperator child,
                        List<MinorFragmentEndpoint> destinations,
                        BatchSchema schema) {
    super(child);
    this.oppositeMajorFragmentId = oppositeMajorFragmentId;
    this.destinations = ImmutableList.copyOf(destinations);
    this.schema = schema;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSender(this, value);
  }

  @Override
  @JsonProperty("receiver-major-fragment")
  public int getOppositeMajorFragmentId() {
    return oppositeMajorFragmentId;
  }

  @Override
  @JsonProperty("destinations")
  public List<MinorFragmentEndpoint> getDestinations() {
    return destinations;
  }

  @Override
  @JsonProperty("schema")
  public BatchSchema getSchema() {
    return schema;
  }
}
