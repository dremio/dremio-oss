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
package com.dremio.exec.physical.base;

import java.util.List;
import java.util.Set;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.graph.GraphValue;
import com.dremio.common.graph.GraphVisitor;
import com.dremio.exec.ops.OperatorMetricRegistry;
import com.dremio.options.Options;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.google.common.base.Preconditions;

@Options
@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({ "id" })
@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "id")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "pop")
public interface PhysicalOperator extends GraphValue<PhysicalOperator> {

  /**
   * Provides capability to build a set of output based on traversing a query graph tree.
   *
   * @param physicalVisitor
   * @return
   */
  <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E;

  /**
   * Regenerate with this node with a new set of children.  This is used in the case of materialization or optimization.
   * @param children
   * @return
   */
  @JsonIgnore
  PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException;

  @Override
  default void accept(GraphVisitor<PhysicalOperator> visitor) {
    visitor.enter(this);
    if (this.iterator() == null) {
      throw new IllegalArgumentException("Null iterator for pop." + this);
    }
    for (PhysicalOperator o : this) {
      Preconditions.checkNotNull(o, String.format("Null in iterator for pop %s.", this));
      o.accept(visitor);
    }
    visitor.leave(this);
  }

  @JsonProperty("id")
  default int getId() {
    return getProps().getOperatorId();
  }

  /**
   * Exists only to help jackson.
   * @param id
   */
  @Deprecated
  default void setId(int id) {
    //
    assert getProps().getOperatorId() == id;
  }

  public OpProps getProps();

  @JsonIgnore
  public int getOperatorType();

  @JsonIgnore
  default int getOperatorSubType() {
    // 0 is reserved. And it is a default core operator's subtype.
    return OperatorMetricRegistry.DEFAULT_CORE_OPERATOR_SUBTYPE;
  }

  /**
   * Other major fragments, this operator will be interested in sending messages to.
   * This is processed only once per major fragment.
   *
   * @return
   */
  @JsonIgnore
  Set<Integer> getExtCommunicableMajorFragments();

  @JsonIgnore
  default long getMemReserve() {
    return getProps().getMemReserve();
  }
}
