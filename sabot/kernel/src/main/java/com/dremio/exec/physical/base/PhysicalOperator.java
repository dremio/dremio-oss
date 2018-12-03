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

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.graph.GraphValue;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.google.protobuf.ByteString;

@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({ "@id" })
@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "@id")
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

  /**
   * @return The memory to preallocate for this operator
   */
  long getInitialAllocation();

  /**
   * @return The maximum memory this operator can allocate
   */
  long getMaxAllocation();

  @JsonProperty("@id")
  int getOperatorId();

  @JsonProperty("@id")
  void setOperatorId(int id);

  @JsonProperty("cost")
  void setCost(double cost);

  @JsonProperty("cost")
  double getCost();

  @JsonIgnore
  BatchSchema getSchema(FunctionLookupContext context);

  void setAsSingle();

  @JsonIgnore
  boolean isSingle();

  /**
   * Name of the user whom to impersonate while setting up the implementation (RecordBatch) of this
   * PhysicalOperator. Default value is "null" in which case we impersonate as user who launched the query.
   * @return
   */
  @JsonProperty("userName")
  public String getUserName();

  @JsonIgnore
  public int getOperatorType();


  @JsonIgnore
  Collection<Entry<String, ByteString>> getSharedData();
}
