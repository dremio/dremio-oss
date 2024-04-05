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
package com.dremio.exec.physical;

import com.dremio.common.graph.Graph;
import com.dremio.common.graph.GraphAlgos;
import com.dremio.common.logical.PlanProperties;
import com.dremio.exec.physical.base.Leaf;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Root;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;

@JsonPropertyOrder({"head", "graph"})
public class PhysicalPlan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalPlan.class);

  PlanProperties properties;
  Graph<PhysicalOperator, Root, Leaf> graph;

  @JsonIgnore Runnable committer;

  // cleaner for failure/cancellation during plan execution
  @JsonIgnore Runnable cleaner;

  @JsonCreator
  public PhysicalPlan(
      @JsonProperty("head") PlanProperties properties,
      @JsonProperty("graph") List<PhysicalOperator> operators) {
    this.properties = properties;
    this.graph = Graph.newGraph(operators, Root.class, Leaf.class);
  }

  public PhysicalPlan(
      PlanProperties properties, List<PhysicalOperator> operators, Runnable committer) {
    this(properties, operators);
    this.committer = committer;
    this.cleaner = null;
  }

  public PhysicalPlan(
      PlanProperties properties,
      List<PhysicalOperator> operators,
      Runnable committer,
      Runnable cleaner) {
    this(properties, operators);
    this.committer = committer;
    this.cleaner = cleaner;
  }

  @JsonProperty("graph")
  public List<PhysicalOperator> getSortedOperators() {
    // reverse the list so that nested references are flattened rather than nested.
    return getSortedOperators(true);
  }

  public Root getRoot() {
    return graph.getRoots().iterator().next();
  }

  public List<PhysicalOperator> getSortedOperators(boolean reverse) {
    List<PhysicalOperator> list = GraphAlgos.TopoSorter.sort(graph);
    if (reverse) {
      return Lists.reverse(list);
    } else {
      return list;
    }
  }

  @JsonIgnore
  public double getCost() {
    double totalCost = 0;
    for (final PhysicalOperator ops : getSortedOperators()) {
      totalCost += ops.getProps().getCost();
    }
    return totalCost;
  }

  @JsonProperty("head")
  public PlanProperties getProperties() {
    return properties;
  }

  /** Converts a physical plan to a string. (Opposite of {@link #parse}.) */
  public String unparse(ObjectWriter writer) {
    try {
      return writer.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public Optional<Runnable> getCommitter() {
    return Optional.ofNullable(committer);
  }

  public Optional<Runnable> getCleaner() {
    return Optional.ofNullable(cleaner);
  }
}
