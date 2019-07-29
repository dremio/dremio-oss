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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.graph.GraphVisitor;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public abstract class AbstractSubScan extends AbstractBase implements SubScan, OpWithMinorSpecificAttrs, Cloneable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSubScan.class);

  private final BatchSchema fullSchema;
  private final Collection<List<String>> referencedTables;

  public AbstractSubScan(
      OpProps props,
      BatchSchema fullSchema,
      Collection<List<String>> referencedTables) {
    super(props);
    this.fullSchema = fullSchema;
    this.referencedTables = referencedTables;
  }

  public AbstractSubScan(
      OpProps props,
      BatchSchema fullSchema,
      List<String> tablePath
      ) {
    super(props);
    this.fullSchema = fullSchema;
    if (tablePath == null) {
      this.referencedTables = ImmutableList.of();
    } else {
      this.referencedTables = ImmutableList.of(tablePath);
    }
  }

  @JsonIgnore
  @Deprecated // TODO: SubScan implementations use factory methods (@JsonCreator) that need this method
  public List<String> getTableSchemaPath() {
    final Collection<List<String>> paths = getReferencedTables();
    if (paths.isEmpty()) {
      return null;
    } else {
      return Iterables.getOnlyElement(paths);
    }
  }

  @Override
  public Collection<List<String>> getReferencedTables() {
    return referencedTables;
  }

  @Override
  public boolean mayLearnSchema() {
    return true;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();

    try {
      return (PhysicalOperator)this.clone();
    } catch (CloneNotSupportedException e) {
      throw new ExecutionSetupException("failed when cloning", e);
    }
  }

  @Override
  public void accept(GraphVisitor<PhysicalOperator> visitor) {
    visitor.enter(this);
    visitor.visit(this);
    visitor.leave(this);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @JsonProperty("fullSchema")
  public BatchSchema getFullSchema() {
    return fullSchema;
  }

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) throws Exception {}

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {}
}
