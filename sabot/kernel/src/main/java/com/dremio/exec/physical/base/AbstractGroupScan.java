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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * GroupScan build on top of a Namespace-sourced SplitWork.
 */
public abstract class AbstractGroupScan extends AbstractBase implements GroupScan<SplitWork> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractGroupScan.class);

  protected final TableMetadata dataset;
  protected final List<SchemaPath> columns;

  private final Supplier<List<List<String>>> referencedTables =
      Suppliers.memoize(
          new Supplier<List<List<String>>>() {
            @Override
            public List<List<String>> get() {
              final List<String> table = dataset.getName().getPathComponents();
              if (table == null) {
                return ImmutableList.of();
              }
              return ImmutableList.of(table);
            }
          });

  public AbstractGroupScan(
      TableMetadata dataset,
      List<SchemaPath> columns) {
    super(dataset.getUser());
    this.dataset = dataset;
    this.columns = columns;
  }

  @JsonIgnore
  @Deprecated
  public List<String> getTableSchemaPath() {
    final Collection<List<String>> paths = getReferencedTables();
    if (paths.isEmpty()) {
      return null;
    } else {
      return Iterables.getOnlyElement(paths);
    }
  }

  @JsonIgnore // used in planning
  @Override
  public Collection<List<String>> getReferencedTables() {
    return referencedTables.get();
  }

  @Override
  public boolean mayLearnSchema() {
    return true;
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext functionLookupContext) {
    return getSchema().maskAndReorder(getColumns());
  }

  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    return dataset.getSplitCount();
  }

  @JsonIgnore
  protected TableMetadata getDataset(){
    return dataset;
  }

  @JsonIgnore
  public BatchSchema getSchema() {
    return dataset.getSchema();
  }

  @Override

  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitGroupScan(this, value);
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public Iterator<SplitWork> getSplits(ExecutionNodeMap nodeMap) {
    return SplitWork.transform(dataset.getSplits(), nodeMap, getDistributionAffinity());
  }

  @Override
  @JsonIgnore
  public int getMinParallelizationWidth() {
    return 1;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return this;
  }

}
