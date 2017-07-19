/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.plan.Convention;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.parquet.FilterCondition;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;

/**
 * GroupScan build on top of a Namespace-sourced SplitWork.
 */
public abstract class AbstractGroupScan extends AbstractBase implements GroupScan<SplitWork> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractGroupScan.class);

  protected final TableMetadata dataset;
  protected final List<SchemaPath> columns;

  public AbstractGroupScan(
      TableMetadata dataset,
      List<SchemaPath> columns) {
    super(dataset.getUser());
    this.dataset = dataset;
    this.columns = columns;
  }

  @JsonIgnore
  public List<String> getTableSchemaPath(){
    return dataset.getName().getPathComponents();
  }

  public BatchSchema getSchema(FunctionLookupContext context) {
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
  public AbstractGroupScan clone(List<SchemaPath> columns) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanStats getScanStats(PlannerSettings settings) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    throw new UnsupportedOperationException();
  }

  @Override
  @JsonIgnore
  public boolean supportsPartitionFilterPushdown() {
    throw new UnsupportedOperationException();
  }

  /**
   * By default, throw exception, since group scan does not have exact column value count.
   */
  @Override
  public long getColumnValueCount(SchemaPath column) {
    throw new UnsupportedOperationException();
  }

  @Override
  @JsonIgnore
  public List<SchemaPath> getPartitionColumns() {
    throw new UnsupportedOperationException();
  }

  /**
   * Default is not to support limit pushdown.
   * @return
   */
  @Override
  @JsonIgnore
  public boolean supportsLimitPushdown() {
    throw new UnsupportedOperationException();
  }

  @JsonIgnore
  public Convention getStoragePluginConvention(){
    throw new UnsupportedOperationException();
  }

  @Override
  public GroupScan applyLimit(long maxRecords) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasFiles() {
    throw new UnsupportedOperationException();
  }

  @Override
  @JsonIgnore
  public Collection<String> getFiles() {
    throw new UnsupportedOperationException();
  }

  @Override
  @JsonIgnore
  public List<String> getSortColumns() {
    throw new UnsupportedOperationException();
  }

  @Override
  public GroupScan cloneWithFilter(List<FilterCondition> condition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportsFilterPushdown() {
    throw new UnsupportedOperationException();
  }

  @Override
  @JsonIgnore
  public boolean hasConditions() {
    throw new UnsupportedOperationException();
  }


  @Override
  @JsonIgnore
  public StoragePlugin<? extends GroupScan<SplitWork>> getPlugin() {
    throw new UnsupportedOperationException();
  }

  @Override
  @JsonIgnore
  public String getDigest() {
    throw new UnsupportedOperationException();
  }

  @Override
  @JsonIgnore
  public ScanCostFactor getScanCostFactor() {
    throw new UnsupportedOperationException();
  }


  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children == null || children.isEmpty());
    return this;
  }

  @JsonIgnore
  public boolean includesModTime() {
    throw new UnsupportedOperationException();
  }


}
