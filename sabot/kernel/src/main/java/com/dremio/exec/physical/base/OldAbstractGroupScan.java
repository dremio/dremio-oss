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
import com.dremio.exec.expr.FakeAllocator;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.parquet.FilterCondition;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.sabot.op.scan.VectorContainerMutator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

@Deprecated
public abstract class OldAbstractGroupScan<T extends CompleteWork> extends AbstractBase implements GroupScan<T> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractGroupScan.class);

  private BatchSchema schema;
  private final List<String> tablePath;

  public OldAbstractGroupScan(String userName, BatchSchema schema, List<String> tablePath) {
    super(userName);
    this.schema = Preconditions.checkNotNull(schema, "Schema must not be null.");
    this.tablePath = tablePath;
  }

  public OldAbstractGroupScan(OldAbstractGroupScan that) {
    super(that);
    this.schema = that.schema;
    this.tablePath = that.tablePath;
  }

  @Override
  public List<String> getTableSchemaPath(){
    return tablePath;
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext functionLookupContext) {
    try(VectorContainer c = new VectorContainer(FakeAllocator.INSTANCE)){
      schema.materializeVectors(getColumns(), new VectorContainerMutator(c));
      c.buildSchema(SelectionVectorMode.NONE);
      return c.getSchema();
    }
  }

  @Override
  @Deprecated
  public Convention getStoragePluginConvention(){
    if(getPlugin() != null){
      return getPlugin().getStoragePluginConvention();
    }
    return null;
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  protected void updateSchema(BatchSchema schema) {
    this.schema = schema;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitGroupScan(this, value);
  }

  @Override
  public OldAbstractGroupScan<T> clone(List<SchemaPath> columns) {
    throw new UnsupportedOperationException(String.format("%s does not implement clone(columns) method!", this.getClass().getCanonicalName()));
  }

  @Override
  @JsonIgnore
  public int getMinParallelizationWidth() {
    return 1;
  }

  @Override
  public ScanStats getScanStats(PlannerSettings settings) {
    return getScanStats();
  }

  @JsonIgnore
  public ScanStats getScanStats() {
    throw new UnsupportedOperationException("This should be implemented.");
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return false;
  }

  @Override
  @JsonIgnore
  public boolean supportsPartitionFilterPushdown() {
    return false;
  }

  /**
   * By default, throw exception, since group scan does not have exact column value count.
   */
  @Override
  public long getColumnValueCount(SchemaPath column) {
    throw new UnsupportedOperationException(String.format("%s does not have exact column value count!", this.getClass().getCanonicalName()));
  }

  @Override
  public int getOperatorType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SchemaPath> getPartitionColumns() {
    return Lists.newArrayList();
  }

  /**
   * Default is not to support limit pushdown.
   * @return
   */
  @Override
  @JsonIgnore
  public boolean supportsLimitPushdown() {
    return false;
  }

  /**
   * By default, return null to indicate rowcount based prune is not supported.
   * Each groupscan subclass should override, if it supports rowcount based prune.
   */
  @Override
  @JsonIgnore
  public GroupScan applyLimit(long maxRecords) {
    return null;
  }

  @Override
  public boolean hasFiles() {
    return false;
  }

  @Override
  public Collection<String> getFiles() {
    return null;
  }

  @Override
  @JsonIgnore
  public List<String> getSortColumns() {
    return Collections.emptyList();
  }

  @Override
  public GroupScan cloneWithFilter(List<FilterCondition> condition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportsFilterPushdown() {
    return false;
  }

  @Override
  public boolean hasConditions() {
    return false;
  }

  @Override
  public abstract StoragePlugin<?> getPlugin();

  @Override
  public String getDigest() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanCostFactor getScanCostFactor() {
    throw new UnsupportedOperationException();
  }


  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children == null || children.isEmpty());
    return this;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.SOFT;
  }

  /**
   * All GroupScans should implement equals() and hashCode()
   */
  @Override
  public abstract boolean equals(final Object o);
  @Override
  public abstract int hashCode();

}
