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
package com.dremio.exec.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Function;

import com.dremio.common.JSONOptions;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.planner.PlannerCallback;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.namespace.TableInstance;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.Message;
import com.dremio.service.namespace.SourceState.MessageLevel;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.ImmutableSet;

/** Abstract class for StorePlugin implementations.
 * See StoragePlugin for description of the interface intent and its methods.
 */
public abstract class AbstractStoragePlugin<C extends ConversionContext> implements StoragePlugin<C> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractStoragePlugin.class);

  private SourceState state = SourceState.GOOD;

  protected AbstractStoragePlugin(){
  }

  @Override
  public Convention getStoragePluginConvention() {
    // Since people seem to not like null, adding Convention.NONE, although it really should be null.
    // Not all storage plugins bring in their own conventions, so it should not be tied to a convention.
    // So for now, we will use Convention.NONE as it does not bring its own convention.
    return Convention.NONE;
  }

  @Override
  public SourceState getState() {
    return state;
  }

  @Override
  public boolean refreshState() {
    return false;
  }

  protected boolean setState(SourceState newState) {
    boolean changed = !newState.equals(this.state);
    this.state = newState;
    return changed;
  }

  public SchemaMutability getMutability() {
    return SchemaMutability.NONE;
  }

  @Override
  public boolean supportsRead() {
    return false;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public RelNode getRel(final RelOptCluster cluster, final RelOptTable relOptTable, final C relContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetConfig getDataset(List<String> tableSchemaPath, TableInstance tableInstance, SchemaConfig schemaConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<DatasetConfig> listDatasets() {
    return Collections.emptyList();
  }

  /**
   * @deprecated Marking for deprecation in next major version release. Use
   */
  @Override
  @Deprecated
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext) {
    return ImmutableSet.of();
  }

  /**
   * @deprecated Marking for deprecation in next major version release. Use
   */
  @Deprecated
  public Set<? extends RelOptRule> getLogicalOptimizerRules(OptimizerRulesContext optimizerContext) {
    return ImmutableSet.of();
  }

  /**
   * @deprecated Marking for deprecation in next major version release. Use
   */
  @Deprecated
  public Set<? extends RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    // To be backward compatible, by default call the getOptimizerRules() method.
    return getOptimizerRules(optimizerRulesContext);
  }

  @Override
  public StoragePlugin2 getStoragePlugin2() {
    return null;
  }

  /**
   *
   * Note: Move this method to {@link StoragePlugin} interface in next major version release.
   */
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext, PlannerPhase phase) {
    switch (phase) {
    case LOGICAL:
      return getLogicalOptimizerRules(optimizerContext);
    case PHYSICAL:
      return getPhysicalOptimizerRules(optimizerContext);
    case JOIN_PLANNING:
    default:
      return ImmutableSet.of();
    }

  }

  /**
   * Return a planner callback for this storage plugin (or null if one doesn't exist).
   * @param queryContext The query context associated with the current planning process.
   * @param phase The phase of query planning that this callback will be registered in.
   * @return A callback or null if the plugin doesn't have any callbacks to include.
   *
   * Note: Move this method to {@link StoragePlugin} interface in next major version release.
   */
  public PlannerCallback getPlannerCallback(QueryContext queryContext, PlannerPhase phase) {
    return null;
  }

  @Override
  public OldAbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<String> tableSchemaPath, List<SchemaPath> columns) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void start() throws IOException {
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public Collection<Function> getFunctions(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return Collections.emptyList();
  }

  @Override
  public Iterable<String> getSubPartitions(List<String> table, List<String> partitionColumns, List<String> partitionValues, SchemaConfig schemaConfig) throws PartitionNotFoundException {
    return Collections.emptyList();
  }

  @Override
  public boolean createView(List<String> tableSchemaPath, View view, SchemaConfig schemaConfig) throws IOException {
    throw new UnsupportedOperationException(getClass().getName() + " does not support creating views");
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  @Override
  public void dropView(SchemaConfig schemaConfig, List<String> tableSchemaPath) throws IOException {
    throw new UnsupportedOperationException(getClass().getName() + " does not support dropping views");
  }

  @Override
  public boolean supportsContains() {
    return false;
  }

  private static SourceState getSourceState(SourceState.SourceStatus status, String... msgs) {
    List<Message> messageList = new ArrayList<>();
    for (String msg : msgs) {
      messageList.add(new Message(MessageLevel.WARN, msg));
    }
    return new SourceState(status, messageList);
  }

  protected static SourceState warnState(String... e) {
    return getSourceState(SourceStatus.warn, e);
  }

  protected static SourceState badState(String... e) {
    return getSourceState(SourceStatus.bad, e);
  }

  protected static SourceState badState(Exception e) {
    return getSourceState(SourceStatus.bad, e.getMessage());
  }
}
