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
package com.dremio.exec.store.materialization;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers;
import com.dremio.exec.planner.acceleration.LogicalPlanDeserializer;
import com.dremio.exec.planner.acceleration.LogicalPlanSerializer;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginInstanceRulesFactory;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.StoragePluginTypeRulesFactory;
import com.dremio.exec.store.Views;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.StoragePluginType;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.protostuff.ByteString;

/**
 * A plugin that exposes existing materializations as materialized view tables for testing.
 *
 * Materialized view table serializes and deserializes the given logical plan.
 */
public class MaterializationTestStoragePlugin implements StoragePlugin {

  private static final Logger logger = LoggerFactory.getLogger(MaterializationTestStoragePlugin.class);

  private final static StoragePluginType TYPE = new StoragePluginType("materialization-test", Factory.class);

  private final String name;
  private final StoragePluginId pluginId;
  private final StoragePluginRegistry registry;
  private final Cache<String, RelNode> plans = CacheBuilder.newBuilder()
      .expireAfterWrite(30, TimeUnit.SECONDS)
      .build();

  /**
   * Constructor signature is required by the initialization mechanism
   * @param config
   * @param context
   * @param name
   */
  public MaterializationTestStoragePlugin(
      final MaterializationTestStoragePluginConfig config,
      final SabotContext context,
      final String name) throws ExecutionSetupException {
    this.name = name;
    this.registry = context.getStorage();
    this.pluginId = new StoragePluginId(name, config, TYPE);
  }

  @Override
  public ViewTable getView(final List<String> path, final SchemaConfig schemaConfig) {
    if (path.size() != 2) {
      logger.debug("path must consists of 2 segments [pluginName, layoutId]. got {}",
          MoreObjects.toStringHelper(this).add("path", path));
      return null;
    }

    final String layoutId = path.get(1);
    final Optional<RelNode> layout = Optional.fromNullable(plans.getIfPresent(layoutId));
    if (!layout.isPresent()) {
      return null;
    }
    return new MaterializedTestViewTable(layout.get(), schemaConfig.getUserName(), schemaConfig.getViewExpansionContext());
  }

  public void put(final String layoutId, final RelNode plan) {
    plans.put(layoutId, plan);
  }


  class MaterializedTestViewTable extends ViewTable {
    private final RelNode plan;
    private final View view;

    public MaterializedTestViewTable(final RelNode plan, final String username, final ViewExpansionContext expansionContext) {
      super(null, username, expansionContext);
      this.plan = plan;
      final List<ViewFieldType> fields = Views.viewToFieldTypes(Views.relDataTypeToFieldType(plan.getRowType()));
      view = Views.fieldTypesToView("materialized-view", "materialized-view-sql", fields, ImmutableList.<String>of());
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      return view.getRowType(typeFactory);
    }

    @Override
    public View getView() {
      return view;
    }

    @Override
    public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable relOptTable) {
      // this value has been set upon creating PlannerSettings in QueryContext
      final RelOptCluster cluster = context.getCluster();
      final RelOptPlanner planner = cluster.getPlanner();
      final CalciteCatalogReader catalog = planner.getContext().unwrap(CalciteCatalogReader.class);

      final LogicalPlanSerializer serializer = KryoLogicalPlanSerializers.forSerialization(cluster);
      final byte[] planBytes = serializer.serialize(plan);
      final LogicalPlanDeserializer deserializer = KryoLogicalPlanSerializers.forDeserialization(cluster, catalog, registry);
      final RelNode newPlan = deserializer.deserialize(planBytes);
      return newPlan;
    }

    @Override
    public String getViewSql() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) throws Exception {
    return ImmutableList.of();
  }

  @Override
  public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldDataset, boolean ignoreAuthErrors) throws Exception {
    return null;
  }

  @Override
  public boolean containerExists(NamespaceKey key) {
    return false;
  }

  @Override
  public boolean datasetExists(NamespaceKey key) {
    return false;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return false;
  }

  @Override
  public SourceState getState() {
    return SourceState.GOOD;
  }

  @Override
  public StoragePluginId getId() {
    return pluginId;
  }

  @Override
  public Class<? extends StoragePluginInstanceRulesFactory> getRulesFactoryClass() {
    return null;
  }

  @Override
  public CheckResult checkReadSignature(ByteString key, DatasetConfig datasetConfig) throws Exception {
    return CheckResult.UNCHANGED;
  }

  @Override
  public void start() throws IOException {
  }

  public static final class Factory implements StoragePluginTypeRulesFactory {

    @Override
    public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, StoragePluginType pluginType) {
      return ImmutableSet.of();
    }

  }

}
