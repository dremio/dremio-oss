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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers;
import com.dremio.exec.planner.acceleration.LogicalPlanDeserializer;
import com.dremio.exec.planner.acceleration.LogicalPlanSerializer;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.ConversionContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.Views;
import com.dremio.service.namespace.TableInstance;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;

/**
 * A plugin that exposes existing materializations as materialized view tables for testing.
 *
 * Materialized view table serializes and deserializes the given logical plan.
 */
public class MaterializationTestStoragePlugin extends AbstractStoragePlugin<ConversionContext.NamespaceConversionContext> {

  private static final Logger logger = LoggerFactory.getLogger(MaterializationTestStoragePlugin.class);

  private final MaterializationTestStoragePluginConfig config;
  private final String pluginName;
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
  public MaterializationTestStoragePlugin(final MaterializationTestStoragePluginConfig config, final SabotContext context,
                                          final String name) throws ExecutionSetupException {
    this.config = config;
    this.pluginName = name;
    this.registry = context.getStorage();
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public RelNode getRel(final RelOptCluster cluster, final RelOptTable relOptTable, final ConversionContext.NamespaceConversionContext relContext) {
    throw new UnsupportedOperationException();
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

  @Override
  public List<DatasetConfig> listDatasets() {
    return Collections.emptyList();
  }

  @Override
  public boolean folderExists(final SchemaConfig schemaConfig, final List<String> folderPath) throws IOException {
    return false;
  }

  @Override
  public DatasetConfig getDataset(final List<String> tableSchemaPath, final TableInstance tableInstance, final SchemaConfig schemaConfig) {
    return null;
  }

  @Override
  public boolean supportsRead() {
    return true;
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

}
