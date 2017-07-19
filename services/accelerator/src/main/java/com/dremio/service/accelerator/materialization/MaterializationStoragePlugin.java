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
package com.dremio.service.accelerator.materialization;

import static com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.UPDATE_COLUMN;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.ColumnMaterializationShuttle;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.FileMaterializationShuttle;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers.KryoDeserializationException;
import com.dremio.exec.planner.acceleration.LogicalPlanDeserializer;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.ConversionContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.Views;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.namespace.TableInstance;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * A plugin that exposes existing materializations as materialized view tables.
 *
 * Materialized view table converts serialized logical plan to {@link RelNode}.
 */
public class MaterializationStoragePlugin extends AbstractStoragePlugin<ConversionContext.NamespaceConversionContext> {

  private static final Logger logger = LoggerFactory.getLogger(MaterializationStoragePlugin.class);

  private final MaterializationStoragePluginConfig config;
  private final Supplier<AccelerationStore> accelerationStore;
  private final Supplier<MaterializationStore> materializationStore;
  private final StoragePluginRegistry registry;
  private final AccelerationManager accelerationManager;

  /**
   * Constructor signature is required by the initialization mechanism
   * @param config
   * @param context
   * @param name
   */
  public MaterializationStoragePlugin(final MaterializationStoragePluginConfig config, final SabotContext context, String name) throws ExecutionSetupException {
    this.config = config;
    this.accelerationManager = context.getAccelerationManager();
    final Provider<KVStoreProvider> kvStoreProvider = new Provider<KVStoreProvider>() {
      @Override
      public KVStoreProvider get() {
        return context.getKVStoreProvider();
      }
    };
    this.accelerationStore = Suppliers.memoize(new Supplier<AccelerationStore>() {
      @Override
      public AccelerationStore get() {
        final AccelerationStore store = new AccelerationStore(kvStoreProvider);
        store.start();
        return store;
      }
    });
    this.materializationStore = Suppliers.memoize(new Supplier<MaterializationStore>() {
      @Override
      public MaterializationStore get() {
        final MaterializationStore store = new MaterializationStore(kvStoreProvider);
        store.start();
        return store;
      }
    });
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

    final LayoutId layoutId = new LayoutId(path.get(1));
    final Optional<Layout> layout = accelerationStore.get().getLayoutById(layoutId);
    if (!layout.isPresent()) {
      return null;
    }
    return new MaterializedViewTable(layout.get(), schemaConfig.getUserName(), schemaConfig.getViewExpansionContext());
  }

  @Override
  public List<DatasetConfig> listDatasets() {
    return Collections.emptyList();
  }

  @Override
  public boolean folderExists(final SchemaConfig schemaConfig, final List<String> folderPath) throws IOException {
    // This should all be in namespace. Okay to respond negatively because there is no such thing as fall through.
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


  class MaterializedViewTable extends ViewTable {
    private final Layout layout;
    private final View view;

    public MaterializedViewTable(final Layout layout, final String username, final ViewExpansionContext expansionContext) {
      super(null, username, expansionContext);
      this.layout = layout;
      List<ViewFieldType> fieldList;
      ImmutableList.Builder<ViewFieldType> builder = ImmutableList.<ViewFieldType>builder().addAll(layout.getLayoutSchema().getFieldList());
      if (layout.getIncremental()) {
        builder.add(new ViewFieldType(UPDATE_COLUMN, SqlTypeName.BIGINT.getName()));
      }
      fieldList = builder.build();
      view =  Views.fieldTypesToView("materialized-view", "materialized-view-sql", fieldList,
          ImmutableList.<String>of());
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
      final LogicalPlanDeserializer deserializer = KryoLogicalPlanSerializers.forDeserialization(cluster, catalog, registry);
      try {
        final RelNode node = deserializer.deserialize(layout.getLogicalPlan().toByteArray());
        if (layout.getIncremental()) {
          RelShuttle shuttle = getMaterializationShuttle(layout);
          return node.accept(shuttle);
        } else {
          return node;
        }
      } catch (KryoDeserializationException e) {
        accelerationManager.replanlayout(layout.getId().getId());
        throw e;
      }
    }

    @Override
    public String getViewSql() {
      throw new UnsupportedOperationException();
    }
  }

  private RelShuttle getMaterializationShuttle(Layout layout) {
    if (layout.getRefreshField() != null) {
      return getColumnBasedMaterializationShuttle(layout);
    } else {
      return getTimestampBasedMaterializationShuttle(layout);
    }
  }

  private RelShuttle getColumnBasedMaterializationShuttle(final Layout layout) {
    final ColumnMaterializationShuttle shuttle = new ColumnMaterializationShuttle(layout.getRefreshField(), Long.MIN_VALUE);
    return materializationStore.get()
        .get(layout.getId())
        .transform(new Function<MaterializedLayout, RelShuttle>() {
          @Nullable
          @Override
          public RelShuttle apply(@Nullable final MaterializedLayout input) {
            if (AccelerationUtils.selfOrEmpty(input.getMaterializationList()).isEmpty()) {
              return shuttle;
            }

            final Long updateId = FluentIterable
                .from(AccelerationUtils.selfOrEmpty(input.getMaterializationList()))
                .toSortedList(new Comparator<Materialization>() {
                  @Override
                  public int compare(Materialization o1, Materialization o2) {
                    // sorting in reverse order
                    return o2.getUpdateId().compareTo(o1.getUpdateId());
                  }
                }).reverse().get(0).getUpdateId();

            if (updateId == null) {
              return shuttle;
            }

            return new ColumnMaterializationShuttle(layout.getRefreshField(), updateId);
          }
        })
        .or(shuttle);
  }

  private RelShuttle getTimestampBasedMaterializationShuttle(final Layout layout) {
    final RelShuttle shuttle = new FileMaterializationShuttle(Long.MIN_VALUE);
    return materializationStore.get().get(layout.getId())
        .transform(new Function<MaterializedLayout, RelShuttle>() {
          @Nullable
          @Override
          public RelShuttle apply(@Nullable final MaterializedLayout input) {
            if (AccelerationUtils.selfOrEmpty(input.getMaterializationList()).isEmpty()) {
              return shuttle;
            }
            final Long updateId = FluentIterable.from(input.getMaterializationList())
                .toSortedList(new Comparator<Materialization>() {
                  @Override
                  public int compare(Materialization o1, Materialization o2) {
                    // sorting in reverse order
                    return o2.getUpdateId().compareTo(o1.getUpdateId());
                  }
                }).get(0).getUpdateId();

            if (updateId == null) {
              return shuttle;
            }

            return new FileMaterializationShuttle(updateId);
          }
        })
        .or(shuttle);
  }

}
