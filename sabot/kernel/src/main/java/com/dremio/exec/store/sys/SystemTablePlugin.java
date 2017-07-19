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
package com.dremio.exec.store.sys;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;

import com.dremio.common.JSONOptions;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.calcite.logical.OldScanCrel;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.common.OldScanRelBase;
import com.dremio.exec.planner.logical.ConvertibleScan;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.AbstractSchema;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.ConversionContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.pojo.PojoDataType;
import com.dremio.exec.util.ImpersonationUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * A "storage" plugin for system tables.
 */
public class SystemTablePlugin extends AbstractStoragePlugin<SystemTablePlugin.StaticTableConversionContext> {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemTablePlugin.class);

  public static final String SYS_SCHEMA_NAME = "sys";

  private final SabotContext context;
  private final String name;
  private final SystemTablePluginConfig config;
  private final SystemSchema schema = new SystemSchema();

  public SystemTablePlugin(SystemTablePluginConfig config, SabotContext context, String name) {
    this.config = config;
    this.context = context;
    this.name = name;
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    parent.add(schema.getName(), schema);
  }

  @JsonIgnore
  public SabotContext getContext() {
    return this.context;
  }

  @Override
  public boolean folderExists(SchemaConfig schemaConfig, List folderPath) throws IOException {
    return false;
  }

  @Override
  public OldAbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<String> tableSchemaPath, List<SchemaPath> columns)
      throws IOException {
    SystemTable table = selection.getWith(context.getLpPersistence(), SystemTable.class);
    return new SystemTableScan(table, this);
  }

  @Override
  public RelNode getRel(final RelOptCluster cluster, final RelOptTable relOptTable, final StaticTableConversionContext relContext) {
    try {
      final String username = ImpersonationUtil.getProcessUserName();
      final GroupScan scan = getPhysicalScan(username, new JSONOptions(relContext.getSelection()),
          relContext.getFullPath(), OldAbstractGroupScan.ALL_COLUMNS);
      return new OldScanCrel(cluster, relOptTable, cluster.traitSetOf(Convention.NONE), relContext.getDataType(), scan, null, OldScanRelBase.DEFAULT_ROW_COUNT_DISCOUNT);
    } catch (final IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * This class defines a namespace for {@link com.dremio.exec.store.sys.SystemTable}s
   */
  private class SystemSchema extends AbstractSchema {

    private final Set<String> tableNames;

    public SystemSchema() {
      super(ImmutableList.<String>of(), SYS_SCHEMA_NAME);
      Set<String> names = Sets.newHashSet();
      for (SystemTable t : SystemTable.values()) {
        names.add(t.getTableName());
      }
      this.tableNames = ImmutableSet.copyOf(names);
    }

    @Override
    public Set<String> getTableNames() {
      return tableNames;
    }

    @Override
    public Table getTable(String name) {
      for (SystemTable table : SystemTable.values()) {
        if (table.getTableName().equalsIgnoreCase(name)) {
          return new StaticTable(SystemTablePlugin.this, table);
        }
      }
      return null;
    }

    @Override
    public String getTypeName() {
      return SystemTablePluginConfig.NAME;
    }

  }

  public static class StaticTable implements TranslatableTable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StaticTable.class);

    private final SystemTablePlugin plugin;
    private final SystemTable table;
    private final PojoDataType dataType;

    public StaticTable(final SystemTablePlugin plugin, final SystemTable table) {
      this.plugin = plugin;
      this.table = table;
      this.dataType = new PojoDataType(table.getPojoClass());
    }

    @Override
    public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable relOptTable) {
      final List<String> fullPath = ImmutableList.of(plugin.name, table.getTableName());
      final RelDataType dataType = getRowType(context.getCluster().getTypeFactory());
      final StaticTableConversionContext relContext = new StaticTableConversionContext(table, fullPath, dataType);
      return new ConvertibleScan(context.getCluster(), context.getCluster().traitSet(), relOptTable, plugin, relContext);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      return dataType.getRowType(typeFactory);
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.SYSTEM_TABLE;
    }

  }

  public static class StaticTableConversionContext extends ConversionContext {
    private final SystemTable selection;
    private final List<String> fullPath;
    private final RelDataType dataType;

    public StaticTableConversionContext(final SystemTable selection, final List<String> fullPath,
                                        final RelDataType dataType) {
      this.selection = selection;
      this.fullPath = fullPath;
      this.dataType = dataType;
    }

    public RelDataType getDataType() {
      return dataType;
    }

    public List<String> getFullPath() {
      return fullPath;
    }

    public SystemTable getSelection() {
      return selection;
    }
  }

}
