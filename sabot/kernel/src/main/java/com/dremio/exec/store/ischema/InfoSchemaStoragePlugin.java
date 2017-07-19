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
package com.dremio.exec.store.ischema;

import static com.dremio.exec.store.ischema.InfoSchemaConstants.IS_SCHEMA_NAME;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.common.OldScanRelBase;
import com.dremio.exec.planner.logical.ConvertibleScan;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.AbstractSchema;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.ConversionContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePluginOptimizerRule;
import com.dremio.exec.util.ImpersonationUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class InfoSchemaStoragePlugin extends AbstractStoragePlugin<InfoSchemaStoragePlugin.InfoSchemaConversionContext> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaStoragePlugin.class);

  private final InfoSchemaConfig config;
  private final SabotContext context;
  private final String name;

  public InfoSchemaStoragePlugin(InfoSchemaConfig config, SabotContext context, String name){
    this.config = config;
    this.context = context;
    this.name = name;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public InfoSchemaGroupScan getPhysicalScan(String userName, JSONOptions selection, List<String> tableSchemaPath, List<SchemaPath> columns)
      throws IOException {
    InfoSchemaTableType table = selection.getWith(context.getLpPersistence(),  InfoSchemaTableType.class);
    return new InfoSchemaGroupScan(table, null, context);
  }

  @Override
  public boolean folderExists(SchemaConfig schemaConfig, List folderPath) throws IOException {
    return false;
  }

  @Override
  public StoragePluginConfig getConfig() {
    return this.config;
  }

  public SabotContext getContext(){
    return context;
  }

  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    ISchema s = new ISchema(parent, this);
    parent.add(s.getName(), s);
  }

  @Override
  public RelNode getRel(final RelOptCluster cluster, final RelOptTable relOptTable, final InfoSchemaConversionContext relContext) {
    try {
      final String username = ImpersonationUtil.getProcessUserName();
      final GroupScan scan = getPhysicalScan(username, new JSONOptions(relContext.getSelection()),
          relContext.getFullPath(), OldAbstractGroupScan.ALL_COLUMNS);
      return new OldScanCrel(cluster, relOptTable, cluster.traitSetOf(Convention.NONE), relContext.getDataType(), scan, null, OldScanRelBase.DEFAULT_ROW_COUNT_DISCOUNT);
    } catch (IOException e) {
      throw new RuntimeException("unable to create group scan", e);
    }
  }

  /**
   * Representation of the INFORMATION_SCHEMA schema.
   */
  private class ISchema extends AbstractSchema{
    private final Map<String, InfoSchemaTable> tables;

    public ISchema(SchemaPlus parent, InfoSchemaStoragePlugin plugin){
      super(ImmutableList.<String>of(), IS_SCHEMA_NAME);
      Map<String, InfoSchemaTable> tbls = Maps.newHashMap();
      for(InfoSchemaTableType tbl : InfoSchemaTableType.values()){
        tbls.put(tbl.name(), new InfoSchemaTable(plugin, tbl));
      }
      this.tables = ImmutableMap.copyOf(tbls);
    }

    @Override
    public Table getTable(String name) {
      return tables.get(name);
    }

    @Override
    public Set<String> getTableNames() {
      return tables.keySet();
    }

    @Override
    public String getTypeName() {
      return InfoSchemaConfig.NAME;
    }
  }

  public static class InfoSchemaTable implements TranslatableTable {
    private final InfoSchemaStoragePlugin plugin;
    private final InfoSchemaTableType tableType;

    public InfoSchemaTable(final InfoSchemaStoragePlugin plugin, final InfoSchemaTableType tableType) {
      this.plugin = plugin;
      this.tableType = tableType;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return tableType.getRowType(typeFactory);
    }

    @Override
    public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable relOptTable) {
      final List<String> fullPath = ImmutableList.of(plugin.name, tableType.name());
      final RelDataType dataType = getRowType(context.getCluster().getTypeFactory());
      final InfoSchemaConversionContext relContext = new InfoSchemaConversionContext(tableType, fullPath, dataType);
      return new ConvertibleScan(context.getCluster(), context.getCluster().traitSet(), relOptTable, plugin, relContext);
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

  public static class InfoSchemaConversionContext extends ConversionContext {
    private final InfoSchemaTableType selection;
    private final List<String> fullPath;
    private final RelDataType dataType;

    public InfoSchemaConversionContext(final InfoSchemaTableType selection, final List<String> fullPath, final RelDataType dataType) {
      this.selection = selection;
      this.fullPath = fullPath;
      this.dataType = dataType;
    }

    public InfoSchemaTableType getSelection() {
      return selection;
    }

    public List<String> getFullPath() {
      return fullPath;
    }

    public RelDataType getDataType() {
      return dataType;
    }
  }


  @Override
  public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return ImmutableSet.of(
        InfoSchemaPushFilterIntoRecordGenerator.IS_FILTER_ON_PROJECT,
        InfoSchemaPushFilterIntoRecordGenerator.IS_FILTER_ON_SCAN);
  }
}
