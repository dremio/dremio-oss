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
package com.dremio.exec.store;

import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;

import com.dremio.datastore.SearchTypes;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.store.NamespaceTable.StatisticImpl;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

/**
 * DatasetTable that is used for table with options.
 */
public class MaterializedDatasetTable implements TranslatableTable {

  private final Supplier<DatasetConfig> datasetConfig;
  private final Supplier<List<PartitionChunk>> partitionChunks;
  private final FileSystemPlugin plugin;
  private final String user;
  private final boolean complexTypeSupport;

  public MaterializedDatasetTable(
      FileSystemPlugin plugin,
      String user,
      Supplier<DatasetConfig> datasetConfig,
      Supplier<List<PartitionChunk>> partitionChunks,
      boolean complexTypeSupport
  ) {
    this.datasetConfig = datasetConfig;
    this.partitionChunks = partitionChunks;
    this.plugin = plugin;
    this.user = user;
    this.complexTypeSupport = complexTypeSupport;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new ScanCrel(
        context.getCluster(),
        context.getCluster().traitSetOf(Convention.NONE),
        plugin.getId(),
        new MaterializedTableMetadata(plugin.getId(), datasetConfig.get(), user, partitionChunks.get()),
        null,
        1.0d,
        true);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return CalciteArrowHelper.wrap(CalciteArrowHelper.fromDataset(datasetConfig.get()))
        .toCalciteRecordType(typeFactory, (Field f) -> !NamespaceTable.SYSTEM_COLUMNS.contains(f.getName()), complexTypeSupport);
  }

  @Override
  public Statistic getStatistic() {
    return new StatisticImpl() {
      @Override
      public Double getRowCount() {
        return (double) datasetConfig.get().getReadDefinition().getScanStats().getRecordCount();
      }

      @Override
      public List<RelReferentialConstraint> getReferentialConstraints() {
        return ImmutableList.of();
      }
    };
  }


  @Override
  public TableType getJdbcTableType() {
    return TableType.TABLE;
  }


  private static class MaterializedTableMetadata extends TableMetadataImpl {

    public MaterializedTableMetadata(StoragePluginId plugin,
                                      DatasetConfig config,
                                      String user,
                                      List<PartitionChunk> splits) {
      super(plugin, config, user, MaterializedSplitsPointer.oldObsoleteOf(getSplitVersion(config), splits, splits.size()));
    }

    private static long getSplitVersion(DatasetConfig datasetConfig) {
      return Optional.ofNullable(datasetConfig)
          .map(DatasetConfig::getReadDefinition)
          .map(ReadDefinition::getSplitVersion)
          .orElse(0L);
    }

    @Override
    public TableMetadata prune(SearchTypes.SearchQuery partitionFilterQuery) {
      // Don't prune based on lucene query
      return this;
    }
  }
}
