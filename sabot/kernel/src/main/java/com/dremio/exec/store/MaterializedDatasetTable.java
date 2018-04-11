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
package com.dremio.exec.store;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
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
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.NamespaceTable.StatisticImpl;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.QuietAccessor;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;

/**
 * DatasetTable that is used for table with options.
 */
public class MaterializedDatasetTable implements TranslatableTable {

  private final QuietAccessor datasetAccessor;
  private final FileSystemPlugin plugin;
  private final String user;

  public MaterializedDatasetTable(FileSystemPlugin plugin, String user, QuietAccessor datasetAccessor) {
    this.datasetAccessor = datasetAccessor;
    this.plugin = plugin;
    this.user = user;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new ScanCrel(
        context.getCluster(),
        context.getCluster().traitSetOf(Convention.NONE),
        plugin.getId(),
        new MaterializedTableMetadata(plugin.getId(), datasetAccessor.getDataset(), user, datasetAccessor.getSplits()),
        null,
        1.0d,
        true);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return BatchSchema.fromDataset(datasetAccessor.getDataset()).toCalciteRecordType(typeFactory, NamespaceTable.SYSTEM_COLUMNS);
  }

  @Override
  public Statistic getStatistic() {
    return new StatisticImpl() {
      @Override
      public Double getRowCount() {
        return (double) datasetAccessor.getDataset().getReadDefinition().getScanStats().getRecordCount();
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
                                      List<DatasetSplit> splits) {
      super(plugin, config, user, MaterializedSplitsPointer.of(splits, splits.size()));
    }

    /**
     * Don't prune based on lucene query
     * @param partitionFilterQuery
     * @return
     * @throws NamespaceException
     */
    @Override
    public TableMetadata prune(SearchTypes.SearchQuery partitionFilterQuery) throws NamespaceException {
      return this;
    }
  }

}
