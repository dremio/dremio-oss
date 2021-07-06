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
package com.dremio.exec.planner.sql.handlers.refresh;

import java.io.IOException;

import org.apache.calcite.tools.RelConversionException;

import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingInvocationPrel;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceException;

public class RefreshDatasetIncrementalPlanBuilder extends RefreshDatasetPlanBuilder {

  public RefreshDatasetIncrementalPlanBuilder(SqlHandlerConfig config, SqlRefreshDataset sqlNode) throws IOException, NamespaceException {
    super(config, sqlNode);
    if (sqlNode.isPartialRefresh()) {
      if (!config.getContext().getOptions().getOption(CatalogOptions.DMP_METADATA_REFRESH_PARTIAL)) {
        throw new UnsupportedOperationException("Partial refresh options aren't enabled for the REFRESH METADATA command.");
      }
    }
  }

  @Override
  protected Prel getDataFileListingPrel() throws RelConversionException {
    final FileSystemPlugin<?> metaStoragePlugin = config.getContext().getCatalogService().getSource(METADATA_STORAGEPLUGIN_NAME);
    return new DirListingInvocationPrel(cluster, cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL),
      table, storagePluginId, refreshExecTableMetadata,
      1.0d, metaStoragePlugin, metadataProvider.getTableUUId());
  }

  @Override
  protected IcebergTableProps getWriterConfig() {
    final String tableMappingPath = Path.of(metaStoragePlugin.getConfig().getPath().toString()).resolve(metadataProvider.getTableUUId()).toString();

    // If dist is local, ensure it is prefixed with the scheme in the dremio.conf - `file:///`
    logger.info("Updating metadata for {} at {}", tableNSKey, tableMappingPath);

    final IcebergTableProps icebergTableProps = new IcebergTableProps(tableMappingPath, metadataProvider.getTableUUId(),
      metadataProvider.getTableSchema(), metadataProvider.getPartitionColumn(), IcebergCommandType.INCREMENTAL_METADATA_REFRESH, tableNSKey.getSchemaPath());
    icebergTableProps.setDetectSchema(true);
    icebergTableProps.setMetadataRefresh(true);
    return icebergTableProps;
  }
}
