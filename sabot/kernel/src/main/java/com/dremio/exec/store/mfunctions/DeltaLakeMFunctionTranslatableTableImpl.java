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
package com.dremio.exec.store.mfunctions;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.MFunctionMetadata;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.store.MFunctionCatalogMetadata;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.deltalake.DeltaConstants;
import com.dremio.exec.store.deltalake.DeltaLakeHistoryScanTableMetadata;
import com.dremio.exec.store.deltalake.DeltaVersion;
import com.dremio.exec.store.deltalake.DeltaVersionResolver;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.tablefunctions.MFunctionTranslatableTable;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.UUID;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

/** Metadata provider for table functions as table_files */
public final class DeltaLakeMFunctionTranslatableTableImpl extends MFunctionTranslatableTable {

  private final DatasetConfig underlyingTableConfig;
  private final StoragePlugin storagePlugin;

  public DeltaLakeMFunctionTranslatableTableImpl(
      MFunctionCatalogMetadata catalogMetadata, MFunctionMetadata mFunctionMetadata) {
    super(catalogMetadata, mFunctionMetadata.getSchemaConfig().getUserName(), true);
    Preconditions.checkArgument(mFunctionMetadata.getCurrentConfig() != null);
    this.underlyingTableConfig = mFunctionMetadata.getCurrentConfig();
    this.storagePlugin = mFunctionMetadata.getPlugin();
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(
      String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return false;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    DatasetConfig config = createDatasetConfigUsingUnderlyingConfig();
    long rowCountEstimate = getRowCountEstimate();
    return new ScanCrel(
        context.getCluster(),
        context.getCluster().traitSetOf(Convention.NONE),
        catalogMetadata.getStoragePluginId(),
        DeltaLakeHistoryScanTableMetadata.create(catalogMetadata, config, user, rowCountEstimate),
        null,
        1.0d,
        ImmutableList.of(),
        false,
        false);
  }

  private DatasetConfig createDatasetConfigUsingUnderlyingConfig() {
    final DatasetConfig shallowConfig = new DatasetConfig();

    shallowConfig.setId(new EntityId().setId(UUID.randomUUID().toString()));
    shallowConfig.setCreatedAt(System.currentTimeMillis());
    shallowConfig.setName(underlyingTableConfig.getName());
    shallowConfig.setFullPathList(underlyingTableConfig.getFullPathList());
    shallowConfig.setType(underlyingTableConfig.getType());
    shallowConfig.setSchemaVersion(underlyingTableConfig.getSchemaVersion());
    shallowConfig.setRecordSchema(catalogMetadata.getBatchSchema().toByteString());

    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setScanStats(underlyingTableConfig.getReadDefinition().getManifestScanStats());
    readDefinition.setManifestScanStats(
        underlyingTableConfig.getReadDefinition().getManifestScanStats());
    readDefinition.setExtendedProperty(
        underlyingTableConfig.getReadDefinition().getExtendedProperty());
    shallowConfig.setReadDefinition(readDefinition);

    shallowConfig.setPhysicalDataset(underlyingTableConfig.getPhysicalDataset());
    if (shallowConfig.getPhysicalDataset().getFormatSettings() != null) {
      shallowConfig.getPhysicalDataset().getFormatSettings().setType(FileType.JSON);
    }

    return shallowConfig;
  }

  private long getRowCountEstimate() {
    if (storagePlugin instanceof SupportsIcebergRootPointer) {
      String tableLocation =
          underlyingTableConfig.getPhysicalDataset().getFormatSettings().getLocation();
      try (FileSystem fs =
          ((SupportsIcebergRootPointer) storagePlugin).createFS(tableLocation, user, null)) {
        Path metadataDirPath = Path.of(tableLocation).resolve(DeltaConstants.DELTA_LOG_DIR);
        DeltaVersionResolver resolver = new DeltaVersionResolver(fs, metadataDirPath);
        DeltaVersion version = resolver.getLastCheckpoint();
        return version.getVersion() > 0 ? version.getVersion() : DremioCost.LARGE_FILE_COUNT;
      } catch (Exception e) {
        throw UserException.validationError(e).buildSilently();
      }
    }
    return DremioCost.LARGE_FILE_COUNT;
  }
}
