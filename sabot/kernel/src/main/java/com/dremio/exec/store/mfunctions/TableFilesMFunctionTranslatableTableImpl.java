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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.MFunctionCatalogMetadata;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.tablefunctions.MFunctionTranslatableTable;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Suppliers;

/**
 * Metadata provider for table functions as table_files
 */
public class TableFilesMFunctionTranslatableTableImpl extends MFunctionTranslatableTable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableFilesMFunctionTranslatableTableImpl.class);

  /*
   * Underlying Table config. It should provide info like metadata location, tablePath
   */
  private final DatasetConfig underlyingTableConfig;
  private final DatasetHandle handle;
  private final StoragePlugin plugin;
  private final DatasetRetrievalOptions options;
  private static final int DEFAULT_RECORD_COUNT = 100_000;

  public TableFilesMFunctionTranslatableTableImpl(
    MFunctionCatalogMetadata catalogMetadata,
    DatasetConfig underlyingTableConfig,
    DatasetHandle handle,
    StoragePlugin plugin,
    DatasetRetrievalOptions options,
    String user,
    boolean complexTypeSupport
  ) {
    super(catalogMetadata, user, complexTypeSupport);
    this.underlyingTableConfig = underlyingTableConfig;
    this.handle = handle;
    this.plugin = plugin;
    this.options = options;
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
  public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return false;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final Supplier<PartitionChunkListing> partitionChunkListing = Suppliers.memoize(this::getPartitionChunkListing);
    DatasetConfig config = createDatasetConfig(partitionChunkListing);
    long splitVersion = Optional.of(config)
      .map(DatasetConfig::getReadDefinition)
      .map(ReadDefinition::getSplitVersion)
      .orElse(0L);
    SplitsPointer splitsPointer = MaterializedSplitsPointer.oldObsoleteOf(splitVersion, toPartitionChunks(partitionChunkListing),1);
    return new ScanCrel(
      context.getCluster(),
      context.getCluster().traitSetOf(Convention.NONE),
      catalogMetadata.getStoragePluginId(),
      new TableFilesFunctionTableMetadata(catalogMetadata.getStoragePluginId(), config, user, catalogMetadata.getBatchSchema(), splitsPointer),
      null,
      1.0d, false, false);
  }

  private PartitionChunkListing getPartitionChunkListing() {
    try {
      return plugin.listPartitionChunks(handle, options.asListPartitionChunkOptions(underlyingTableConfig));
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }
  }

  private List<PartitionProtobuf.PartitionChunk> toPartitionChunks(Supplier<PartitionChunkListing> listingSupplier) {
    final Iterator<? extends PartitionChunk> chunks = listingSupplier.get()
      .iterator();

    final List<PartitionProtobuf.PartitionChunk> toReturn = new ArrayList<>();
    int i = 0;
    while (chunks.hasNext()) {
      final PartitionChunk chunk = chunks.next();
      toReturn.addAll(MetadataObjectsUtils.newPartitionChunk(i + "-", chunk)
        .collect(Collectors.toList()));
      i++;
    }

    return toReturn;
  }

  /**
   * @return new dataset config with existing icebergMetadata and FileConfig
   */
  private DatasetConfig createDatasetConfig(Supplier<PartitionChunkListing> listingSupplier) {
    final DatasetConfig toReturn = MetadataObjectsUtils.newShallowConfig(handle);
    toReturn.setName("table_files" + "(" + (handle.getDatasetPath().getName() + ")")); //eg -> table_files('table')
    final DatasetMetadata datasetMetadata;
    try {
      datasetMetadata = plugin.getDatasetMetadata(handle, listingSupplier.get(),
        options.asGetMetadataOptions(null));
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
        .buildSilently();
    }

    MetadataObjectsUtils.overrideExtended(toReturn, datasetMetadata, Optional.empty(),
      DEFAULT_RECORD_COUNT, options.maxMetadataLeafColumns());
    //For iceberg, data files contents fetched from Manifest file which is of AVRO type.
    //To understand the datasetConfig correctly it should be set to correct fileType instead of native iceberg.
    if (toReturn.getPhysicalDataset().getFormatSettings() != null) {
      toReturn.getPhysicalDataset().getFormatSettings().setType(FileType.AVRO);
    }
    //Set recordCount with DataFiles , which is present with manifestStats
    final long datasetRecordCount = datasetMetadata.getManifestStats().getRecordCount();
    toReturn.getReadDefinition().setScanStats(new ScanStats()
      .setScanFactor(datasetMetadata.getManifestStats().getScanFactor())
      .setType(datasetMetadata.getManifestStats().isExactRecordCount() ? ScanStatsType.EXACT_ROW_COUNT : ScanStatsType.NO_EXACT_ROW_COUNT)
      .setRecordCount(datasetRecordCount >= 0 ? datasetRecordCount : DEFAULT_RECORD_COUNT));
    return toReturn;
  }
}
