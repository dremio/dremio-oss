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
package com.dremio.plugins.icebergcatalog.store;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.hadoop.HadoopFileSystemConfigurationAdapter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.planner.sql.handlers.refresh.UnlimitedSplitsMetadataProvider;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.IcebergFormatConfig;
import com.dremio.exec.store.iceberg.IcebergFormatPlugin;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingRecordReader;
import com.dremio.exec.store.metadatarefresh.footerread.FooterReadTableFunction;
import com.dremio.exec.store.parquet.ParquetScanTableFunction;
import com.dremio.exec.store.parquet.ParquetSplitCreator;
import com.dremio.exec.store.parquet.ScanTableFunction;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionManager;
import com.dremio.plugins.icebergcatalog.dfs.HadoopFileSystemCache;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;

public class IcebergCatalogPlugin
    implements StoragePlugin,
        SupportsIcebergRootPointer,
        SupportsInternalIcebergTable,
        SupportsListingDatasets {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(IcebergCatalogPlugin.class);

  private final IcebergCatalogPluginConfig config;
  private final SabotContext sabotContext;
  private final String name;
  private final Configuration fsConf;
  private final HadoopFileSystemCache hadoopFs;
  private CatalogAccessor catalogAccessor = null;
  private final AtomicBoolean isOpen = new AtomicBoolean(false);

  public IcebergCatalogPlugin(
      IcebergCatalogPluginConfig config, SabotContext sabotContext, String name) {
    this.config = config;
    this.sabotContext = sabotContext;
    this.name = name;
    this.fsConf = FileSystemPlugin.getNewFsConf();
    this.hadoopFs = new HadoopFileSystemCache(sabotContext.getOptionManager());
  }

  @VisibleForTesting
  protected IcebergCatalogPlugin(
      IcebergCatalogPluginConfig config,
      SabotContext sabotContext,
      String name,
      HadoopFileSystemCache hadoopFs) {
    this.config = config;
    this.sabotContext = sabotContext;
    this.name = name;
    this.fsConf = FileSystemPlugin.getNewFsConf();
    this.hadoopFs = hadoopFs;
  }

  public String getName() {
    return name;
  }

  public CatalogAccessor getCatalogAccessor() {
    if (!isOpen.get()) {
      throw UserException.sourceInBadState()
          .message("Iceberg Catalog Source %s is either not started or already closed", getName())
          .addContext("name", getName())
          .buildSilently();
    }
    return catalogAccessor;
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
    return getCatalogAccessor().listDatasetHandles(getName(), this);
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(
      EntityPath datasetPath, GetDatasetOption... options) {
    List<String> components = datasetPath.getComponents();
    if (components.size() < 3) {
      return Optional.empty();
    }

    if (getCatalogAccessor().datasetExists(components)) {
      return Optional.of(getCatalogAccessor().getDatasetHandle(components, this, options));
    }

    logger.warn("DatasetHandle '{}' empty, table not found.", datasetPath);
    return Optional.empty();
  }

  @Override
  public PartitionChunkListing listPartitionChunks(
      DatasetHandle datasetHandle, ListPartitionChunkOption... options) {

    IcebergCatalogTableProvider icebergTableProvider =
        datasetHandle.unwrap(IcebergCatalogTableProvider.class);
    return icebergTableProvider.listPartitionChunks(options);
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle,
      PartitionChunkListing chunkListing,
      GetMetadataOption... options) {

    IcebergCatalogTableProvider icebergTableProvider =
        datasetHandle.unwrap(IcebergCatalogTableProvider.class);
    return icebergTableProvider.getDatasetMetadata(options);
  }

  @Override
  public boolean containerExists(EntityPath containerPath, GetMetadataOption... options) {
    return getCatalogAccessor().datasetExists(containerPath.getComponents());
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    // TODO: implement RBAC
    return true;
  }

  @Override
  public SourceState getState() {
    if (!isOpen.get()) {
      logger.error("Iceberg Catalog Source {} is either not started or already closed.", getName());
      return SourceState.badState(
          String.format(
              "Could not connect to %s, check your connection information and credentials",
              getName()),
          String.format("Iceberg Catalog Source %s has not been started.", getName()));
    }

    try {
      getCatalogAccessor().checkState();
      return SourceState.GOOD;
    } catch (Exception ex) {
      logger.debug(
          "Caught exception while trying to get status of Iceberg Catalog Source {}, error: ",
          getName(),
          ex);
      return SourceState.badState(
          String.format(
              "Could not connect to %s, check your connection information and credentials",
              getName()),
          String.format("Failure connecting to source: %s", ex.getMessage()));
    }
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return sabotContext
        .getConfig()
        .getClass(
            "dremio.plugins.icebergcatalog.rulesfactory",
            StoragePluginRulesFactory.class,
            IcebergCatalogRulesFactory.class);
  }

  @Override
  public void start() throws IOException {
    config.validateOnStart(sabotContext);

    catalogAccessor = config.createCatalog(fsConf, sabotContext);

    isOpen.set(true);
  }

  @Override
  public void close() throws Exception {
    if (!isOpen.getAndSet(false)) {
      return;
    }

    try {
      catalogAccessor.close();
    } catch (Exception e) {
      logger.warn("Failed to close catalog instance", e);
    }

    try {
      hadoopFs.close();
    } catch (Exception e) {
      logger.warn("Failed to close file system provider", e);
    }
  }

  @Override
  public boolean isMetadataValidityCheckRecentEnough(
      Long lastMetadataValidityCheckTime, Long currentTime, OptionManager optionManager) {
    final long metadataAggressiveExpiryTime =
        optionManager.getOption(PlannerSettings.METADATA_EXPIRY_CHECK_INTERVAL_SECS) * 1000;
    // dataset metadata validity was checked too long ago (or never)
    return lastMetadataValidityCheckTime != null
        && lastMetadataValidityCheckTime + metadataAggressiveExpiryTime >= currentTime;
  }

  @Override
  public FormatPlugin getFormatPlugin(FormatPluginConfig formatConfig) {
    if (formatConfig instanceof IcebergFormatConfig) {
      IcebergFormatPlugin icebergFormatPlugin =
          new IcebergFormatPlugin(
              "iceberg", sabotContext, (IcebergFormatConfig) formatConfig, null);
      icebergFormatPlugin.initialize((IcebergFormatConfig) formatConfig, this);
      return icebergFormatPlugin;
    }
    throw new UnsupportedOperationException(
        "Format plugins for non iceberg use cases are not supported.");
  }

  @Override
  public Configuration getFsConfCopy() {
    return new Configuration(fsConf);
  }

  private FileSystem createFileSystem(
      String filePath, String userName, OperatorContext operatorContext) throws IOException {
    return this.sabotContext
        .getFileSystemWrapper()
        .wrap(
            newFileSystem(filePath, userName, operatorContext),
            getName(),
            config,
            operatorContext,
            config.isAsyncEnabled(),
            false);
  }

  private FileSystem newFileSystem(
      String filePath, String userName, OperatorContext operatorContext) {
    return HadoopFileSystem.get(
        hadoopFs.load(filePath, getFsConfCopy(), userName),
        operatorContext == null ? null : operatorContext.getStats(),
        config.isAsyncEnabled());
  }

  @Override
  public FileSystem createFS(String filePath, String userName, OperatorContext operatorContext)
      throws IOException {
    return createFileSystem(filePath, userName, operatorContext);
  }

  @Override
  public FileSystem createFSWithAsyncOptions(
      String filePath, String userName, OperatorContext operatorContext) throws IOException {
    return createFileSystem(filePath, userName, operatorContext);
  }

  @Override
  public FileSystem createFSWithoutHDFSCache(
      String filePath, String userName, OperatorContext operatorContext) throws IOException {
    return createFileSystem(filePath, userName, operatorContext);
  }

  @Override
  public List<String> resolveTableNameToValidPath(List<String> tableSchemaPath) {
    return tableSchemaPath.stream().skip(1).collect(Collectors.toList());
  }

  @Override
  public BlockBasedSplitGenerator.SplitCreator createSplitCreator(
      OperatorContext context, byte[] extendedBytes, boolean isInternalIcebergTable) {
    return new ParquetSplitCreator(context, false);
  }

  @Override
  public ScanTableFunction createScanTableFunction(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig) {
    return new ParquetScanTableFunction(fec, context, props, functionConfig);
  }

  @Override
  public FooterReadTableFunction getFooterReaderTableFunction(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig) {
    return new FooterReadTableFunction(fec, context, props, functionConfig);
  }

  @Override
  public AbstractRefreshPlanBuilder createRefreshDatasetPlanBuilder(
      SqlHandlerConfig config,
      SqlRefreshDataset sqlRefreshDataset,
      UnlimitedSplitsMetadataProvider metadataProvider,
      boolean isFullRefresh) {
    throw new UnsupportedOperationException(
        "Refresh dataset for tables from iceberg catalog is not supported.");
  }

  @Override
  public DirListingRecordReader createDirListRecordReader(
      OperatorContext context,
      FileSystem fs,
      DirListInputSplitProto.DirListInputSplit dirListInputSplit,
      boolean isRecursive,
      BatchSchema tableSchema,
      List<PartitionProtobuf.PartitionValue> partitionValues) {
    return new DirListingRecordReader(
        context,
        fs,
        dirListInputSplit,
        isRecursive,
        tableSchema,
        partitionValues,
        false,
        false,
        null);
  }

  @Override
  public boolean isIcebergMetadataValid(DatasetConfig config, NamespaceKey tableSchemaPath) {
    if (config.getPhysicalDataset().getIcebergMetadata() == null
        || config.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation() == null
        || config.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation().isEmpty()) {
      return false;
    }

    List<String> dataset = tableSchemaPath.getPathComponents();

    String existingRootPointer =
        config.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation();
    try {
      String latestRootPointer =
          getCatalogAccessor().getTableMetadata(dataset).metadataFileLocation();
      if (!existingRootPointer.equals(latestRootPointer)) {
        logger.debug(
            "Iceberg Dataset {} metadata is not valid. Existing root pointer in catalog: {}. Latest Iceberg table root pointer: {}.",
            tableSchemaPath,
            existingRootPointer,
            latestRootPointer);
        return false;
      }
    } catch (NoSuchTableException e) {
      throw UserException.ioExceptionError(e)
          .message(String.format("Dataset path '%s', table not found.", tableSchemaPath))
          .buildSilently();
    }
    return true;
  }

  @Override
  public TableOperations createIcebergTableOperations(
      FileIO fileIO, String queryUserName, IcebergTableIdentifier tableIdentifier) {
    // TODO: maybe create an IcebergRESTModel for DML/DDL?
    throw new UnsupportedOperationException();
  }

  @Override
  public FileIO createIcebergFileIO(
      FileSystem fs,
      OperatorContext context,
      List<String> dataset,
      String datasourcePluginUID,
      Long fileLength) {
    return new DremioFileIO(
        fs,
        context,
        dataset,
        datasourcePluginUID,
        fileLength,
        new HadoopFileSystemConfigurationAdapter(fsConf));
  }

  @Override
  public TableMetadata loadTableMetadata(
      FileIO io, OperatorContext context, List<String> dataset, String metadataLocation) {
    return Optional.ofNullable(getCatalogAccessor().getTableMetadata(dataset))
        .orElseGet(() -> IcebergUtils.loadTableMetadata(io, context, metadataLocation));
  }
}
