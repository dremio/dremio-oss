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
package com.dremio.plugins.awsglue.store;


import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import javax.inject.Provider;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.glue.DremioGlueTableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LockManagers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.glue.catalog.util.AWSGlueConfig;
import com.dremio.common.FSConstants;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.connector.metadata.extensions.ValidateMetadataOption;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.AlterTableOption;
import com.dremio.exec.catalog.DatasetSplitsPointer;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.RollbackOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.planner.sql.handlers.refresh.UnlimitedSplitsMetadataProvider;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.SupportsPF4JStoragePlugin;
import com.dremio.exec.store.dfs.AddColumn;
import com.dremio.exec.store.dfs.AddPrimaryKey;
import com.dremio.exec.store.dfs.ChangeColumn;
import com.dremio.exec.store.dfs.DropColumn;
import com.dremio.exec.store.dfs.DropPrimaryKey;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.hive.Hive2StoragePluginConfig;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.iceberg.glue.IcebergGlueModel;
import com.dremio.exec.store.iceberg.glue.IcebergGlueTableIdentifier;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.committer.ReadSignatureProvider;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingRecordReader;
import com.dremio.exec.store.metadatarefresh.footerread.FooterReadTableFunction;
import com.dremio.exec.store.parquet.ScanTableFunction;
import com.dremio.io.file.FileSystem;
import com.dremio.plugins.util.awsauth.DremioAWSCredentialsProviderFactoryV2;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GlueException;

/**
 * This plugin is a wrapper over Hive2 Storage plugin
 * During instantiation it creates a hive 2 plugin and delegates all calls to it
 */
public class AWSGlueStoragePlugin implements StoragePlugin, MutablePlugin, SupportsReadSignature,
  SupportsListingDatasets, SupportsPF4JStoragePlugin, SupportsInternalIcebergTable, SupportsIcebergRootPointer, SupportsIcebergMutablePlugin {

  private static final Logger logger = LoggerFactory.getLogger(AWSGlueStoragePlugin.class);
  private static final String AWS_GLUE_HIVE_METASTORE_PLACEHOLDER = "DremioGlueHive";
  private static final String AWS_GLUE_HIVE_CLIENT_FACTORY =
    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory";
  private static final String IMETASTORE_CLIENT_FACTORY_CLASS =
    "hive.imetastoreclient.factory.class";
  private static final String GLUE_AWS_CREDENTIALS_FACTORY = "com.dremio.exec.store.hive.GlueAWSCredentialsFactory";
  public static final String ASSUMED_ROLE_ARN = "fs.s3a.assumed.role.arn";
  public static final String ASSUMED_ROLE_CREDENTIALS_PROVIDER = "fs.s3a.assumed.role.credentials.provider";
  public static final String ASSUME_ROLE_PROVIDER = "com.dremio.plugins.s3.store.STSCredentialProviderV1";

  // AWS Credential providers
  public static final String ACCESS_KEY_PROVIDER = SimpleAWSCredentialsProvider.NAME;
  public static final String EC2_METADATA_PROVIDER = "com.amazonaws.auth.InstanceProfileCredentialsProvider";
  public static final String AWS_PROFILE_PROVIDER = "com.dremio.plugins.s3.store.AWSProfileCredentialsProviderV1";


  private StoragePlugin hiveStoragePlugin;

  private AWSGluePluginConfig config;
  private SabotContext context;
  private String name;
  private Provider<StoragePluginId> idProvider;

  private GlueClient glueClient;
  private Configuration glueConfTableOperations;

  public AWSGlueStoragePlugin(AWSGluePluginConfig config,
                              SabotContext context,
                              String name, Provider<StoragePluginId> idProvider) {
    this.config = config;
    this.context = context;
    this.name = name;
    this.idProvider = idProvider;
  }

  protected void setup() {
    Hive2StoragePluginConfig hiveConf = new Hive2StoragePluginConfig();

    // set hive configuration properties
    final List<Property> finalProperties = new ArrayList<>();
    final List<Property> propertyList = config.propertyList;

    // Unit tests pass a mock factory to use.
    boolean imetastoreClientDefined = propertyList != null && config.propertyList.stream().
      anyMatch(property -> property.name.equalsIgnoreCase(IMETASTORE_CLIENT_FACTORY_CLASS));

    // If client factory is not overridden then use aws-glue-data-catalog-client-for-apache-hive-metastore
    if (!imetastoreClientDefined) {
      finalProperties.add(new Property(IMETASTORE_CLIENT_FACTORY_CLASS,
        AWS_GLUE_HIVE_CLIENT_FACTORY));
    }

    finalProperties.add(new Property(AWSGlueConfig.AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS,
      GLUE_AWS_CREDENTIALS_FACTORY));

    finalProperties.add(new Property(FSConstants.FS_S3A_REGION, config.regionNameSelection.getRegionName()));

    String mainAWSCredProvider = getMainCredentialsProvider(config, finalProperties);

    if (!Strings.isNullOrEmpty(config.assumedRoleARN)) {
      finalProperties.add(new Property(ASSUMED_ROLE_ARN, config.assumedRoleARN));
      finalProperties.add(new Property(ASSUMED_ROLE_CREDENTIALS_PROVIDER, mainAWSCredProvider));
      mainAWSCredProvider = ASSUME_ROLE_PROVIDER;
    }

    finalProperties.add(new Property(Constants.AWS_CREDENTIALS_PROVIDER, mainAWSCredProvider));
    finalProperties.add(new Property("com.dremio.aws_credentials_provider", config.credentialType.toString()));
    finalProperties.add(new Property("hive.exec.orc.zerocopy", "false"));

    if (propertyList != null && !propertyList.isEmpty()) {
      finalProperties.addAll(propertyList);
    }

    finalProperties.add(new Property(Constants.SECURE_CONNECTIONS, String.valueOf(config.secure)));

    // copy config options from glue config to hive config
    hiveConf.propertyList = finalProperties;
    hiveConf.enableAsync = config.enableAsync;
    hiveConf.isCachingEnabledForHDFS = config.isCachingEnabled;
    hiveConf.isCachingEnabledForS3AndAzureStorage = config.isCachingEnabled;
    hiveConf.maxCacheSpacePct = config.maxCacheSpacePct;
    hiveConf.defaultCtasFormat = config.defaultCtasFormat;


    // set placeholders for hostname and port
    hiveConf.hostname = AWS_GLUE_HIVE_METASTORE_PLACEHOLDER;
    hiveConf.port = 9083;

    // instantiate hive plugin
    hiveStoragePlugin = hiveConf.newPlugin(context, name, idProvider);

    //glueconf for iceberg table operations
    this.glueConfTableOperations = getConfForGlue();
  }

  /**
   * Populate plugin properties into finalProperties parameter and return the credentials provider.
   * @param config
   * @param finalProperties
   * @return
   */
  protected String getMainCredentialsProvider(AWSGluePluginConfig config, List<Property> finalProperties) {
    switch (config.credentialType) {
      case ACCESS_KEY:
        if (("".equals(config.accessKey)) || ("".equals(config.accessSecret))) {
          throw UserException.validationError()
            .message("Failure creating AWS Glue connection. You must provide AWS Access Key and AWS Access Secret.")
            .build(logger);
        }
        finalProperties.add(new Property(Constants.ACCESS_KEY, config.accessKey));
        finalProperties.add(new Property(Constants.SECRET_KEY, config.accessSecret));
        return ACCESS_KEY_PROVIDER;
      case AWS_PROFILE:
        if (config.awsProfile != null) {
          finalProperties.add(new Property("com.dremio.awsProfile", config.awsProfile));
        }
        return AWS_PROFILE_PROVIDER;
      case EC2_METADATA:
        return EC2_METADATA_PROVIDER;
      default:
        throw new RuntimeException("Failure creating AWS Glue connection. Invalid credentials type.");
    }
  }

  private AwsCredentialsProvider getAwsCredentialsProvider() {

    return DremioAWSCredentialsProviderFactoryV2.getAWSCredentialsProvider(glueConfTableOperations);

  }

  private GlueClient getGlueClient() {

    if (glueClient == null) {
      try {
        this.glueClient = GlueClient.builder().region(Region.of(config.regionNameSelection.getRegionName())).credentialsProvider(getAwsCredentialsProvider()).build();
      } catch (IllegalStateException e) {
        logger.error("Unable to setup glue client - " + e.getMessage());
        throw UserException.unsupportedError(e).message("Unable to instantiate the glueClient object to talk to Glue. Please check your credentials and region in the configuration").buildSilently();
      }
    }
    return glueClient;
  }


  private Configuration getConfForGlue() {

    Configuration config = getFsConfCopy();
    config.set(CatalogProperties.WAREHOUSE_LOCATION, config.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, ""));
    config.set(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
    return config;
  }

  @Override
  public Configuration getFsConfCopy() {
    return ((SupportsIcebergRootPointer) hiveStoragePlugin).getFsConfCopy();
  }

  @Override
  public FormatPlugin getFormatPlugin(FormatPluginConfig formatConfig) {
    return ((SupportsIcebergRootPointer) hiveStoragePlugin).getFormatPlugin(formatConfig);
  }

  @Override
  public FileSystem createFS(String filePath, String userName, OperatorContext operatorContext) throws IOException {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).createFS(filePath, userName, operatorContext);
  }

  @Override
  public FileSystem createFSWithAsyncOptions(String filePath, String userName, OperatorContext operatorContext) throws IOException {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).createFSWithAsyncOptions(filePath, userName, operatorContext);
  }

  @Override
  public FileSystem createFSWithoutHDFSCache(String filePath, String userName, OperatorContext operatorContext) throws IOException {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).createFSWithoutHDFSCache(filePath, userName, operatorContext);
  }

  @Override
  public boolean isIcebergMetadataValid(DatasetConfig config, NamespaceKey key, NamespaceService userNamespaceService) {
    return ((SupportsIcebergRootPointer) hiveStoragePlugin).isIcebergMetadataValid(config, key, userNamespaceService);
  }

  @Override
  public TableOperations createIcebergTableOperations(FileSystem fs, String queryUserName, IcebergTableIdentifier tableIdentifier) {
    Map<String, String> properties = new HashMap<>();
    for (Map.Entry<String, String> property : glueConfTableOperations) {
      properties.put(property.getKey(), property.getValue());
    }

    IcebergGlueTableIdentifier glueTableIdentifier = (IcebergGlueTableIdentifier) tableIdentifier;
    FileIO fileIO = createIcebergFileIO(fs, null, null, null, null);

    return new DremioGlueTableOperations(getGlueClient(), LockManagers.from(properties),
      IcebergGlueModel.GLUE, properties, fileIO,
      TableIdentifier.of(glueTableIdentifier.getNamespace(), glueTableIdentifier.getTableName()));
  }

  @Override
  public FileIO createIcebergFileIO(FileSystem fs, OperatorContext context, List<String> dataset,
      String datasourcePluginUID, Long fileLength) {
    return ((SupportsIcebergRootPointer) hiveStoragePlugin).createIcebergFileIO(fs, context, dataset,
        datasourcePluginUID, fileLength);
  }

  @Override
  public boolean canGetDatasetMetadataInCoordinator() {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).canGetDatasetMetadataInCoordinator();
  }

  @Override
  public List<String> resolveTableNameToValidPath(List<String> tableSchemaPath) {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).resolveTableNameToValidPath(tableSchemaPath);
  }

  @Override
  public BlockBasedSplitGenerator.SplitCreator createSplitCreator(OperatorContext context, byte[] extendedBytes, boolean isInternalIcebergTable) {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).createSplitCreator(context, extendedBytes, isInternalIcebergTable);
  }

  @Override
  public ScanTableFunction createScanTableFunction(FragmentExecutionContext fec, OperatorContext context, OpProps props, TableFunctionConfig functionConfig) {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).createScanTableFunction(fec, context, props, functionConfig);
  }

  @Override
  public DirListingRecordReader createDirListRecordReader(OperatorContext context,
                                                          FileSystem fs,
                                                          DirListInputSplitProto.DirListInputSplit dirListInputSplit,
                                                          boolean isRecursive,
                                                          BatchSchema tableSchema,
                                                          List<PartitionProtobuf.PartitionValue> partitionValues) {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).createDirListRecordReader(context, fs, dirListInputSplit, isRecursive, tableSchema, partitionValues);
  }

  @Override
  public boolean allowUnlimitedSplits(DatasetHandle handle, DatasetConfig datasetConfig, String user) {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).allowUnlimitedSplits(handle, datasetConfig, user);
  }

  @Override
  public void runRefreshQuery(String refreshQuery, String user) throws Exception {
    ((SupportsInternalIcebergTable) hiveStoragePlugin).runRefreshQuery(refreshQuery, user);
  }

  @Override
  public FooterReadTableFunction getFooterReaderTableFunction(FragmentExecutionContext fec, OperatorContext context, OpProps props, TableFunctionConfig functionConfig) {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).getFooterReaderTableFunction(fec, context, props, functionConfig);
  }

  @Override
  public AbstractRefreshPlanBuilder createRefreshDatasetPlanBuilder(SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset, UnlimitedSplitsMetadataProvider metadataProvider, boolean isFullRefresh) {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).createRefreshDatasetPlanBuilder(config, sqlRefreshDataset, metadataProvider, isFullRefresh);
  }

  @Override
  public ReadSignatureProvider createReadSignatureProvider(ByteString existingReadSignature,
                                                            final String dataTableRoot,
                                                            final long queryStartTime,
                                                            List<String> partitionPaths,
                                                            Predicate<String> partitionExists,
                                                            boolean isFullRefresh, boolean isPartialRefresh) {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).createReadSignatureProvider(existingReadSignature, dataTableRoot, queryStartTime, partitionPaths, partitionExists, isFullRefresh, isPartialRefresh);
  }

  @Override
  public boolean supportReadSignature(DatasetMetadata metadata, boolean isFileDataset) {
    return ((SupportsInternalIcebergTable) hiveStoragePlugin).supportReadSignature(metadata, isFileDataset);
  }

  @Override
  public CreateTableEntry createNewTable(NamespaceKey tableSchemaPath, SchemaConfig schemaConfig,
                                         IcebergTableProps icebergTableProps, WriterOptions writerOptions,
                                         Map<String, Object> storageOptions, boolean isResultsTable) {
    Preconditions.checkState(!isResultsTable, "job results cannot be stored in the GlueStoragePlugin");
    return ((MutablePlugin) hiveStoragePlugin).createNewTable(tableSchemaPath, schemaConfig, icebergTableProps, writerOptions, storageOptions, isResultsTable);
  }

  public IcebergModel getIcebergModel(String location, NamespaceKey key, String userName) {
    FileSystem fs = null;
    try {
      fs = createFS(location, SystemUser.SYSTEM_USERNAME, null);
    } catch (IOException e) {
      throw UserException.validationError(e).message("Failure creating File System instance for path %s", location).buildSilently();
    }
    List<String> dbAndTableName = resolveTableNameToValidPath(key.getPathComponents());
    String dbName = dbAndTableName.get(0);
    String tableName = dbAndTableName.get(1);
    return new IcebergGlueModel(dbName, tableName, fs, userName, null, this);
  }

  @Override
  public IcebergModel getIcebergModel(IcebergTableProps tableProps, String userName, OperatorContext context, FileSystem fs) {
    if (fs == null) {
      try {
        fs = createFS(tableProps.getTableLocation(), SystemUser.SYSTEM_USERNAME, null);
      } catch (IOException e) {
        throw UserException.validationError(e).message("Failure creating File System instance for path %s", tableProps.getTableLocation()).buildSilently();
      }
    }
    return new IcebergGlueModel(tableProps.getDatabaseName(), tableProps.getTableName(), fs, userName, null, this);
  }

  private String resolveTableLocation(String dbName, String tableName, WriterOptions writerOptions) {
    String queryLocation = writerOptions.getTableLocation();
    if (StringUtils.isNotEmpty(queryLocation)) {
      return PathUtils.removeTrailingSlash(queryLocation);
    }

    if (!context.getOptionManager().getOption(ExecConstants.ENABLE_HIVE_DATABASE_LOCATION)) {
      return null;
    }

    try {
      GetDatabaseResponse response = getGlueClient().getDatabase(GetDatabaseRequest.builder().name(dbName).build());
      if (response == null || response.database() == null || StringUtils.isEmpty(response.database().locationUri())) {
        return null;
      }

      // use database location as parent 'folder' for a new table
      String dbLocationUri = PathUtils.removeTrailingSlash(response.database().locationUri());
      return dbLocationUri + '/' + tableName;
    } catch (GlueException e) {
      logger.warn("Unable to retrieve glue database [" + dbName +"].", e);
      return null;
    } catch (Throwable e) {
      logger.error("Unable to resolve location for glue database [" + dbName +"].", e);
      return null;
    }
  }

  @Override
  public void createEmptyTable(NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, BatchSchema batchSchema, WriterOptions writerOptions) {

    List<String> dbAndTableName = resolveTableNameToValidPath(tableSchemaPath.getPathComponents());
    String dbName = dbAndTableName.get(0);
    String tableName = dbAndTableName.get(1);

    String tableLocation = resolveTableLocation(dbName, tableName, writerOptions);
    if (StringUtils.isEmpty(tableLocation)) {
      String warehouseLocation = PathUtils.removeTrailingSlash(glueConfTableOperations.get(CatalogProperties.WAREHOUSE_LOCATION));
      if (StringUtils.isEmpty(warehouseLocation) || HiveConf.ConfVars.METASTOREWAREHOUSE.getDefaultValue().equals(warehouseLocation)) {
        logger.error("Advanced Property {} not set. Please set it to have a valid location to create table.", HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
        throw UserException.unsupportedError().message("Unable to create table. Please set the default warehouse location").buildSilently();
      }
      tableLocation = warehouseLocation
        + tableSchemaPath.getPathComponents().stream().reduce("", (a, b) -> a + "/" + b);
    }

    IcebergModel icebergModel = getIcebergModel(tableLocation, tableSchemaPath, schemaConfig.getUserName());
    PartitionSpec partitionSpec = Optional.ofNullable(writerOptions.getTableFormatOptions().getIcebergSpecificOptions()
      .getIcebergTableProps()).map(props -> props.getDeserializedPartitionSpec()).orElse(null);
    IcebergOpCommitter icebergOpCommitter = icebergModel.getCreateTableCommitter(tableName, icebergModel.getTableIdentifier(tableLocation), batchSchema,
      writerOptions.getPartitionColumns(), null, partitionSpec);
    icebergOpCommitter.commit();
  }

  @Override
  public StoragePluginId getId() {
    return idProvider.get();
  }

  @Override
  public void dropTable(NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions) {
    ((MutablePlugin) hiveStoragePlugin).dropTable(tableSchemaPath, schemaConfig, tableMutationOptions);
  }

  @Override
  public void alterTable(NamespaceKey tableSchemaPath, DatasetConfig datasetConfig, AlterTableOption alterTableOption,
                          SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions) {
    SplitsPointer splits = DatasetSplitsPointer.of(context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation = IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    IcebergModel icebergModel = getIcebergModel(metadataLocation, tableSchemaPath, schemaConfig.getUserName());
    icebergModel.alterTable(icebergModel.getTableIdentifier(metadataLocation), alterTableOption);
  }

  @Override
  public void truncateTable(NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions) {

    DatasetConfig datasetConfig = null;
    try {
      datasetConfig = context.getNamespaceService(schemaConfig.getUserName()).getDataset(tableSchemaPath);
    } catch (NamespaceException e) {
      logger.error("Unable to get datasetConfig for the table to truncate");
      throw UserException.unsupportedError(e).message("Unable to get the table info from Dremio.Failed to truncate the table.Please check the logs").buildSilently();
    }
    SplitsPointer splits = DatasetSplitsPointer.of(context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation = IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    IcebergModel icebergModel = getIcebergModel(metadataLocation, tableSchemaPath, schemaConfig.getUserName());
    icebergModel.truncateTable(icebergModel.getTableIdentifier(metadataLocation));
  }

  @Override
  public void rollbackTable(NamespaceKey tableSchemaPath,
                            DatasetConfig datasetConfig,
                            SchemaConfig schemaConfig,
                            RollbackOption rollbackOption,
                            TableMutationOptions tableMutationOptions) {
    SplitsPointer splits = DatasetSplitsPointer.of(context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation = IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    IcebergModel icebergModel = getIcebergModel(metadataLocation, tableSchemaPath, schemaConfig.getUserName());
    icebergModel.rollbackTable(icebergModel.getTableIdentifier(metadataLocation), rollbackOption);
  }

  @Override
  public void addColumns(NamespaceKey key,
                         DatasetConfig datasetConfig,
                         SchemaConfig schemaConfig,
                         List<Field> columnsToAdd,
                         TableMutationOptions tableMutationOptions) {
    SplitsPointer splits = DatasetSplitsPointer.of(context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation = IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    AddColumn columnOperations = new AddColumn(key, context, datasetConfig, schemaConfig,
      getIcebergModel(metadataLocation, key, schemaConfig.getUserName()), com.dremio.io.file.Path.of(metadataLocation), this);
    columnOperations.performOperation(columnsToAdd);
  }

  @Override
  public void dropColumn(NamespaceKey key,
                         DatasetConfig datasetConfig,
                         SchemaConfig schemaConfig,
                         String columnToDrop,
                         TableMutationOptions tableMutationOptions) {
    SplitsPointer splits = DatasetSplitsPointer.of(context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation = IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    DropColumn columnOperations = new DropColumn(key, context, datasetConfig, schemaConfig,
      getIcebergModel(metadataLocation, key, schemaConfig.getUserName()), com.dremio.io.file.Path.of(metadataLocation), this);
    columnOperations.performOperation(columnToDrop);
  }

  @Override
  public void changeColumn(NamespaceKey key,
                           DatasetConfig datasetConfig,
                           SchemaConfig schemaConfig,
                           String columnToChange,
                           Field fieldFromSql,
                           TableMutationOptions tableMutationOptions) {
    SplitsPointer splits = DatasetSplitsPointer.of(context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation = IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    ChangeColumn columnOperations = new ChangeColumn(key, context, datasetConfig, schemaConfig,
      getIcebergModel(metadataLocation, key, schemaConfig.getUserName()), com.dremio.io.file.Path.of(metadataLocation), this);
    columnOperations.performOperation(columnToChange, fieldFromSql);
  }

  @Override
  public void addPrimaryKey(NamespaceKey table,
                            DatasetConfig datasetConfig,
                            SchemaConfig schemaConfig,
                            List<Field> columns,
                            ResolvedVersionContext versionContext) {
    SplitsPointer splits = DatasetSplitsPointer.of(context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation = IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    AddPrimaryKey op = new AddPrimaryKey(table, context, datasetConfig, schemaConfig,
      getIcebergModel(metadataLocation, table, schemaConfig.getUserName()), com.dremio.io.file.Path.of(metadataLocation), this);
    op.performOperation(columns);
  }

  @Override
  public void dropPrimaryKey(NamespaceKey table,
                             DatasetConfig datasetConfig,
                             SchemaConfig schemaConfig,
                             ResolvedVersionContext versionContext) {
    SplitsPointer splits = DatasetSplitsPointer.of(context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation = IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    DropPrimaryKey op = new DropPrimaryKey(table, context, datasetConfig, schemaConfig,
      getIcebergModel(metadataLocation, table, schemaConfig.getUserName()), com.dremio.io.file.Path.of(metadataLocation), this);
    op.performOperation();
  }

  @Override
  public List<String> getPrimaryKey(NamespaceKey table,
                                    DatasetConfig datasetConfig,
                                    SchemaConfig schemaConfig,
                                    ResolvedVersionContext versionContext,
                                    boolean saveInKvStore) {
    if (datasetConfig.getPhysicalDataset() == null || // PK only supported for physical datasets
      datasetConfig.getPhysicalDataset().getIcebergMetadata() == null) { // Physical dataset not Iceberg format
      return null;
    }

    return IcebergUtils.validateAndGeneratePrimaryKey(this, context, table, datasetConfig, schemaConfig, versionContext, saveInKvStore);
  }

  @Override
  public List<String> getPrimaryKeyFromMetadata(NamespaceKey table,
                                                DatasetConfig datasetConfig,
                                                SchemaConfig schemaConfig,
                                                ResolvedVersionContext versionContext,
                                                boolean saveInKvStore) {
    final String userName = schemaConfig.getUserName();
    final IcebergModel icebergModel;
    final String path;
    if (DatasetHelper.isInternalIcebergTable(datasetConfig)) {
      final FileSystemPlugin<?> metaStoragePlugin = context.getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);
      icebergModel = metaStoragePlugin.getIcebergModel();
      String metadataTableName = datasetConfig.getPhysicalDataset().getIcebergMetadata().getTableUuid();
      path = metaStoragePlugin.resolveTablePathToValidPath(metadataTableName).toString();
    } else if (DatasetHelper.isIcebergDataset(datasetConfig)) {
      SplitsPointer splits = DatasetSplitsPointer.of(context.getNamespaceService(userName), datasetConfig);
      path = IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
      icebergModel = getIcebergModel(path, table, userName);
    } else {
      return null;
    }

    return IcebergUtils.getPrimaryKey(icebergModel, path, table, datasetConfig, userName, this, context, saveInKvStore);
  }

  @Override
  public Writer getWriter(PhysicalOperator child, String location, WriterOptions options, OpProps props)
    throws IOException {
    throw new UnsupportedOperationException("AWS Glue plugin doesn't support table creation via CTAS.");
  }

  @Override
  public boolean toggleSchemaLearning(NamespaceKey table, SchemaConfig schemaConfig, boolean enableSchemaLearning) {
    throw new UnsupportedOperationException("AWS Glue plugin doesn't support schema update.");
  }

  @Override
  public boolean createOrUpdateView(NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, View view, ViewOptions viewOptions) throws IOException {
    throw new UnsupportedOperationException("AWS Glue plugin doesn't support view creation via CREATE VIEW.");
  }

  @Override
  public void dropView(NamespaceKey tableSchemaPath, ViewOptions viewOptions, SchemaConfig schemaConfig) throws IOException {
    throw new UnsupportedOperationException("AWS Glue plugin doesn't support view drop via DROP VIEW.");
  }

  @Override
  public boolean hasAccessPermission(String user,
                                     NamespaceKey key,
                                     DatasetConfig datasetConfig) {
    return hiveStoragePlugin.hasAccessPermission(user,
      key, datasetConfig);
  }

  @Override
  public SourceState getState() {
    return hiveStoragePlugin.getState();
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return hiveStoragePlugin.getSourceCapabilities();
  }

  @Override
  public Class<? extends com.dremio.exec.store.StoragePluginRulesFactory> getRulesFactoryClass() {
    return hiveStoragePlugin.getRulesFactoryClass();
  }

  @Override
  public void start() throws IOException {
    setup();
    hiveStoragePlugin.start();
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath,
                           com.dremio.exec.store.SchemaConfig schemaConfig) {
    return hiveStoragePlugin.getView(tableSchemaPath, schemaConfig);
  }

  @Override
  public BytesOutput provideSignature(DatasetHandle datasetHandle,
                                      DatasetMetadata metadata) throws ConnectorException {
    return ((SupportsReadSignature)hiveStoragePlugin).provideSignature(
      datasetHandle, metadata);
  }

  @Override
  public MetadataValidity validateMetadata(
    BytesOutput signature,
    DatasetHandle datasetHandle,
    DatasetMetadata metadata,
    ValidateMetadataOption... options
  ) throws ConnectorException {
    return ((SupportsReadSignature)hiveStoragePlugin).validateMetadata(signature,
      datasetHandle, metadata, options);
  }

  @Override
  public void close() throws Exception {
    hiveStoragePlugin.close();
  }

  @Override
  public boolean containerExists(EntityPath containerPath) {
    return hiveStoragePlugin.containerExists(containerPath);
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
    DatasetHandle datasetHandle,
    PartitionChunkListing chunkListing,
    GetMetadataOption... options
  ) throws ConnectorException {
    return hiveStoragePlugin.getDatasetMetadata(datasetHandle,
      chunkListing, options);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle,
                                                   ListPartitionChunkOption... options)
    throws ConnectorException {
    return hiveStoragePlugin.listPartitionChunks(datasetHandle,
      options);
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath,
                                                  GetDatasetOption... options)
    throws ConnectorException {
    return hiveStoragePlugin.getDatasetHandle(datasetPath,
      options);
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
    throws ConnectorException {
    return ((SupportsListingDatasets)hiveStoragePlugin).listDatasetHandles(options);
  }

  @Override
  public <T> T getPF4JStoragePlugin() {
    return ((SupportsPF4JStoragePlugin)hiveStoragePlugin).getPF4JStoragePlugin();
  }

  @Override
  public boolean isAWSGlue() {
    return true;
  }

  @Override
  public String getDefaultCtasFormat() {
    return ((MutablePlugin) hiveStoragePlugin).getDefaultCtasFormat();
  }
}
