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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.inject.Provider;

import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.glue.catalog.util.AWSGlueConfig;
import com.dremio.common.FSConstants;
import com.dremio.common.exceptions.UserException;
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
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.SupportsPF4JStoragePlugin;
import com.dremio.exec.store.hive.Hive2StoragePluginConfig;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Strings;

/**
 * This plugin is a wrapper over Hive2 Storage plugin
 * During instantiation it creates a hive 2 plugin and delegates all calls to it
 */
public class AWSGlueStoragePlugin implements StoragePlugin, SupportsReadSignature,
  SupportsListingDatasets, SupportsPF4JStoragePlugin {

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

  private StoragePlugin hiveStoragePlugin;
  public AWSGlueStoragePlugin(AWSGluePluginConfig config,
                              SabotContext context,
                              String name, Provider<StoragePluginId> idProvider) {
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

    String mainAWSCredProvider;
    switch (config.credentialType) {
      case ACCESS_KEY:
        if (("".equals(config.accessKey)) || ("".equals(config.accessSecret))) {
          throw UserException.validationError()
            .message("Failure creating AWS Glue connection. You must provide AWS Access Key and AWS Access Secret.")
            .build(logger);
        }
        mainAWSCredProvider = ACCESS_KEY_PROVIDER;
        finalProperties.add(new Property(Constants.ACCESS_KEY, config.accessKey));
        finalProperties.add(new Property(Constants.SECRET_KEY, config.accessSecret));
        break;
      case EC2_METADATA:
        mainAWSCredProvider = EC2_METADATA_PROVIDER;
        break;
      default:
        throw new RuntimeException("Failure creating AWS Glue connection. Invalid credentials type.");
    }

    if (!Strings.isNullOrEmpty(config.assumedRoleARN)) {
      finalProperties.add(new Property(ASSUMED_ROLE_ARN, config.assumedRoleARN));
      finalProperties.add(new Property(ASSUMED_ROLE_CREDENTIALS_PROVIDER, mainAWSCredProvider));
      mainAWSCredProvider = ASSUME_ROLE_PROVIDER;
    }

    finalProperties.add(new Property(Constants.AWS_CREDENTIALS_PROVIDER, mainAWSCredProvider));
    finalProperties.add(new Property("com.dremio.aws_credentials_provider", config.credentialType.toString()));

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

    // set placeholders for hostname and port
    hiveConf.hostname = AWS_GLUE_HIVE_METASTORE_PLACEHOLDER;
    hiveConf.port = 9083;

    // instantiate hive plugin
    hiveStoragePlugin = hiveConf.newPlugin(context, name, idProvider);
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
}
