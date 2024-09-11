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
package com.dremio.plugins.azure;

import static com.dremio.hadoop.security.alias.DremioCredentialProvider.DREMIO_SCHEME_PREFIX;
import static com.dremio.plugins.azure.AzureStorageFileSystem.AZURE_SHAREDKEY_SIGNER_TYPE;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.extensions.ValidateMetadataOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.DirectorySupportLackingFileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.plugins.util.ContainerFileSystem.ContainerFailure;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Storage plugin for Microsoft Azure Storage */
public class AzureStoragePlugin
    extends DirectorySupportLackingFileSystemPlugin<AbstractAzureStorageConf> {

  private static final Logger logger = LoggerFactory.getLogger(AzureStoragePlugin.class);
  private static final String DREMIO_PLUS_AZURE_VAULT_SCHEME_PREFIX =
      DREMIO_SCHEME_PREFIX.concat("azure-key-vault+");

  public AzureStoragePlugin(
      AbstractAzureStorageConf config,
      SabotContext context,
      String name,
      Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
  }

  @Override
  public SourceState getState() {
    String sourceName = getConfig().getPath().toString();
    try {
      AzureStorageFileSystem fs = getSystemUserFS().unwrap(AzureStorageFileSystem.class);
      fs.refreshFileSystems();
      List<ContainerFailure> failures = fs.getSubFailures();
      if (failures.isEmpty()) {
        return SourceState.GOOD;
      }
      StringBuilder sb = new StringBuilder();
      for (ContainerFailure f : failures) {
        sb.append(f.getName());
        sb.append(": ");
        sb.append(f.getException().getMessage());
        sb.append("\n");
      }

      return SourceState.warnState(sb.toString());

    } catch (Exception e) {
      return SourceState.badState(
          String.format("Could not connect to %s. Check your settings and credentials", getName()),
          e);
    }
  }

  // DX-17365: Azure does not return correct "Last Modified" times for folders, therefore new
  //          subfolder discovery is not possible by checking super folder's 'Last Modified" time.
  //          A full refresh is required to guarantee that container content is up-to-date.
  //          Always return metadata invalid in order to trigger a refresh.
  @Override
  public MetadataValidity validateMetadata(
      BytesOutput signature,
      DatasetHandle datasetHandle,
      DatasetMetadata metadata,
      ValidateMetadataOption... options) {
    return MetadataValidity.INVALID;
  }

  private static String addPrefixIfNotExist(String prefix, String input) {
    if (prefix == null) {
      return input;
    }
    if (input == null) {
      return null;
    }
    return input.toLowerCase().startsWith(prefix) ? input : prefix.concat(input);
  }

  @Override
  protected List<Property> getProperties() {
    final AbstractAzureStorageConf config = getConfig();
    final List<Property> properties = new ArrayList<>();

    // configure hadoop fs implementation
    properties.add(
        new Property("fs.dremioAzureStorage.impl", AzureStorageFileSystem.class.getName()));
    properties.add(new Property("fs.dremioAzureStorage.impl.disable.cache", "true"));

    // configure azure properties.
    properties.add(new Property(AzureStorageFileSystem.ACCOUNT, config.accountName));
    properties.add(new Property(AzureStorageFileSystem.SECURE, Boolean.toString(config.enableSSL)));
    properties.add(new Property(AzureStorageFileSystem.MODE, config.accountKind.name()));
    properties.add(
        new Property(
            AzureStorageOptions.ENABLE_CHECKSUM.getOptionName(),
            Boolean.toString(
                getContext().getOptionManager().getOption(AzureStorageOptions.ENABLE_CHECKSUM))));

    AzureAuthenticationType credentialsType = config.credentialsType;
    switch (credentialsType) {
      case AZURE_ACTIVE_DIRECTORY:
        properties.add(
            new Property(
                AzureStorageFileSystem.CREDENTIALS_TYPE,
                AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY.name()));
        properties.add(new Property(AzureStorageFileSystem.CLIENT_ID, config.clientId));

        if (!SecretRef.isNullOrEmpty(config.clientSecret)) {
          properties.add(
              new Property(
                  AzureStorageFileSystem.CLIENT_SECRET,
                  SecretRef.toConfiguration(config.clientSecret, DREMIO_SCHEME_PREFIX)));
        } else if (StringUtils.isNotBlank(config.getClientSecretUri())) {
          // This path should never occur unless there are errors during Catalog start.
          properties.add(
              new Property(
                  AzureStorageFileSystem.CLIENT_SECRET,
                  addPrefixIfNotExist(
                      DREMIO_PLUS_AZURE_VAULT_SCHEME_PREFIX, config.getClientSecretUri())));
          logger.warn(
              "clientSecretUri still in use, old AzureStorageConf format was not updated properly.");
        } else {
          throw new IllegalStateException("Missing clientSecret value");
        }

        properties.add(new Property(AzureStorageFileSystem.TOKEN_ENDPOINT, config.tokenEndpoint));
        break;

      case ACCESS_KEY:
        properties.add(
            new Property(
                AzureStorageFileSystem.CREDENTIALS_TYPE,
                AzureAuthenticationType.ACCESS_KEY.name()));

        if (!SecretRef.isNullOrEmpty(config.accessKey)) {
          properties.add(
              new Property(
                  AzureStorageFileSystem.KEY,
                  SecretRef.toConfiguration(config.accessKey, DREMIO_SCHEME_PREFIX)));
          // static credentials (this should be the default used by azure)
          properties.add(
              new Property(AZURE_SHAREDKEY_SIGNER_TYPE, SharedKeyCredentials.class.getName()));
        } else if (StringUtils.isNotBlank(config.getAccessKeyUri())) {
          // This path should never occur unless there are errors during Catalog start.
          properties.add(
              new Property(
                  AzureStorageFileSystem.KEY,
                  addPrefixIfNotExist(
                      DREMIO_PLUS_AZURE_VAULT_SCHEME_PREFIX, config.getAccessKeyUri())));
          properties.add(
              new Property(AZURE_SHAREDKEY_SIGNER_TYPE, AzureSharedKeyCredentials.class.getName()));
          logger.warn(
              "accessKeyUri still in use, old AzureStorageConf format was not updated properly.");
        } else {
          throw new IllegalStateException("Missing accessKey value");
        }
        break;

      default:
        throw new IllegalStateException("Unrecognized credential type: " + credentialsType);
    }

    if (config.containers != null && config.containers.size() > 0) {
      String containers =
          config.containers.stream()
              .filter(c -> c != null && c.length() > 0)
              .map(
                  c -> {
                    Preconditions.checkArgument(
                        !c.contains(","), "Container Names cannot contain commas.");
                    return c;
                  })
              .collect(Collectors.joining(","));
      properties.add(new Property(AzureStorageFileSystem.CONTAINER_LIST, containers));
    }

    if (config.rootPath.length() > 1) {
      String path = config.rootPath;
      properties.add(new Property(AzureStorageFileSystem.ROOT_PATH, path));
    }

    // Properties are added in order so make sure that any hand provided properties override
    // settings done via specific config
    List<Property> parentProperties = super.getProperties();
    if (parentProperties != null) {
      properties.addAll(parentProperties);
    }

    return properties;
  }

  @Override
  public CreateTableEntry createNewTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig config,
      IcebergTableProps icebergTableProps,
      WriterOptions writerOptions,
      Map<String, Object> storageOptions,
      boolean isResultsTable) {
    final String containerName = getAndCheckContainerName(tableSchemaPath);
    final CreateTableEntry entry =
        super.createNewTable(
            tableSchemaPath,
            config,
            icebergTableProps,
            writerOptions,
            storageOptions,
            isResultsTable);

    final AzureStorageFileSystem fs = getSystemUserFS().unwrap(AzureStorageFileSystem.class);

    if (!fs.containerExists(containerName)) {
      throw UserException.validationError()
          .message("Cannot create the table because '%s' container does not exist.", containerName)
          .build(logger);
    }

    // TODO (DX-15571): Add a check for if the current user can possibly have write permissions
    // on the container. This is only valid when using Azure Active Directory (OAuth) authentication
    // as SharedKeys do not have an identity.

    //    if (!fs.mayHaveWritePermission(containerName)) {
    //      throw UserException.validationError()
    //          .message("No write permission to '%s' container.", containerName)
    //          .build(logger);
    //    }

    return entry;
  }

  @VisibleForTesting
  String getAndCheckContainerName(NamespaceKey key) {
    Preconditions.checkArgument(key.size() >= 2, "key must be at least two parts");
    final List<String> resolvedPath =
        resolveTableNameToValidPath(key.getPathComponents()); // strips source name
    final String containerName = resolvedPath.get(0);
    if (resolvedPath.size() == 1) {
      throw UserException.validationError()
          .message("Creating containers is not supported (name: %s).", containerName)
          .build(logger);
    }
    return containerName;
  }

  @Override
  protected boolean isAsyncEnabledForQuery(OperatorContext context) {
    return context != null && context.getOptions().getOption(AzureStorageOptions.ASYNC_READS);
  }

  @Override
  public boolean supportReadSignature(DatasetMetadata metadata, boolean isFileDataset) {
    return false;
  }
}
