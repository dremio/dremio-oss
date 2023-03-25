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
package com.dremio.plugins.gcs;

import static com.dremio.io.file.UriSchemes.DREMIO_GCS_SCHEME;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.DirectorySupportLackingFileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.plugins.util.ContainerFileSystem.ContainerFailure;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration;
import com.google.common.base.Preconditions;

/**
 * Plugin for Google Cloud Storage.
 */
@Options
public class GoogleStoragePlugin extends DirectorySupportLackingFileSystemPlugin<GCSConf> {
  public static final BooleanValidator ASYNC_READS = new BooleanValidator("store.gcs.async", true);
  public static final String GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE_DEFAULT = "8388608";
  private static final Logger logger = LoggerFactory.getLogger(GoogleStoragePlugin.class);

  public GoogleStoragePlugin(
    GCSConf config,
    SabotContext context,
    String name,
    Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
  }

  @Override
  protected List<Property> getProperties() {
    List<Property> properties = new ArrayList<>();
    properties.add(new Property(String.format("fs.%s.impl", DREMIO_GCS_SCHEME), GoogleBucketFileSystem.class.getName()));
    properties.add(new Property(String.format("fs.%s.impl.disable.cache", DREMIO_GCS_SCHEME), "true"));
    properties.add(new Property(GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getKey(),
      GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE_DEFAULT));

    GCSConf conf = getConfig();

    if ("".equals(conf.projectId)) {
      throw UserException.validationError()
        .message("Failure creating GCS connection. You must provide Project ID")
        .build(logger);
    }
    switch (conf.authMode) {
      case SERVICE_ACCOUNT_KEYS:
        if ("".equals(conf.clientEmail) ||
          "".equals(conf.clientId) ||
          "".equals(conf.privateKey) ||
          "".equals(conf.privateKeyId)) {
          throw UserException.validationError()
            .message("Failure creating GCS connection. You must provide Private Key ID, Private Key, Client E-mail and Client ID.")
            .build(logger);
        }
        break;
      case AUTO:
      default:
        break;
    }

    switch (conf.authMode) {
      case SERVICE_ACCOUNT_KEYS:
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_KEY_FILE, "true"));
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_CLIENT_ID, conf.clientId));
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_CLIENT_EMAIL, conf.clientEmail));
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_PRIVATE_KEY_ID, conf.privateKeyId));
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_PRIVATE_KEY, conf.privateKey));
        break;
      case AUTO:
      default:
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_KEY_FILE, "false"));
        break;
    }

    properties.add(new Property(GoogleBucketFileSystem.DREMIO_PROJECT_ID, conf.projectId));

    switch (conf.allowlistedBucketsMode) {
      case LIST:
      default:
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_WHITELIST_MODE, "true"));
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_WHITELIST_BUCKETS,
          (conf.bucketWhitelist != null && !conf.bucketWhitelist.isEmpty()) ? String.join(",", conf.bucketWhitelist) : ""));
        break;
      case REGEX:
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_WHITELIST_MODE, "false"));
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_WHITELIST_BUCKETS_REGEX, conf.bucketWhitelistRegexFilter));
        break;
    }

    if (conf.getProperties() != null) {
      properties.addAll(conf.getProperties());
    }
    return properties;
  }

  @Override
  public CreateTableEntry createNewTable(
    NamespaceKey tableSchemaPath, SchemaConfig config,
    IcebergTableProps icebergProps,
    WriterOptions writerOptions,
    Map<String, Object> storageOptions,
    boolean isResultsTable
  ) {
    Preconditions.checkArgument(tableSchemaPath.size() >= 2, "key must be at least two parts");
    final String containerName = tableSchemaPath.getPathComponents().get(1);
    if (tableSchemaPath.size() == 2) {
      throw UserException.validationError()
        .message("Creating buckets is not supported (name: %s)", containerName)
        .build(logger);
    }

    final CreateTableEntry entry = super.createNewTable(tableSchemaPath, config, icebergProps, writerOptions, storageOptions, isResultsTable);

    final GoogleBucketFileSystem fs = getSystemUserFS().unwrap(GoogleBucketFileSystem.class);

    if (!fs.containerExists(containerName)) {
      throw UserException.validationError()
        .message("Cannot create the table because '%s' container does not exist.", containerName)
        .build(logger);
    }
    return entry;
  }

  @Override
  public boolean supportsColocatedReads() {
    return false;
  }

  @Override
  public SourceState getState() {
    try {
      GoogleBucketFileSystem fs = getSystemUserFS().unwrap(GoogleBucketFileSystem.class);
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
      return SourceState.badState(e.getMessage());
    }
  }

  @Override
  protected boolean isAsyncEnabledForQuery(OperatorContext context) {
    return context != null && context.getOptions().getOption(ASYNC_READS);
  }

  public boolean supportReadSignature(DatasetMetadata metadata, boolean isFileDataset) {
    return false;
  }

}
