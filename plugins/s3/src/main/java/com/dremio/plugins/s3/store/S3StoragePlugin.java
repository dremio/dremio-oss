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
package com.dremio.plugins.s3.store;

import static org.apache.hadoop.fs.s3a.Constants.ALLOW_REQUESTER_PAYS;
import static org.apache.hadoop.fs.s3a.Constants.CREATE_FILE_STATUS_CHECK;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_THREADS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_TOTAL_TASKS;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;

import java.nio.file.AccessMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Provider;

import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.S3ConnectionConstants;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.DirectorySupportLackingFileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.io.file.Path;
import com.dremio.plugins.util.ContainerFileSystem.ContainerFailure;
import com.dremio.plugins.util.awsauth.AWSCredentialsConfigurator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

/**
 * S3 Extension of FileSystemStoragePlugin
 */
public class S3StoragePlugin extends DirectorySupportLackingFileSystemPlugin<AbstractS3PluginConfig> {

  private static final Logger logger = LoggerFactory.getLogger(S3StoragePlugin.class);

  public static final String EXTERNAL_BUCKETS = "dremio.s3.external.buckets";
  public static final String WHITELISTED_BUCKETS = "dremio.s3.whitelisted.buckets";

  // AWS Credential providers
  public static final String ACCESS_KEY_PROVIDER = SimpleAWSCredentialsProvider.NAME;
  public static final String EC2_METADATA_PROVIDER = "com.amazonaws.auth.InstanceProfileCredentialsProvider";
  public static final String NONE_PROVIDER = AnonymousAWSCredentialsProvider.NAME;
  public static final String ASSUME_ROLE_PROVIDER = "com.dremio.plugins.s3.store.STSCredentialProviderV1";
  // Credential provider for DCS data roles
  public static final String DREMIO_ASSUME_ROLE_PROVIDER = "com.dremio.service.coordinator" +
    ".DremioAssumeRoleCredentialsProviderV1";
  public static final String AWS_PROFILE_PROVIDER = "com.dremio.plugins.s3.store.AWSProfileCredentialsProviderV1";
  public static final String WEB_IDENTITY_TOKEN_PROVIDER = "com.dremio.plugins.s3.store.WebIdentityCredentialsProviderV1";

  private final AWSCredentialsConfigurator awsCredentialsConfigurator;
  private final boolean isAuthTypeNone;

  public S3StoragePlugin(AbstractS3PluginConfig config, SabotContext context, String name, Provider<StoragePluginId> idProvider,
                         AWSCredentialsConfigurator awsCredentialsConfigurator, boolean isAuthTypeNone) {
    super(config, context, name, idProvider);
    this.awsCredentialsConfigurator = awsCredentialsConfigurator;
    this.isAuthTypeNone = isAuthTypeNone;
  }

  @Override
  protected List<Property> getProperties() {
    final AbstractS3PluginConfig config = getConfig();
    final List<Property> finalProperties = new ArrayList<>();
    finalProperties.add(new Property(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, "dremioS3:///"));
    finalProperties.add(new Property("fs.dremioS3.impl", S3FileSystem.class.getName()));
    finalProperties.add(new Property("fs.dremioS3.impl.disable.cache","true"));
    finalProperties.add(new Property(MAXIMUM_CONNECTIONS, String.valueOf(S3ConnectionConstants.DEFAULT_MAX_CONNECTIONS)));
    finalProperties.add(new Property(FAST_UPLOAD, "true"));
    finalProperties.add(new Property(Constants.FAST_UPLOAD_BUFFER, "disk"));
    finalProperties.add(new Property(Constants.FAST_UPLOAD_ACTIVE_BLOCKS, "4")); // 256mb (so a single parquet file should be able to flush at once).
    finalProperties.add(new Property(MAX_THREADS, String.valueOf(S3ConnectionConstants.DEFAULT_MAX_THREADS)));
    finalProperties.add(new Property(MULTIPART_SIZE, "67108864")); // 64mb
    finalProperties.add(new Property(MAX_TOTAL_TASKS, "30"));

    if(config.compatibilityMode) {
      finalProperties.add(new Property(S3FileSystem.COMPATIBILITY_MODE, "true"));
    }

    String mainAWSCredProvider = awsCredentialsConfigurator.configureCredentials(finalProperties);
    logger.debug("For source:{}, mainCredentialsProvider determined is {}", getName(), mainAWSCredProvider);

    if (!Strings.isNullOrEmpty(config.assumedRoleARN) && !NONE_PROVIDER.equals(mainAWSCredProvider)) {
      finalProperties.add(new Property(Constants.ASSUMED_ROLE_ARN, config.assumedRoleARN));
      finalProperties.add(new Property(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER, mainAWSCredProvider));
      mainAWSCredProvider = ASSUME_ROLE_PROVIDER;
    }

    finalProperties.add(new Property(Constants.AWS_CREDENTIALS_PROVIDER, mainAWSCredProvider));

    final List<Property> propertyList = super.getProperties();
    if (propertyList != null && !propertyList.isEmpty()) {
      finalProperties.addAll(propertyList);
    }

    finalProperties.add(new Property(SECURE_CONNECTIONS, String.valueOf(config.secure)));
    if (config.externalBucketList != null && !config.externalBucketList.isEmpty()) {
      finalProperties.add(new Property(EXTERNAL_BUCKETS, Joiner.on(",").join(config.externalBucketList)));
    } else {
      if (isAuthTypeNone) {
        throw UserException.validationError()
          .message("Failure creating S3 connection. You must provide one or more external buckets when you choose no authentication.")
          .build(logger);
      }
    }

    if (config.whitelistedBuckets != null && !config.whitelistedBuckets.isEmpty()) {
      finalProperties.add(new Property(WHITELISTED_BUCKETS, Joiner.on(",").join(config.whitelistedBuckets)));
    }

    if (!Strings.isNullOrEmpty(config.kmsKeyARN)) {
      finalProperties.add(new Property(SERVER_SIDE_ENCRYPTION_KEY, config.kmsKeyARN));
      finalProperties.add(new Property(SERVER_SIDE_ENCRYPTION_ALGORITHM, S3AEncryptionMethods.SSE_KMS.getMethod()));
      finalProperties.add(new Property(SECURE_CONNECTIONS, Boolean.TRUE.toString()));
    }

    finalProperties.add(new Property(ALLOW_REQUESTER_PAYS, Boolean.toString(config.requesterPays)));
    finalProperties.add(new Property(CREATE_FILE_STATUS_CHECK, Boolean.toString(config.enableFileStatusCheck)));
    logger.debug("getProperties: Create file status check: {}", config.enableFileStatusCheck);

    return finalProperties;
  }

  @Override
  public boolean supportsColocatedReads() {
    return false;
  }

  @Override
  public SourceState getState() {
    try {
      S3FileSystem fs = getSystemUserFS().unwrap(S3FileSystem.class);
      //This next call is just to validate that the path specified is valid
      fsHealthChecker.healthCheck(getConfig().getPath(), ImmutableSet.of(AccessMode.READ));
      fs.refreshFileSystems();
      List<ContainerFailure> failures = fs.getSubFailures();
      if(failures.isEmpty()) {
        return SourceState.GOOD;
      }
      StringBuilder sb = new StringBuilder();
      for(ContainerFailure f : failures) {
        sb.append(f.getName());
        sb.append(": ");
        sb.append(f.getException().getMessage());
        sb.append("\n");
      }
      return SourceState.warnState(fileConnectionErrorMessage(getConfig().getPath()), sb.toString());

    } catch (Exception e) {
      return SourceState.badState(fileConnectionErrorMessage(getConfig().getPath()), e);
    }
  }

  private String fileConnectionErrorMessage(Path filePath) {
    return String.format("Could not connect to %s. Check your S3 data source settings and credentials.",
        (filePath.toString().equals("/")) ? "S3 source" : filePath.toString());
  }

  @Override
  public CreateTableEntry createNewTable(
    NamespaceKey tableSchemaPath, SchemaConfig config,
    IcebergTableProps icebergTableProps,
    WriterOptions writerOptions,
    Map<String, Object> storageOptions,
    boolean isResultsTable
  ) {
    Preconditions.checkArgument(tableSchemaPath.size() >= 2, "key must be at least two parts");
    final List<String> resolvedPath = resolveTableNameToValidPath(tableSchemaPath.getPathComponents()); // strips source name
    final String containerName = resolvedPath.get(0);
    if (resolvedPath.size() == 1) {
      throw UserException.validationError()
        .message("Creating buckets is not supported (name: %s)", containerName)
        .build(logger);
    }

    final CreateTableEntry entry = super.createNewTable(tableSchemaPath, config,
      icebergTableProps, writerOptions, storageOptions, isResultsTable);

    final S3FileSystem fs = getSystemUserFS().unwrap(S3FileSystem.class);

    if (!fs.containerExists(containerName)) {
      throw UserException.validationError()
          .message("Cannot create the table because '%s' bucket does not exist", containerName)
          .build(logger);
    }

    return entry;
  }

  @Override
  protected boolean isAsyncEnabledForQuery(OperatorContext context) {
    return context != null && context.getOptions().getOption(S3Options.ASYNC);
  }

  @Override
  public boolean supportReadSignature(DatasetMetadata metadata, boolean isFileDataset) {
    return false;
  }
}
