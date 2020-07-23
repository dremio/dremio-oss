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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ALLOW_REQUESTER_PAYS;
import static org.apache.hadoop.fs.s3a.Constants.CREATE_FILE_STATUS_CHECK;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_THREADS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_TOTAL_TASKS;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
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
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.io.file.FileSystem;
import com.dremio.plugins.util.ContainerFileSystem.ContainerFailure;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * S3 Extension of FileSystemStoragePlugin
 */
public class S3StoragePlugin extends FileSystemPlugin<S3PluginConfig> {

  private static final Logger logger = LoggerFactory.getLogger(S3StoragePlugin.class);

  /**
   * Controls how many parallel connections HttpClient spawns.
   * Hadoop configuration property {@link org.apache.hadoop.fs.s3a.Constants#MAXIMUM_CONNECTIONS}.
   */
  public static final int DEFAULT_MAX_CONNECTIONS = 1000;
  public static final String EXTERNAL_BUCKETS = "dremio.s3.external.buckets";
  public static final String WHITELISTED_BUCKETS = "dremio.s3.whitelisted.buckets";

  // AWS Credential providers
  public static final String ACCESS_KEY_PROVIDER = SimpleAWSCredentialsProvider.NAME;
  public static final String EC2_METADATA_PROVIDER = "com.amazonaws.auth.InstanceProfileCredentialsProvider";
  public static final String NONE_PROVIDER = AnonymousAWSCredentialsProvider.NAME;
  public static final String ASSUME_ROLE_PROVIDER = "com.dremio.plugins.s3.store.STSCredentialProviderV1";

  public S3StoragePlugin(S3PluginConfig config, SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
  }

  @Override
  protected List<Property> getProperties() {
    final S3PluginConfig config = getConfig();
    final List<Property> finalProperties = new ArrayList<>();
    finalProperties.add(new Property(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, "dremioS3:///"));
    finalProperties.add(new Property("fs.dremioS3.impl", S3FileSystem.class.getName()));
    finalProperties.add(new Property(MAXIMUM_CONNECTIONS, String.valueOf(DEFAULT_MAX_CONNECTIONS)));
    finalProperties.add(new Property(FAST_UPLOAD, "true"));
    finalProperties.add(new Property(Constants.FAST_UPLOAD_BUFFER, "disk"));
    finalProperties.add(new Property(Constants.FAST_UPLOAD_ACTIVE_BLOCKS, "4")); // 256mb (so a single parquet file should be able to flush at once).
    finalProperties.add(new Property(MAX_THREADS, "24"));
    finalProperties.add(new Property(MULTIPART_SIZE, "67108864")); // 64mb
    finalProperties.add(new Property(MAX_TOTAL_TASKS, "30"));

    if(config.compatibilityMode) {
      finalProperties.add(new Property(S3FileSystem.COMPATIBILITY_MODE, "true"));
    }

    String mainAWSCredProvider;

    switch (config.credentialType) {
      case ACCESS_KEY:
        if (("".equals(config.accessKey)) || ("".equals(config.accessSecret))) {
          throw UserException.validationError()
            .message("Failure creating S3 connection. You must provide AWS Access Key and AWS Access Secret.")
            .build(logger);
        }
        mainAWSCredProvider = ACCESS_KEY_PROVIDER;
        finalProperties.add(new Property(ACCESS_KEY, config.accessKey));
        finalProperties.add(new Property(SECRET_KEY, config.accessSecret));
        break;
      case EC2_METADATA:
        mainAWSCredProvider = EC2_METADATA_PROVIDER;
        break;
      case NONE:
        mainAWSCredProvider = NONE_PROVIDER;
        break;
      default:
        throw new RuntimeException("Failure creating S3 connection. Invalid credentials type.");
    }

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
      if (config.credentialType == AWSAuthenticationType.NONE) {
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
      ensureDefaultName();
      S3FileSystem fs = getSystemUserFS().unwrap(S3FileSystem.class);
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

      return SourceState.warnState(sb.toString());

    } catch (Exception e) {
      return SourceState.badState(e);
    }
  }

  private void ensureDefaultName() throws IOException {
    String urlSafeName = URLEncoder.encode(getName(), "UTF-8");
    getFsConf().set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, "dremioS3://" + urlSafeName);
    // we create a new fs wrapper since we are calling initialize on it
    final FileSystem fs = createFS(SYSTEM_USERNAME);
    // do not use fs.getURI() or fs.getConf() directly as they will produce wrong results
    fs.unwrap(org.apache.hadoop.fs.FileSystem.class).initialize(URI.create(getFsConf().get(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY)), getFsConf());
  }

  @Override
  public CreateTableEntry createNewTable(
    SchemaConfig config,
    NamespaceKey key,
    IcebergTableProps icebergTableProps,
    WriterOptions writerOptions,
    Map<String, Object> storageOptions
  ) {
    Preconditions.checkArgument(key.size() >= 2, "key must be at least two parts");
    final List<String> resolvedPath = resolveTableNameToValidPath(key.getPathComponents()); // strips source name
    final String containerName = resolvedPath.get(0);
    if (resolvedPath.size() == 1) {
      throw UserException.validationError()
        .message("Creating buckets is not supported", containerName)
        .build(logger);
    }

    final CreateTableEntry entry = super.createNewTable(config, key,
      icebergTableProps, writerOptions, storageOptions);

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
}
