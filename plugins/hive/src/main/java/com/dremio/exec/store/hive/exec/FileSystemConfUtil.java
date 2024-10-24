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
package com.dremio.exec.store.hive.exec;

import com.dremio.common.FSConstants;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import static com.dremio.io.file.UriSchemes.AZURE_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_AZURE_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_HDFS_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_S3_SCHEME;
import static com.dremio.io.file.UriSchemes.GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.HDFS_SCHEME;
import static com.dremio.io.file.UriSchemes.S3_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET;

public class FileSystemConfUtil {

  private static final String FS_S3A_BUCKET = "fs.s3a.bucket.";
  private static final String FS_S3A_AWS_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";
  private static final String FS_S3_MAXIMUM_CONNECTIONS = "fs.s3a.connection.maximum";
  private static final String FS_S3_FAST_UPLOAD = "fs.s3a.fast.upload";
  private static final String FS_S3_FAST_UPLOAD_BUFFER = "fs.s3a.fast.upload.buffer";
  private static final String FS_S3_FAST_UPLOAD_ACTIVE_BLOCKS = "fs.s3a.fast.upload.active.blocks";
  private static final String FS_S3_MAX_THREADS = "fs.s3a.threads.max";
  private static final String FS_S3_MULTIPART_SIZE = "fs.s3a.multipart.size";
  private static final String FS_S3_MAX_TOTAL_TASKS = "fs.s3a.max.total.tasks";

  public static final String FS_DREMIO_S3_IMPL = "fs.dremioS3.impl";
  public static final String FS_DREMIO_GCS_IMPL = "fs.dremiogcs.impl";
  public static final String FS_DREMIO_AZURE_IMPL = "fs.dremioAzureStorage.impl";
  public static final String FS_DREMIO_HDFS_IMPL = "fs.hdfs.impl";

  public static final Set<String> GCS_FILE_SYSTEM = ImmutableSet.of(GCS_SCHEME, DREMIO_GCS_SCHEME);
  public static final Set<String> S3_FILE_SYSTEM =
      ImmutableSet.of("s3a", S3_SCHEME, "s3n", DREMIO_S3_SCHEME);
  public static final Set<String> AZURE_FILE_SYSTEM =
      ImmutableSet.of(AZURE_SCHEME, "wasb", "abfs", "abfss");
  public static final Set<String> HDFS_FILE_SYSTEM = ImmutableSet.of(HDFS_SCHEME);

  public static final ImmutableMap<String, String> ADL_PROPS =
      ImmutableMap.of(
          "fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem",
          "fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");

  // Azure WASB and WASBS file system implementation
  public static final ImmutableMap<String, String> WASB_PROPS =
      ImmutableMap.of(
          "fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
          "fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb",
          "fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure",
          "fs.AbstractFileSystem.wasbs.impl", "org.apache.hadoop.fs.azure.Wasbs");

  // Azure ABFS and ABFSS file system implementation
  public static final ImmutableMap<String, String> ABFS_PROPS =
      ImmutableMap.of(
          "fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
          "fs.AbstractFileSystem.abfs.impl", "org.apache.hadoop.fs.azurebfs.Abfs",
          "fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
          "fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfss");

  public static ImmutableMap<String, String> S3_PROPS =
      ImmutableMap.of(FS_S3_MAXIMUM_CONNECTIONS, "1000",
          FS_S3_FAST_UPLOAD, "true",
          FS_S3_FAST_UPLOAD_BUFFER, "disk",
          FS_S3_FAST_UPLOAD_ACTIVE_BLOCKS, "4",
          FS_S3_MAX_THREADS, "24",
          FS_S3_MULTIPART_SIZE, "67108864",
          FS_S3_MAX_TOTAL_TASKS, "30");

  public static ImmutableMap<String, String> FS_CACHE_DISABLES =
      ImmutableMap.of("fs.dremioS3.impl.disable.cache", "true",
      "fs.dremiogcs.impl.disable.cache", "true",
      "fs.dremioAzureStorage.impl.disable.cache", "true",
      "fs.dremioAdl.impl.disable.cache", "true");

  public static void initializeConfiguration(URI name, Configuration conf) throws IOException {
    switch (name.getScheme()) {
      case DREMIO_S3_SCHEME:
        conf.set(FS_DREMIO_S3_IMPL, "com.dremio.plugins.s3.store.S3FileSystem");
        updateS3Properties(conf, name);
        break;
      case DREMIO_HDFS_SCHEME:
        conf.set(FS_DREMIO_HDFS_IMPL, "org.apache.hadoop.hdfs.DistributedFileSystem");
        break;
      case DREMIO_AZURE_SCHEME:
        conf.set(FS_DREMIO_AZURE_IMPL, "com.dremio.plugins.azure.AzureStorageFileSystem");
        updateAzureConfiguration(conf, name);
        break;
      case DREMIO_GCS_SCHEME:
        conf.set(FS_DREMIO_GCS_IMPL, "com.dremio.plugins.gcs.GoogleBucketFileSystem");
        break;
      default:
        throw new UnsupportedOperationException("Unsupported async read path for hive parquet: " + name.getScheme());
    }
  }

  private static void updateAzureConfiguration(Configuration conf, URI uri) {
    // default is key based, same as azure sources
    String accountName = getAccountNameFromURI(conf.get("authority"), uri);
    // strip any url information if any
    String accountNameWithoutSuffix = accountName.split("[.]")[0];
    conf.set("dremio.azure.account", accountNameWithoutSuffix);
    String authType = getAuthTypeForAccount(conf, accountName, accountNameWithoutSuffix);
    String key = null;

    String old_scheme = conf.get("old_scheme");
    if (old_scheme.equals(FileSystemUriSchemes.WASB_SCHEME) || old_scheme.equals(FileSystemUriSchemes.WASB_SECURE_SCHEME)) {
      conf.setIfUnset("dremio.azure.mode", "STORAGE_V1");
    } else if (old_scheme.equals(FileSystemUriSchemes.ABFS_SCHEME) || old_scheme.equals(FileSystemUriSchemes.ABFS_SECURE_SCHEME)) {
      conf.setIfUnset("dremio.azure.mode", "STORAGE_V2");
    }

    if (authType.equals(AuthType.SharedKey.name())) {
      key = getValueForProperty(conf, FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME, accountName,
          accountNameWithoutSuffix, "Account Key not present in the configuration.");
      conf.set("dremio.azure.key", key);
      conf.set("dremio.azure.credentialsType", "ACCESS_KEY");
    } else if (authType.equals(AuthType.OAuth.name())) {
      updateOAuthConfig(conf, accountName, accountNameWithoutSuffix);
      conf.set("dremio.azure.credentialsType", "AZURE_ACTIVE_DIRECTORY");
    } else {
      throw new UnsupportedOperationException("This credentials type is not supported " + authType);
    }

  }

  private static String getAuthTypeForAccount(Configuration conf, String accountName, String accountNameWithoutSuffix) {
    // try with entire authority (includes destination), fall back to account name without suffix
    String authType = conf.get(getAccountConfigurationName(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME,
        accountName), conf.get(getAccountConfigurationName(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME,
        accountNameWithoutSuffix)));
    if (authType != null) {
      return authType;
    }
    // fall back to property name without account info and a default value
    return conf.get(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey.name());
  }

  private static String getAccountConfigurationName(String fsAzurePropertyName, String accountName) {
    return fsAzurePropertyName + "." + accountName;
  }

  static void updateOAuthConfig(Configuration conf, String accountName, String accountNameWithoutSuffix) {
    String refreshToken = getValueForProperty(conf, FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT,
        accountName, accountNameWithoutSuffix, "OAuth Client Endpoint not found.");
    String clientId = getValueForProperty(conf, FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID, accountName,
        accountNameWithoutSuffix, "OAuth Client Id not found.");
    String password = getValueForProperty(conf, FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET, accountName,
        accountNameWithoutSuffix, "OAuth Client Password not found.");
    conf.set(FSConstants.AZURE_CLIENT_ID, clientId);
    conf.set(FSConstants.AZURE_TOKEN_ENDPOINT, refreshToken);
    conf.set(FSConstants.AZURE_CLIENT_SECRET, password);
  }

  private static String getValueForProperty(Configuration conf, String propertyName,
                                            String accountName, String accountNameWithoutSuffix,
                                            String errMsg) {
    String property = conf.get(getAccountConfigurationName(propertyName, accountName),
        conf.get(getAccountConfigurationName(propertyName, accountNameWithoutSuffix)));
    if (property != null) {
      return property;
    }
    property = conf.get(propertyName);
    Preconditions.checkState(StringUtils.isNotEmpty(property), errMsg);
    return property;
  }

  private static String getAccountNameFromURI(String authority, URI uri) {
    if (null == authority) {
      throw new IllegalArgumentException("Malformed URI : " + uri.toString());
    } else if (!authority.contains("@")) {
      throw new IllegalArgumentException("Malformed URI : " + uri.toString());
    } else {
      String[] authorityParts = authority.split("@", 2);
      if (authorityParts.length >= 2 && (authorityParts[0] == null || !authorityParts[0].isEmpty())) {
        return authorityParts[1];
      } else {
        String errMsg = String.format("'%s' has a malformed authority, expected container name. Authority takes the form abfs://[<container name>@]<account name>", uri.toString());
        throw new IllegalArgumentException(errMsg);
      }
    }
  }

  private static void updateS3Properties(Configuration conf, URI originalURI) {
    // Hadoop s3 supports a default list of 1. Basic Credentials
    // 2. Environment variables 3. Instance role
    // If provider is not set in configuration try to derive one of the three.
    if (StringUtils.isEmpty(getCredentialsProvider(conf, originalURI))) {
      if (!StringUtils.isEmpty(getAccessKey(conf, originalURI))) {
        conf.set(FS_S3A_AWS_CREDENTIALS_PROVIDER, "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
      } else if (System.getenv("AWS_ACCESS_KEY_ID") != null || System.getenv("AWS_ACCESS_KEY") != null) {
        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        if (accessKey == null) {
          accessKey = System.getenv("AWS_ACCESS_KEY");
        }

        String secretKey = System.getenv("AWS_SECRET_KEY");
        if (secretKey == null) {
          secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        }
        conf.set(FS_S3A_AWS_CREDENTIALS_PROVIDER, "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        conf.set(FSConstants.FS_S3A_ACCESS_KEY, accessKey);
        conf.set(FSConstants.FS_S3A_SECRET_KEY, secretKey);
      } else {
        conf.set(FS_S3A_AWS_CREDENTIALS_PROVIDER, "com.amazonaws.auth.InstanceProfileCredentialsProvider");
      }
    }
    // copy over bucket properties to source configuration
    updateSourcePropsFromBucketProps(conf, originalURI);
  }

  private static void updateSourcePropsFromBucketProps(Configuration conf, URI originalURI) {
    String bucketConfName = FS_S3A_BUCKET + originalURI.getAuthority() + ".access.key";
    String bucketAccessKey = conf.get(bucketConfName);
    if (StringUtils.isNotEmpty(bucketAccessKey)) {
      // dremio s3 file system does not support bucket overrides
      conf.set(FSConstants.FS_S3A_ACCESS_KEY, bucketAccessKey);
    }
    String bucketSecretConfName = FS_S3A_BUCKET + originalURI.getAuthority() + ".secret.key";
    String bucketSecretKey = conf.get(bucketSecretConfName);
    if (StringUtils.isNotEmpty(bucketSecretKey)) {
      // dremio s3 file system does not support bucket overrides
      conf.set(FSConstants.FS_S3A_SECRET_KEY, bucketSecretKey);
    }

    String endpointForBucket = conf.get(FS_S3A_BUCKET + originalURI.getAuthority() + ".endpoint");
    if (StringUtils.isNotEmpty(endpointForBucket)) {
      conf.set("fs.s3a.endpoint", endpointForBucket);
    }

    String credentialsProviderForBucket = conf.get(FS_S3A_BUCKET + originalURI.getAuthority() +
        ".aws.credentials.provider");
    if (StringUtils.isNotEmpty(credentialsProviderForBucket)) {
      conf.set("fs.s3a.aws.credentials.provider", credentialsProviderForBucket);
    }

    String httpSchemeForBucket = conf.get(FS_S3A_BUCKET + originalURI.getAuthority() +
        ".connection.ssl.enabled");
    if (StringUtils.isNotEmpty(httpSchemeForBucket)) {
      conf.set("fs.s3a.connection.ssl.enabled", httpSchemeForBucket);
    }
  }

  private static String getAccessKey(Configuration conf, URI originalURI) {
    String bucketConfName = FS_S3A_BUCKET + originalURI.getAuthority() + ".access.key";
    return conf.get(bucketConfName, conf.get(FSConstants.FS_S3A_ACCESS_KEY));
  }

  private static String getCredentialsProvider(Configuration conf, URI originalURI) {
    String bucketConfName = FS_S3A_BUCKET + originalURI.getAuthority() + ".aws.credentials" +
        ".provider";
    return conf.get(bucketConfName, conf.get(FS_S3A_AWS_CREDENTIALS_PROVIDER));
  }

}
