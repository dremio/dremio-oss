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
package com.dremio.plugins.dataplane.store;

import static com.dremio.hadoop.security.alias.DremioCredentialProvider.DREMIO_SCHEME_PREFIX;
import static com.dremio.plugins.azure.AbstractAzureStorageConf.AccountKind.STORAGE_V2;
import static com.dremio.plugins.azure.AzureAuthenticationType.ACCESS_KEY;
import static com.dremio.plugins.azure.AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;
import static com.dremio.plugins.gcs.GoogleStoragePlugin.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE_DEFAULT;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DefaultCtasFormatSelection;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.DoNotDisplay;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.VersionedStoragePluginConfig;
import com.dremio.exec.store.dfs.CacheProperties;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.plugins.azure.AzureAuthenticationType;
import com.dremio.plugins.azure.AzureStorageFileSystem;
import com.dremio.plugins.gcs.GCSConf.AuthMode;
import com.dremio.plugins.gcs.GoogleBucketFileSystem;
import com.dremio.plugins.s3.store.S3FileSystem;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration;
import com.google.common.base.Strings;
import io.protostuff.Tag;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Provider;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.apache.hadoop.fs.s3a.Constants;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDataplanePluginConfig
    // TODO: DX-92696: Remove the inheritance of DataplanePlugins from FileSystemConf.
    extends FileSystemConf<AbstractDataplanePluginConfig, DataplanePlugin>
    implements VersionedStoragePluginConfig {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDataplanePluginConfig.class);

  // Tag 1 is used for nessieEndpoint only in Nessie

  // Tag 2 is used for nessieAccessToken only in Nessie

  @Tag(3)
  @DisplayMetadata(label = "AWS access key")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String awsAccessKey;

  @Tag(4)
  @Secret
  @DisplayMetadata(label = "AWS access secret")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public SecretRef awsAccessSecret;

  @Tag(5)
  @DisplayMetadata(label = "AWS root path")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String awsRootPath;

  @Tag(6)
  @DisplayMetadata(label = "Connection properties")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public List<Property> propertyList = new ArrayList<>();

  @Tag(7)
  @DisplayMetadata(label = "IAM role to assume")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String assumedRoleARN;

  // Tag 8 is used for credentialType in subclasses

  @Tag(9)
  @DisplayMetadata(label = "Enable asynchronous access when possible")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public boolean asyncEnabled = true;

  @Tag(10)
  @DisplayMetadata(label = "Enable local caching when possible")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public boolean isCachingEnabled = true;

  @Tag(11)
  @Min(value = 1, message = "Max percent of total available cache space must be between 1 and 100")
  @Max(
      value = 100,
      message = "Max percent of total available cache space must be between 1 and 100")
  @DisplayMetadata(label = "Max percent of total available cache space to use when possible")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public int maxCacheSpacePct = 100;

  @Tag(12)
  @DoNotDisplay
  @DisplayMetadata(label = "Default CTAS format")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public DefaultCtasFormatSelection defaultCtasFormat = DefaultCtasFormatSelection.ICEBERG;

  // Tag 13 is reserved

  // Tag 14 is used for nessieAuthType only in Nessie

  // Tag 15 is used for awsProfile only in Nessie

  // Tag 16 is used for secure only in Nessie

  public enum StorageProviderType {
    @Tag(1)
    @DisplayMetadata(label = "AWS")
    AWS,

    @Tag(2)
    @DisplayMetadata(label = "Azure")
    AZURE,

    @Tag(3)
    @DisplayMetadata(label = "Google (Preview)")
    GOOGLE,
  }

  @Tag(17)
  @DisplayMetadata(label = "Storage provider")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  // Warning: This is public for easy Json deserialization. Do not access it directly, use
  // getStorageProvider() instead.
  public StorageProviderType storageProvider;

  @Tag(18)
  @DisplayMetadata(label = "Azure storage account name")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String azureStorageAccount;

  @Tag(19)
  @DisplayMetadata(label = "Azure root path")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String azureRootPath;

  @Tag(20)
  @DisplayMetadata(label = "Authentication method")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public AzureAuthenticationType azureAuthenticationType;

  @Tag(21)
  @Secret
  @DisplayMetadata(label = "Shared access key")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public SecretRef azureAccessKey;

  @Tag(22)
  @DisplayMetadata(label = "Application ID")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String azureApplicationId;

  @Tag(23)
  @Secret
  @DisplayMetadata(label = "Client secret")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public SecretRef azureClientSecret;

  @Tag(24)
  @DisplayMetadata(label = "OAuth 2.0 token endpoint")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String azureOAuthTokenEndpoint;

  @Tag(25)
  @DisplayMetadata(label = "Google project ID")
  public String googleProjectId;

  @Tag(26)
  @DisplayMetadata(label = "Google root path")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String googleRootPath;

  @Tag(27)
  @DisplayMetadata(label = "Authentication method")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public AuthMode googleAuthenticationType;

  @Tag(28)
  @DisplayMetadata(label = "Private key ID")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String googlePrivateKeyId;

  @Tag(29)
  @Secret
  @DisplayMetadata(label = "Private key")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public SecretRef googlePrivateKey;

  @Tag(30)
  @DisplayMetadata(label = "Client email")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String googleClientEmail;

  @Tag(31)
  @DisplayMetadata(label = "Client ID")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String googleClientId;

  @Override
  public CacheProperties getCacheProperties() {
    return new CacheProperties() {
      @Override
      public boolean isCachingEnabled(OptionManager optionManager) {
        return isCachingEnabled;
      }

      @Override
      public int cacheMaxSpaceLimitPct() {
        return maxCacheSpacePct;
      }
    };
  }

  @Override
  public abstract DataplanePlugin newPlugin(
      SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider);

  @Override
  public boolean isAsyncEnabled() {
    return asyncEnabled;
  }

  public StorageProviderType getStorageProvider() {
    // Legacy plugin configs before supporting multiple storage types won't have this property set,
    // assume AWS if not set.
    if (storageProvider == null) {
      return StorageProviderType.AWS;
    }
    return storageProvider;
  }

  public String getRootPath() {
    switch (getStorageProvider()) {
      case AWS:
        return awsRootPath;
      case AZURE:
        return azureRootPath;
      case GOOGLE:
        return googleRootPath;
      default:
        throw new IllegalStateException("Unexpected value: " + getStorageProvider());
    }
  }

  @Override
  public Path getPath() {
    // This is not actually allowed to be unset. This is called in FileSystemPlugin's constructor
    // and we don't want to fail there since the error message is not as clear. Empty paths will
    // fail instead during DataplanePlugin#start when calling validateRootPath.
    if (Strings.isNullOrEmpty(getRootPath())) {
      return null;
    }

    return Path.of(getRootPath());
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public String getConnection() {
    switch (getStorageProvider()) {
      case AWS:
        return CloudFileSystemScheme.S3_FILE_SYSTEM_SCHEME.getScheme() + ":///";
      case AZURE:
        return CloudFileSystemScheme.AZURE_STORAGE_FILE_SYSTEM_SCHEME.getScheme() + "/";
      case GOOGLE:
        return CloudFileSystemScheme.GOOGLE_CLOUD_FILE_SYSTEM.getScheme() + ":///";
      default:
        throw new IllegalStateException("Unexpected value: " + getStorageProvider());
    }
  }

  @Override
  public boolean isPartitionInferenceEnabled() {
    return false;
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.USER_TABLE;
  }

  @Override
  public List<Property> getProperties() {
    final List<Property> properties =
        propertyList != null ? new ArrayList<>(propertyList) : new ArrayList<>();

    switch (getStorageProvider()) {
      case AWS:
        properties.add(new Property("fs.dremioS3.impl", S3FileSystem.class.getName()));
        properties.add(new Property("fs.dremioS3.impl.disable.cache", "true"));
        // Disable features of S3AFileSystem which incur unnecessary performance overhead.
        // User-provided values for these properties take precedence.
        addPropertyIfNotPresent(
            properties, new Property(Constants.CREATE_FILE_STATUS_CHECK, "false"));
        addPropertyIfNotPresent(
            properties,
            new Property(
                Constants.DIRECTORY_MARKER_POLICY, Constants.DIRECTORY_MARKER_POLICY_KEEP));
        break;
      case AZURE:
        if (Strings.isNullOrEmpty(azureStorageAccount)) {
          throw UserException.validationError()
              .message(
                  "Failure creating an Azure connection. You must provide an Azure storage account name [azureStorageAccount].")
              .build(logger);
        }

        properties.add(
            new Property("fs.dremioAzureStorage.impl", AzureStorageFileSystem.class.getName()));
        properties.add(new Property("fs.dremioAzureStorage.impl.disable.cache", "true"));
        properties.add(new Property(AzureStorageFileSystem.MODE, STORAGE_V2.name()));
        properties.add(new Property(AzureStorageFileSystem.ACCOUNT, azureStorageAccount));
        properties.add(new Property(AzureStorageFileSystem.ROOT_PATH, azureRootPath));
        applyAzureAuthenticationProperties(properties);
        break;
      case GOOGLE:
        if (Strings.isNullOrEmpty(googleProjectId)) {
          throw UserException.validationError()
              .message(
                  "Failure creating a GCS connection. You must provide a Google project ID [googleProjectId].")
              .build(logger);
        }

        properties.add(new Property("fs.dremiogcs.impl", GoogleBucketFileSystem.class.getName()));
        properties.add(new Property("fs.dremiogcs.impl.disable.cache", "true"));
        addPropertyIfNotPresent(
            properties,
            new Property(
                GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getKey(),
                GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE_DEFAULT));
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_PROJECT_ID, googleProjectId));
        applyGoogleAuthenticationProperties(properties);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + getStorageProvider());
    }

    return properties;
  }

  private void applyAzureAuthenticationProperties(List<Property> properties) {
    if (azureAuthenticationType == null) {
      throw UserException.validationError()
          .message(
              "Failure creating an Azure connection. You must provide an authentication method [azureAuthenticationType].")
          .build(logger);
    }

    switch (azureAuthenticationType) {
      case ACCESS_KEY:
        if (SecretRef.isNullOrEmpty(azureAccessKey)) {
          throw UserException.validationError()
              .message(
                  "Failure creating an Azure connection. You must provide a shared access key [azureAccessKey].")
              .build(logger);
        }

        properties.add(new Property(AzureStorageFileSystem.CREDENTIALS_TYPE, ACCESS_KEY.name()));
        properties.add(
            new Property(
                AzureStorageFileSystem.KEY,
                SecretRef.toConfiguration(azureAccessKey, DREMIO_SCHEME_PREFIX)));
        properties.add(
            new Property(
                AzureStorageFileSystem.AZURE_SHAREDKEY_SIGNER_TYPE,
                SharedKeyCredentials.class.getName()));
        break;
      case AZURE_ACTIVE_DIRECTORY:
        if (Strings.isNullOrEmpty(azureApplicationId)
            || SecretRef.isNullOrEmpty(azureClientSecret)
            || Strings.isNullOrEmpty(azureOAuthTokenEndpoint)) {
          throw UserException.validationError()
              .message(
                  "Failure creating an Azure connection. You must provide an application ID, client secret, and OAuth endpoint [azureApplicationId, azureClientSecret, azureOAuthTokenEndpoint].")
              .build(logger);
        }

        properties.add(
            new Property(AzureStorageFileSystem.CREDENTIALS_TYPE, AZURE_ACTIVE_DIRECTORY.name()));
        properties.add(new Property(AzureStorageFileSystem.CLIENT_ID, azureApplicationId));
        properties.add(
            new Property(
                AzureStorageFileSystem.CLIENT_SECRET,
                SecretRef.toConfiguration(azureClientSecret, DREMIO_SCHEME_PREFIX)));
        properties.add(
            new Property(AzureStorageFileSystem.TOKEN_ENDPOINT, azureOAuthTokenEndpoint));
        break;
      default:
        throw new IllegalStateException("Unrecognized credential type: " + azureAuthenticationType);
    }
  }

  private void applyGoogleAuthenticationProperties(List<Property> properties) {
    if (googleAuthenticationType == null) {
      throw UserException.validationError()
          .message(
              "Failure creating a GCS connection. You must provide an authentication method [googleAuthenticationType].")
          .build(logger);
    }
    switch (googleAuthenticationType) {
      case SERVICE_ACCOUNT_KEYS:
        if (Strings.isNullOrEmpty(googleClientId)
            || Strings.isNullOrEmpty(googleClientEmail)
            || Strings.isNullOrEmpty(googlePrivateKeyId)
            || SecretRef.isNullOrEmpty(googlePrivateKey)) {
          throw UserException.validationError()
              .message(
                  "Failure creating a GCS connection. You must provide a client email, client ID, private key ID, and private key [googleClientEmail, googleClientId, googlePrivateKeyId, googlePrivateKey].")
              .build(logger);
        }

        properties.add(new Property(GoogleBucketFileSystem.DREMIO_KEY_FILE, "true"));
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_CLIENT_ID, googleClientId));
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_CLIENT_EMAIL, googleClientEmail));
        properties.add(
            new Property(GoogleBucketFileSystem.DREMIO_PRIVATE_KEY_ID, googlePrivateKeyId));
        properties.add(
            new Property(
                GoogleBucketFileSystem.DREMIO_PRIVATE_KEY,
                SecretRef.toConfiguration(googlePrivateKey, DREMIO_SCHEME_PREFIX)));
        break;
      case AUTO:
        properties.add(new Property(GoogleBucketFileSystem.DREMIO_KEY_FILE, "false"));
        break;
      default:
        throw new IllegalStateException(
            "Unrecognized credential type: " + googleAuthenticationType);
    }
  }

  @Override
  public String getDefaultCtasFormat() {
    return defaultCtasFormat.getDefaultCtasFormat();
  }

  // TODO: DX-92705: Move to the NessiePlugins.
  protected NessieApiV2 getNessieRestClient(
      String name, String nessieEndpoint, SecretRef nessieAccessToken) {
    final NessieClientBuilder builder =
        NessieClientBuilder.createClientBuilder("HTTP", null).withUri(URI.create(nessieEndpoint));

    if (!SecretRef.isNullOrEmpty(nessieAccessToken)) {
      builder.withAuthentication(new SecureBearerAuthentication(nessieAccessToken));
    }

    try {
      return builder.withTracing(true).withApiCompatibilityCheck(false).build(NessieApiV2.class);
    } catch (IllegalArgumentException e) {
      throw UserException.resourceError(e)
          .message(
              "Unable to create source [%s], " + "%s must be a valid http or https address",
              name, nessieEndpoint)
          .build(logger);
    }
  }

  public abstract String getSourceTypeName();

  private static void addPropertyIfNotPresent(List<Property> propertyList, Property property) {
    if (propertyList.stream().noneMatch(p -> p.name.equals(property.name))) {
      propertyList.add(property);
    }
  }
}
