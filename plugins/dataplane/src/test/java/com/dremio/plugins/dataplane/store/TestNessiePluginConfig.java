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

import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_AWS_STORAGE_ENABLED;
import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_AZURE_STORAGE_ENABLED;
import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_GCS_STORAGE_ENABLED;
import static com.dremio.exec.store.DataplanePluginOptions.NESSIE_PLUGIN_ENABLED;
import static com.dremio.nessiemetadata.cache.NessieMetadataCacheOptions.BYPASS_DATAPLANE_CACHE;
import static com.dremio.nessiemetadata.cache.NessieMetadataCacheOptions.DATAPLANE_ICEBERG_METADATA_CACHE_EXPIRE_AFTER_ACCESS_MINUTES;
import static com.dremio.nessiemetadata.cache.NessieMetadataCacheOptions.DATAPLANE_ICEBERG_METADATA_CACHE_SIZE_ITEMS;
import static com.dremio.plugins.NessieClientOptions.BYPASS_CONTENT_CACHE;
import static com.dremio.plugins.NessieClientOptions.NESSIE_CONTENT_CACHE_SIZE_ITEMS;
import static com.dremio.plugins.NessieClientOptions.NESSIE_CONTENT_CACHE_TTL_MINUTES;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.ACCESS_KEY_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.ASSUME_ROLE_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.AWS_PROFILE_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.EC2_METADATA_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.NONE_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.plugins.azure.AzureAuthenticationType;
import com.dremio.plugins.dataplane.store.AbstractDataplanePluginConfig.StorageProviderType;
import com.dremio.plugins.gcs.GCSConf.AuthMode;
import com.dremio.service.users.UserService;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TestNessiePluginConfig {

  private static final String SOURCE_NAME = "testNessieSource";
  public static final String NESSIE_URL = "http://localhost:19120/";
  public static final String IGNORED = "ignored";

  @Mock private SabotContext sabotContext;
  @Mock private UserService userService;
  @Mock private OptionManager optionManager;

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS) // For MethodSource
  class AwsStorageConfig {
    @Test
    public void testAwsIsStorageTypeIfUnset() {
      NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();

      assertThat(nessiePluginConfig.getStorageProvider()).isEqualTo(StorageProviderType.AWS);
    }

    @Test
    public void testAwsAccessKeyConfig() {
      String awsAccessKey = "SomeAccessKey";
      String awsAccessSecret = "SomeAccessSecret";
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();

      nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;
      nessiePluginConfig.awsAccessKey = awsAccessKey;
      nessiePluginConfig.awsAccessSecret = () -> awsAccessSecret;

      assertThat(nessiePluginConfig.getProperties())
          .containsAll(
              Arrays.asList(
                  new Property("fs.s3a.aws.credentials.provider", ACCESS_KEY_PROVIDER),
                  new Property("fs.s3a.access.key", awsAccessKey),
                  new Property("fs.s3a.secret.key", awsAccessSecret)));
    }

    @Test
    public void testAwsEc2Metadata() {
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();

      nessiePluginConfig.credentialType = AWSAuthenticationType.EC2_METADATA;

      assertThat(nessiePluginConfig.getProperties())
          .contains(new Property("fs.s3a.aws.credentials.provider", EC2_METADATA_PROVIDER));
    }

    @Test
    public void testAwsEc2MetadataAndIamRole() {
      String assumedRoleARN = "SomeAssumedRoleArn";
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();

      nessiePluginConfig.assumedRoleARN = assumedRoleARN;
      nessiePluginConfig.credentialType = AWSAuthenticationType.EC2_METADATA;

      assertThat(nessiePluginConfig.getProperties())
          .containsAll(
              Arrays.asList(
                  new Property("fs.s3a.aws.credentials.provider", ASSUME_ROLE_PROVIDER),
                  new Property("fs.s3a.assumed.role.credentials.provider", EC2_METADATA_PROVIDER),
                  new Property("fs.s3a.assumed.role.arn", assumedRoleARN)));
    }

    @Test
    public void testAwsProfileConfig() {
      String awsProfile = "SomeAwsProfile";
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();

      nessiePluginConfig.credentialType = AWSAuthenticationType.AWS_PROFILE;
      nessiePluginConfig.awsProfile = awsProfile;

      assertThat(nessiePluginConfig.getProperties())
          .containsAll(
              Arrays.asList(
                  new Property("fs.s3a.aws.credentials.provider", AWS_PROFILE_PROVIDER),
                  new Property("com.dremio.awsProfile", awsProfile)));
    }

    @Test
    public void testAwsNoneAuthConfig() {
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();

      nessiePluginConfig.credentialType = AWSAuthenticationType.NONE;

      assertThat(nessiePluginConfig.getProperties())
          .contains(new Property("fs.s3a.aws.credentials.provider", NONE_PROVIDER));
    }

    @Test
    public void testEncryptConnectionWithSecureAsFalse() {
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();
      nessiePluginConfig.credentialType = AWSAuthenticationType.NONE; // Unused, but required

      nessiePluginConfig.secure = false;

      assertThat(nessiePluginConfig.getProperties())
          .contains(new Property(SECURE_CONNECTIONS, "false"));
    }

    @Test
    public void testEncryptConnectionWithSecureAsTrue() {
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();
      nessiePluginConfig.credentialType = AWSAuthenticationType.NONE; // Unused, but required

      nessiePluginConfig.secure = true;

      assertThat(nessiePluginConfig.getProperties())
          .contains(new Property(SECURE_CONNECTIONS, "true"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "invalid..bucketName")
    public void testAwsInvalidRootPath(String awsRootPath) {
      NessiePluginConfig nessiePluginConfig = setUpForStart(StorageProviderType.AWS);
      nessiePluginConfig.awsRootPath = awsRootPath;

      DataplanePlugin nessieSource = nessiePluginConfig.newPlugin(sabotContext, SOURCE_NAME, null);

      assertThatThrownBy(nessieSource::start)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Invalid AWS S3 root path.");
    }

    @Test
    public void testAwsMissingAuthType() {
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();

      nessiePluginConfig.credentialType = null;

      assertThatThrownBy(nessiePluginConfig::getProperties)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Failure creating an S3 connection.")
          .hasMessageContaining("credentialType");
    }

    @Test
    public void testAwsMissingAuthTypeWithKeySecret() {
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();

      nessiePluginConfig.credentialType = null;
      nessiePluginConfig.awsAccessKey = IGNORED;
      nessiePluginConfig.awsAccessSecret = SecretRef.of(IGNORED);

      assertThatThrownBy(nessiePluginConfig::getProperties)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Failure creating an S3 connection.")
          .hasMessageContaining("credentialType");
    }

    private Stream<Arguments> missingAccessKeySecretConfigArguments() {
      return Stream.of(
          Arguments.of(null, null),
          Arguments.of(IGNORED, null),
          Arguments.of(null, IGNORED),
          Arguments.of("", ""),
          Arguments.of(IGNORED, ""),
          Arguments.of("", IGNORED));
    }

    @ParameterizedTest
    @MethodSource("missingAccessKeySecretConfigArguments")
    public void testAwsMissingAccessKeySecret(String awsAccessKey, String awsAccessSecret) {
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();
      nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;

      nessiePluginConfig.awsAccessKey = awsAccessKey;
      nessiePluginConfig.awsAccessSecret = SecretRef.of(awsAccessSecret);

      assertThatThrownBy(nessiePluginConfig::getProperties)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Failure creating an S3 connection.")
          .hasMessageContaining("awsAccessKey")
          .hasMessageContaining("awsAccessSecret");
    }

    @Test
    public void testNessiePluginConfigIncludesS3APerfOptimizations() {
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();
      nessiePluginConfig.credentialType = AWSAuthenticationType.NONE; // Unused, but required

      List<Property> expected =
          ImmutableList.of(
              new Property(Constants.CREATE_FILE_STATUS_CHECK, "false"),
              new Property(
                  Constants.DIRECTORY_MARKER_POLICY, Constants.DIRECTORY_MARKER_POLICY_KEEP));
      assertThat(nessiePluginConfig.getProperties()).containsAll(expected);
    }

    @Test
    public void testNessiePluginConfigCanOverrideS3APerfOptimizations() {
      NessiePluginConfig nessiePluginConfig = basicAwsConfig();
      nessiePluginConfig.credentialType = AWSAuthenticationType.NONE; // Unused, but required
      List<Property> overrides =
          ImmutableList.of(
              new Property(Constants.CREATE_FILE_STATUS_CHECK, "true"),
              new Property(
                  Constants.DIRECTORY_MARKER_POLICY, Constants.DIRECTORY_MARKER_POLICY_DELETE));

      nessiePluginConfig.propertyList = overrides;

      List<Property> notExpected =
          ImmutableList.of(
              new Property(Constants.CREATE_FILE_STATUS_CHECK, "false"),
              new Property(
                  Constants.DIRECTORY_MARKER_POLICY, Constants.DIRECTORY_MARKER_POLICY_KEEP));
      List<Property> properties = nessiePluginConfig.getProperties();
      assertThat(properties).containsAll(overrides);
      assertThat(properties).doesNotContainAnyElementsOf(notExpected);
    }
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS) // For MethodSource
  class AzureStorageConfig {
    @Test
    public void testAzureFilesystemConfig() {
      NessiePluginConfig nessiePluginConfig = basicAzureConfig();
      nessiePluginConfig.azureAuthenticationType =
          AzureAuthenticationType.ACCESS_KEY; // Unused, but required
      nessiePluginConfig.azureAccessKey = () -> IGNORED;
      final String azureStorageAccount = "SomeStorageAccount";
      final String azureRootPath = "SomeRootPath";

      nessiePluginConfig.azureStorageAccount = azureStorageAccount;
      nessiePluginConfig.azureRootPath = azureRootPath;

      assertThat(nessiePluginConfig.getProperties())
          .containsAll(
              Arrays.asList(
                  new Property("dremio.azure.mode", "STORAGE_V2"),
                  new Property("dremio.azure.account", azureStorageAccount),
                  new Property("dremio.azure.rootPath", azureRootPath)));
    }

    @Test
    public void testAzureAccessKeyConfig() {
      NessiePluginConfig nessiePluginConfig = basicAzureConfig();
      nessiePluginConfig.azureStorageAccount = IGNORED;
      final String azureAccessKey = "SomeAccessKey";

      nessiePluginConfig.azureAuthenticationType = AzureAuthenticationType.ACCESS_KEY;
      nessiePluginConfig.azureAccessKey = () -> azureAccessKey;

      assertThat(nessiePluginConfig.getProperties())
          .containsAll(
              Arrays.asList(
                  new Property("dremio.azure.credentialsType", "ACCESS_KEY"),
                  new Property("dremio.azure.key", azureAccessKey),
                  new Property(
                      "fs.azure.sharedkey.signer.type",
                      "org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials")));
    }

    @Test
    public void testAzureAadConfig() {
      NessiePluginConfig nessiePluginConfig = basicAzureConfig();
      nessiePluginConfig.azureStorageAccount = IGNORED;
      final String azureApplicationId = "SomeApplicationId";
      final String azureClientSecret = "SomeClientSecret";
      final String azureOAuthTokenEndpoint = "SomeOAuthTokenEndpoint";

      nessiePluginConfig.azureAuthenticationType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;
      nessiePluginConfig.azureApplicationId = azureApplicationId;
      nessiePluginConfig.azureClientSecret = () -> azureClientSecret;
      nessiePluginConfig.azureOAuthTokenEndpoint = azureOAuthTokenEndpoint;

      assertThat(nessiePluginConfig.getProperties())
          .containsAll(
              Arrays.asList(
                  new Property("dremio.azure.credentialsType", "AZURE_ACTIVE_DIRECTORY"),
                  new Property("dremio.azure.clientId", azureApplicationId),
                  new Property("dremio.azure.clientSecret", azureClientSecret),
                  new Property("dremio.azure.tokenEndpoint", azureOAuthTokenEndpoint)));
    }

    @Test
    public void testAzureMissingStorageAccount() {
      NessiePluginConfig nessiePluginConfig = basicAzureConfig();

      nessiePluginConfig.azureStorageAccount = null;

      assertThatThrownBy(nessiePluginConfig::getProperties)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Failure creating an Azure connection.")
          .hasMessageContaining("azureStorageAccount");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "invalid..bucketName")
    public void testAzureInvalidRootPath(String azureRootPath) {
      when(optionManager.getOption(DATAPLANE_AZURE_STORAGE_ENABLED)).thenReturn(true);
      NessiePluginConfig nessiePluginConfig = setUpForStart(StorageProviderType.AZURE);
      nessiePluginConfig.storageProvider = StorageProviderType.AZURE;
      nessiePluginConfig.azureRootPath = azureRootPath;

      DataplanePlugin nessieSource = nessiePluginConfig.newPlugin(sabotContext, SOURCE_NAME, null);

      assertThatThrownBy(nessieSource::start)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Invalid Azure root path.");
    }

    @Test
    public void testAzureMissingAuthType() {
      NessiePluginConfig nessiePluginConfig = basicAzureConfig();
      nessiePluginConfig.azureStorageAccount = IGNORED;

      nessiePluginConfig.azureAuthenticationType = null;

      assertThatThrownBy(nessiePluginConfig::getProperties)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Failure creating an Azure connection.")
          .hasMessageContaining("azureAuthenticationType");
    }

    @Test
    public void testAzureMissingAccessKey() {
      NessiePluginConfig nessiePluginConfig = basicAzureConfig();
      nessiePluginConfig.azureStorageAccount = IGNORED;
      nessiePluginConfig.azureAuthenticationType = AzureAuthenticationType.ACCESS_KEY;

      nessiePluginConfig.azureAccessKey = null;

      assertThatThrownBy(nessiePluginConfig::getProperties)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Failure creating an Azure connection.")
          .hasMessageContaining("azureAccessKey");
    }

    private Stream<Arguments> missingAadConfigArguments() {
      return Stream.of(
          Arguments.of(null, null, null),
          Arguments.of(IGNORED, null, null),
          Arguments.of(null, IGNORED, null),
          Arguments.of(null, null, IGNORED),
          Arguments.of(null, IGNORED, IGNORED),
          Arguments.of(IGNORED, null, IGNORED),
          Arguments.of(IGNORED, IGNORED, null));
    }

    @ParameterizedTest
    @MethodSource("missingAadConfigArguments")
    public void testAzureMissingAadConfig(
        String azureApplicationId, String azureClientSecret, String azureOAuthTokenEndpoint) {
      NessiePluginConfig nessiePluginConfig = basicAzureConfig();
      nessiePluginConfig.azureStorageAccount = IGNORED;
      nessiePluginConfig.azureAuthenticationType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

      nessiePluginConfig.azureApplicationId = azureApplicationId;
      nessiePluginConfig.azureClientSecret = SecretRef.of(azureClientSecret);
      nessiePluginConfig.azureOAuthTokenEndpoint = azureOAuthTokenEndpoint;

      assertThatThrownBy(nessiePluginConfig::getProperties)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Failure creating an Azure connection.")
          .hasMessageContaining("azureApplicationId")
          .hasMessageContaining("azureClientSecret")
          .hasMessageContaining("azureOAuthTokenEndpoint");
    }
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS) // For MethodSource
  class GoogleStorageConfig {
    @Test
    public void testGoogleFilesystemConfig() {
      NessiePluginConfig nessiePluginConfig = basicGoogleConfig();
      nessiePluginConfig.googleAuthenticationType = AuthMode.AUTO;
      final String googleProjectId = "SomeProjectId";

      nessiePluginConfig.googleProjectId = googleProjectId;

      assertThat(nessiePluginConfig.getProperties())
          .containsAll(List.of(new Property("dremio.gcs.projectId", googleProjectId)));
    }

    @Test
    public void testGoogleAutoConfig() {
      NessiePluginConfig nessiePluginConfig = basicGoogleConfig();
      nessiePluginConfig.googleProjectId = IGNORED;

      nessiePluginConfig.googleAuthenticationType = AuthMode.AUTO;

      assertThat(nessiePluginConfig.getProperties())
          .containsAll(List.of(new Property("dremio.gcs.use_keyfile", "false")));
    }

    @Test
    public void testGoogleServiceAccountKeysConfig() {
      NessiePluginConfig nessiePluginConfig = basicGoogleConfig();
      nessiePluginConfig.googleProjectId = IGNORED;
      final String googleClientId = "SomeClientId";
      final String googleClientEmail = "SomeClientEmail";
      final String googlePrivateKeyId = "SomePrivateKeyId";
      final String googlePrivateKey = "SomePrivateKey";

      nessiePluginConfig.googleAuthenticationType = AuthMode.SERVICE_ACCOUNT_KEYS;
      nessiePluginConfig.googleClientId = googleClientId;
      nessiePluginConfig.googleClientEmail = googleClientEmail;
      nessiePluginConfig.googlePrivateKeyId = googlePrivateKeyId;
      nessiePluginConfig.googlePrivateKey = () -> googlePrivateKey;

      assertThat(nessiePluginConfig.getProperties())
          .containsAll(
              Arrays.asList(
                  new Property("dremio.gcs.use_keyfile", "true"),
                  new Property("dremio.gcs.clientId", googleClientId),
                  new Property("dremio.gcs.clientEmail", googleClientEmail),
                  new Property("dremio.gcs.privateKeyId", googlePrivateKeyId),
                  new Property("dremio.gcs.privateKey", googlePrivateKey)));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "invalid..bucketName")
    public void testGoogleInvalidRootPath(String googleRootPath) {
      when(optionManager.getOption(DATAPLANE_GCS_STORAGE_ENABLED)).thenReturn(true);
      NessiePluginConfig nessiePluginConfig = setUpForStart(StorageProviderType.GOOGLE);
      nessiePluginConfig.storageProvider = StorageProviderType.GOOGLE;
      nessiePluginConfig.googleRootPath = googleRootPath;

      DataplanePlugin nessieSource = nessiePluginConfig.newPlugin(sabotContext, SOURCE_NAME, null);

      assertThatThrownBy(nessieSource::start)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Invalid GCS root path.");
    }

    @Test
    public void testGoogleMissingStorageAccount() {
      NessiePluginConfig nessiePluginConfig = basicGoogleConfig();
      nessiePluginConfig.googleProjectId = null;

      assertThatThrownBy(nessiePluginConfig::getProperties)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Failure creating a GCS connection.")
          .hasMessageContaining("googleProjectId");
    }

    @Test
    public void testGoogleMissingAuthType() {
      NessiePluginConfig nessiePluginConfig = basicGoogleConfig();
      nessiePluginConfig.googleProjectId = IGNORED;

      nessiePluginConfig.googleAuthenticationType = null;

      assertThatThrownBy(nessiePluginConfig::getProperties)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Failure creating a GCS connection.")
          .hasMessageContaining("googleAuthenticationType");
    }

    private Stream<Arguments> missingServiceAccountKeysConfigArguments() {
      return Stream.of(
          Arguments.of(null, null, null, null),
          Arguments.of(IGNORED, null, null, null),
          Arguments.of(null, IGNORED, null, null),
          Arguments.of(null, null, IGNORED, null),
          Arguments.of(null, null, null, IGNORED),
          Arguments.of(null, IGNORED, IGNORED, IGNORED),
          Arguments.of(IGNORED, null, IGNORED, IGNORED),
          Arguments.of(IGNORED, IGNORED, null, IGNORED),
          Arguments.of(IGNORED, IGNORED, IGNORED, null));
    }

    @ParameterizedTest
    @MethodSource("missingServiceAccountKeysConfigArguments")
    public void testGoogleMissingServiceAccountKeysConfig(
        String googleClientId,
        String googleClientEmail,
        String googlePrivateKeyId,
        String googlePrivateKey) {
      NessiePluginConfig nessiePluginConfig = basicGoogleConfig();
      nessiePluginConfig.googleProjectId = IGNORED;
      nessiePluginConfig.googleAuthenticationType = AuthMode.SERVICE_ACCOUNT_KEYS;

      nessiePluginConfig.googleClientId = googleClientId;
      nessiePluginConfig.googleClientEmail = googleClientEmail;
      nessiePluginConfig.googlePrivateKeyId = googlePrivateKeyId;
      nessiePluginConfig.googlePrivateKey = SecretRef.of(googlePrivateKey);

      assertThatThrownBy(nessiePluginConfig::getProperties)
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Failure creating a GCS connection.")
          .hasMessageContaining("googleClientId")
          .hasMessageContaining("googleClientEmail")
          .hasMessageContaining("googlePrivateKey")
          .hasMessageContaining("googlePrivateKey");
    }
  }

  private static NessiePluginConfig basicNessieConnectionConfig() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = NESSIE_URL;
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    return nessiePluginConfig;
  }

  private static NessiePluginConfig basicAwsConfig() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.storageProvider = StorageProviderType.AWS;
    return nessiePluginConfig;
  }

  private static NessiePluginConfig basicAzureConfig() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.storageProvider = StorageProviderType.AZURE;
    return nessiePluginConfig;
  }

  private static NessiePluginConfig basicGoogleConfig() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.storageProvider = StorageProviderType.GOOGLE;
    return nessiePluginConfig;
  }

  private NessiePluginConfig setUpForStart(StorageProviderType storageProviderType) {
    setUpNessieCacheSettings();
    setUpDataplaneCacheSettings();
    switch (storageProviderType) {
      case AZURE:
        when(optionManager.getOption(DATAPLANE_AZURE_STORAGE_ENABLED)).thenReturn(true);
        break;
      case GOOGLE:
        when(optionManager.getOption(DATAPLANE_GCS_STORAGE_ENABLED)).thenReturn(true);
        break;
      case AWS:
      default:
        when(optionManager.getOption(DATAPLANE_AWS_STORAGE_ENABLED)).thenReturn(true);
        break;
    }
    doReturn(true).when(optionManager).getOption(NESSIE_PLUGIN_ENABLED);

    return basicNessieConnectionConfig();
  }

  private void setUpNessieCacheSettings() {
    doReturn(NESSIE_CONTENT_CACHE_SIZE_ITEMS.getDefault().getNumVal())
        .when(optionManager)
        .getOption(NESSIE_CONTENT_CACHE_SIZE_ITEMS);
    doReturn(NESSIE_CONTENT_CACHE_TTL_MINUTES.getDefault().getNumVal())
        .when(optionManager)
        .getOption(NESSIE_CONTENT_CACHE_TTL_MINUTES);
    doReturn(BYPASS_CONTENT_CACHE.getDefault().getBoolVal())
        .when(optionManager)
        .getOption(BYPASS_CONTENT_CACHE);
    when(sabotContext.getOptionManager()).thenReturn(optionManager);
  }

  private void setUpDataplaneCacheSettings() {
    doReturn(DATAPLANE_ICEBERG_METADATA_CACHE_SIZE_ITEMS.getDefault().getNumVal())
        .when(optionManager)
        .getOption(DATAPLANE_ICEBERG_METADATA_CACHE_SIZE_ITEMS);
    doReturn(DATAPLANE_ICEBERG_METADATA_CACHE_EXPIRE_AFTER_ACCESS_MINUTES.getDefault().getNumVal())
        .when(optionManager)
        .getOption(DATAPLANE_ICEBERG_METADATA_CACHE_EXPIRE_AFTER_ACCESS_MINUTES);
    doReturn(BYPASS_DATAPLANE_CACHE.getDefault().getBoolVal())
        .when(optionManager)
        .getOption(BYPASS_DATAPLANE_CACHE);
    when(sabotContext.getOptionManager()).thenReturn(optionManager);
  }
}
