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

import static com.dremio.exec.ExecConstants.VERSIONED_SOURCE_CAPABILITIES_USE_NATIVE_PRIVILEGES_ENABLED;
import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_AZURE_STORAGE_ENABLED;
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
import static com.dremio.plugins.dataplane.NessiePluginConfigConstants.MINIMUM_NESSIE_SPECIFICATION_VERSION;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.InvalidNessieApiVersionException;
import com.dremio.exec.store.InvalidSpecificationVersionException;
import com.dremio.exec.store.InvalidURLException;
import com.dremio.exec.store.SemanticVersionParserException;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.options.OptionManager;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.UsernameAwareNessieClientImpl;
import com.dremio.plugins.azure.AzureAuthenticationType;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.users.UserService;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.NessieApiCompatibilityException;
import org.projectnessie.model.NessieConfiguration;

public class TestNessiePluginConfig {

  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private SabotContext sabotContext;

  @Mock private UserService userService;

  @Mock private OptionManager optionManager;

  @Mock private NessieApiV2 nessieApiV2;

  @Mock private NessieConfiguration nessieConfiguration;

  @Mock private NessieClient nessieClient;

  private static final String SOURCE_NAME = "testNessieSource";

  @Test
  public void testAWSCredentialsProviderWithAccessKey() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.awsAccessKey = "test-access-key";
    nessiePluginConfig.awsAccessSecret = () -> "test-access-key";
    nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;
    List<Property> awsProviderProperties = new ArrayList<>();

    assertThat(
            nessiePluginConfig
                .getAWSCredentialsProvider()
                .configureCredentials(awsProviderProperties))
        .isEqualTo(ACCESS_KEY_PROVIDER);
  }

  @Test
  public void testAWSCredentialsProviderWithAwsProfile() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    List<Property> awsProviderProperties = new ArrayList<>();
    nessiePluginConfig.credentialType = AWSAuthenticationType.AWS_PROFILE;
    nessiePluginConfig.awsProfile = "test-awsProfile";

    assertThat(
            nessiePluginConfig
                .getAWSCredentialsProvider()
                .configureCredentials(awsProviderProperties))
        .isEqualTo(AWS_PROFILE_PROVIDER);
    Property expectedProperty =
        new Property("com.dremio.awsProfile", nessiePluginConfig.awsProfile);
    assertThat(awsProviderProperties.get(0).name).isEqualTo(expectedProperty.name);
    assertThat(awsProviderProperties.get(0).value).isEqualTo(expectedProperty.value);
  }

  @Test
  public void testAWSCredentialsProviderWithNoneAuthentication() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    List<Property> awsProviderProperties = new ArrayList<>();
    nessiePluginConfig.credentialType = AWSAuthenticationType.NONE;
    assertThat(
            nessiePluginConfig
                .getAWSCredentialsProvider()
                .configureCredentials(awsProviderProperties))
        .isEqualTo(NONE_PROVIDER);
  }

  @Test
  public void testEmptyAWSAccessKey() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.awsAccessKey = "";
    nessiePluginConfig.awsAccessSecret = () -> "test-secret-key";
    nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;
    List<Property> awsProviderProperties = new ArrayList<>();

    assertThatThrownBy(
            () ->
                nessiePluginConfig
                    .getAWSCredentialsProvider()
                    .configureCredentials(awsProviderProperties))
        .hasMessageContaining(
            "Failure creating S3 connection. You must provide AWS Access Key and AWS Access Secret.");
  }

  @Test
  public void testAWSCredentialsProviderWithEC2Metadata() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    List<Property> awsProviderProperties = new ArrayList<>();
    nessiePluginConfig.credentialType = AWSAuthenticationType.EC2_METADATA;
    assertThat(
            nessiePluginConfig
                .getAWSCredentialsProvider()
                .configureCredentials(awsProviderProperties))
        .isEqualTo(EC2_METADATA_PROVIDER);
  }

  @Test
  public void testAWSCredentialsProviderWithEC2MetadataAndIAMRole() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    List<Property> awsProviderProperties = new ArrayList<>();
    nessiePluginConfig.assumedRoleARN = "test-assume-role-arn";
    nessiePluginConfig.credentialType = AWSAuthenticationType.EC2_METADATA;
    assertThat(
            nessiePluginConfig
                .getAWSCredentialsProvider()
                .configureCredentials(awsProviderProperties))
        .isEqualTo(ASSUME_ROLE_PROVIDER);
    Property expectedProperty =
        new Property("fs.s3a.assumed.role.arn", nessiePluginConfig.assumedRoleARN);
    assertThat(awsProviderProperties.get(0).name).isEqualTo(expectedProperty.name);
    assertThat(awsProviderProperties.get(0).value).isEqualTo(expectedProperty.value);
  }

  @Test
  public void testMissingBearerToken() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = "http://localhost:19120/";
    nessiePluginConfig.awsAccessKey = "test-access-key";
    nessiePluginConfig.awsAccessSecret = () -> "test-secret-key";
    nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.nessieAuthType = NessieAuthType.BEARER;

    assertThatThrownBy(() -> nessiePluginConfig.validateNessieAuthSettings("test_nessie_source"))
        .hasMessageContaining("bearer token provided is empty");
  }

  @Test
  public void testEmptyBearerToken() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieAccessToken = SecretRef.empty();
    nessiePluginConfig.nessieEndpoint = "http://localhost:19120/";
    nessiePluginConfig.awsAccessKey = "test-access-key";
    nessiePluginConfig.awsAccessSecret = () -> "test-secret-key";
    nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.nessieAuthType = NessieAuthType.BEARER;

    assertThatThrownBy(() -> nessiePluginConfig.validateNessieAuthSettings("test_nessie_source"))
        .hasMessageContaining("bearer token provided is empty");
  }

  @Test
  public void testInvalidNessieAuthType() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = "http://localhost:19120/";
    nessiePluginConfig.awsAccessKey = "test-access-key";
    nessiePluginConfig.awsAccessSecret = () -> "test-secret-key";
    nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.nessieAuthType = null;

    assertThatThrownBy(() -> nessiePluginConfig.validateNessieAuthSettings("test_nessie_source"))
        .hasMessageContaining("Invalid Nessie Auth type");
  }

  @Test
  public void testEncryptConnectionWithSecureAsFalse() {
    // Arrange
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.secure = false;
    nessiePluginConfig.credentialType = AWSAuthenticationType.NONE; // To bypass unrelated checks

    Property expectedProperty = new Property(SECURE_CONNECTIONS, "false");

    // Act
    List<Property> properties = nessiePluginConfig.getProperties();

    // Assert
    assertThat(properties).contains(expectedProperty);
  }

  @Test
  public void testEncryptConnectionWithSecureAsTrue() {
    // Arrange
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.secure = true;
    nessiePluginConfig.credentialType = AWSAuthenticationType.NONE; // To bypass unrelated checks

    Property expectedProperty = new Property(SECURE_CONNECTIONS, "true");

    // Act
    List<Property> properties = nessiePluginConfig.getProperties();

    assertThat(properties).contains(expectedProperty);
  }

  @Test
  public void testValidatePluginEnabled() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    when(sabotContext.getOptionManager()).thenReturn(optionManager);
    when(optionManager.getOption(NESSIE_PLUGIN_ENABLED)).thenReturn(false);

    assertThatThrownBy(() -> nessiePluginConfig.validatePluginEnabled(sabotContext))
        .hasMessageContaining("Nessie Source is not supported");
  }

  @Test
  public void testGetNessieClient() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = "http://test-nessie";

    setUpNessieCacheSettings();
    when(sabotContext.getUserService()).thenReturn(userService);

    // Currently NessiePlugin is using the wrapper UsernameAwareNessieClientImpl (on top of
    // NessieClient).
    // This is basically to test the correct NessieClient instance used from NessiePlugin.
    // If there are multiple wrappers used for each service (like one for coordinator and another
    // for executor), then
    // this might break
    assertThat(nessiePluginConfig.getNessieClient("NESSIE_SOURCE", sabotContext))
        .isInstanceOf(UsernameAwareNessieClientImpl.class);
  }

  @Test
  public void testGetNessieClientThrowsError() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = "invalid://test-nessie";

    assertThatThrownBy(() -> nessiePluginConfig.getNessieClient("NESSIE_SOURCE", sabotContext))
        .hasMessageContaining("must be a valid http or https address");
  }

  @Test
  public void testInvalidNessieSpecificationVersion() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();

    assertThatThrownBy(() -> nessiePluginConfig.validateNessieSpecificationVersionHelper("x.y.z"))
        .isInstanceOf(SemanticVersionParserException.class)
        .hasMessageContaining("Cannot parse Nessie specification version");
  }

  @Test
  public void testInvalidNessieEndpointURL() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = "http://invalid/v0";
    when(nessieApiV2.getConfig()).thenThrow(IllegalArgumentException.class);

    assertThatThrownBy(() -> nessiePluginConfig.getNessieConfig(nessieApiV2))
        .isInstanceOf(InvalidURLException.class)
        .hasMessageContaining("Make sure that Nessie endpoint URL [http://invalid/v0] is valid");
  }

  @Test
  public void testIncompatibleNessieApiInEndpointURL() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = "http://invalid/v0";
    when(nessieApiV2.getConfig()).thenThrow(NessieApiCompatibilityException.class);

    assertThatThrownBy(() -> nessiePluginConfig.getNessieConfig(nessieApiV2))
        .isInstanceOf(InvalidNessieApiVersionException.class)
        .hasMessageContaining("Invalid API version.");
  }

  @Test
  public void testInvalidLowerNessieSpecificationVersion() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();

    assertThatThrownBy(() -> nessiePluginConfig.validateNessieSpecificationVersionHelper("1.0.0"))
        .isInstanceOf(InvalidSpecificationVersionException.class)
        .hasMessageContaining("Nessie Server should comply with Nessie specification version");
  }

  @Test
  public void testValidEquivalentNessieSpecificationVersion() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();

    assertThatCode(() -> nessiePluginConfig.validateNessieSpecificationVersionHelper("2.0.0"))
        .doesNotThrowAnyException();
  }

  @Test
  public void testValidGreaterNessieSpecificationVersion() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();

    assertThatCode(() -> nessiePluginConfig.validateNessieSpecificationVersionHelper("2.0.1"))
        .doesNotThrowAnyException();
  }

  @Test
  public void testInvalidLowerNessieSpecificationVersionFor0_58() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();

    assertThatCode(
            () -> nessiePluginConfig.validateNessieSpecificationVersionHelper("2.0.0-beta.1"))
        .isInstanceOf(InvalidSpecificationVersionException.class)
        .hasMessageContaining("Nessie Server should comply with Nessie specification version");
  }

  @Test
  public void testInvalidNullNessieSpecificationVersion() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();

    assertThatThrownBy(() -> nessiePluginConfig.validateNessieSpecificationVersionHelper(null))
        .isInstanceOf(InvalidSpecificationVersionException.class)
        .hasMessageContaining("Nessie Server should comply with Nessie specification version")
        .hasMessageContaining("Also make sure that Nessie endpoint URL is valid.");
  }

  @Test
  public void testInValidRootPathDuringStart() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = "http://localhost:19120/"; // Unused, but necessary
    nessiePluginConfig.nessieAccessToken = () -> "sometoken"; // Unused, but necessary
    nessiePluginConfig.awsRootPath = "invalid..bucketName";

    setUpNessieCacheSettings();
    setUpDataplaneCacheSettings();
    doReturn(true).when(optionManager).getOption(NESSIE_PLUGIN_ENABLED);
    when(sabotContext.getUserService()).thenReturn(userService);

    DataplanePlugin nessieSource =
        nessiePluginConfig.newPlugin(sabotContext, "NESSIE_SOURCE", null);

    assertThatThrownBy(nessieSource::start)
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid AWS S3 root path.");
  }

  @Test
  public void testEmptyRootPathDuringStart() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = "http://localhost:19120/"; // Unused, but necessary
    nessiePluginConfig.nessieAccessToken = () -> "sometoken"; // Unused, but necessary
    nessiePluginConfig.awsRootPath = "";

    setUpNessieCacheSettings();
    setUpDataplaneCacheSettings();
    doReturn(true).when(optionManager).getOption(NESSIE_PLUGIN_ENABLED);
    when(sabotContext.getUserService()).thenReturn(userService);

    DataplanePlugin nessieSource =
        nessiePluginConfig.newPlugin(sabotContext, "NESSIE_SOURCE", null);

    assertThatThrownBy(nessieSource::start)
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid AWS S3 root path.");
  }

  @Test
  public void testHealthyGetStateCall() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    when(nessieClient.getDefaultBranch())
        .thenReturn(
            ResolvedVersionContext.ofBranch(
                "testBranch", "2b3a38be1df114556a019986dcfbfedda593925f"));
    when(nessieClient.getNessieApi()).thenReturn(nessieApiV2);
    when(nessieApiV2.getConfig()).thenReturn(nessieConfiguration);
    when(nessieConfiguration.getSpecVersion()).thenReturn(MINIMUM_NESSIE_SPECIFICATION_VERSION);

    assertThat(nessiePluginConfig.getState(nessieClient, SOURCE_NAME, sabotContext))
        .isEqualTo(SourceState.GOOD);
  }

  @Test
  public void testUnHealthyGetStateCall() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    when(nessieClient.getDefaultBranch()).thenThrow(new UnAuthenticatedException());

    SourceState sourceState = nessiePluginConfig.getState(nessieClient, SOURCE_NAME, sabotContext);
    assertThat(sourceState.getStatus()).isEqualTo(SourceState.SourceStatus.bad);
    assertThat(sourceState.getMessages())
        .contains(
            new SourceState.Message(
                SourceState.MessageLevel.ERROR,
                (String.format(
                    "Could not connect to [%s]. Unable to authenticate to the Nessie server.",
                    SOURCE_NAME))));
    assertThat(sourceState.getSuggestedUserAction())
        .contains("Make sure that the token is valid and not expired");
  }

  @Test
  public void testUnHealthyGetStateCallForNessieApiInCompatibility() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    when(nessieClient.getNessieApi()).thenReturn(nessieApiV2);
    when(nessieApiV2.getConfig()).thenThrow(NessieApiCompatibilityException.class);

    SourceState sourceState = nessiePluginConfig.getState(nessieClient, SOURCE_NAME, sabotContext);
    assertThat(sourceState.getStatus()).isEqualTo(SourceState.SourceStatus.bad);
    assertThat(sourceState.getMessages())
        .contains(
            new SourceState.Message(
                SourceState.MessageLevel.ERROR,
                (String.format("Could not connect to [%s].", SOURCE_NAME))));
    assertThat(sourceState.getSuggestedUserAction()).contains("Invalid API version.");
  }

  @Test
  public void testNessiePluginConfigUseNativePrivilegesWhenVersionedRbacEntityDisabled() {
    // Intentionally disabling UnnecessaryStubbing since we want to make sure even when
    // VERSIONED_RBAC_ENTITY_ENABLED
    // is enabled, it does not impact NessiePluginConfig.
    Mockito.lenient()
        .when(optionManager.getOption(VERSIONED_SOURCE_CAPABILITIES_USE_NATIVE_PRIVILEGES_ENABLED))
        .thenReturn(false);

    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    assertThat(nessiePluginConfig.useNativePrivileges(optionManager)).isFalse();
  }

  @Test
  public void testNessiePluginConfigUseNativePrivilegesWhenVersionedRbacEntityEnabled() {
    // Intentionally disabling UnnecessaryStubbing since we want to make sure even when
    // VERSIONED_RBAC_ENTITY_ENABLED
    // is enabled, it does not impact NessiePluginConfig.
    Mockito.lenient()
        .when(optionManager.getOption(VERSIONED_SOURCE_CAPABILITIES_USE_NATIVE_PRIVILEGES_ENABLED))
        .thenReturn(true);

    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    assertThat(nessiePluginConfig.useNativePrivileges(optionManager)).isFalse();
  }

  @Test
  public void testAzureStorageProviderKeyDisabled() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.storageProvider = AbstractDataplanePluginConfig.StorageProviderType.AZURE;
    when(optionManager.getOption(DATAPLANE_AZURE_STORAGE_ENABLED)).thenReturn(false);

    assertThatThrownBy(() -> nessiePluginConfig.validateAzureStorageProviderEnabled(optionManager))
        .hasMessageContaining("Azure storage provider type is not supported");
  }

  @Test
  public void testAzureStorageProviderKeyDisabledButAws() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.storageProvider = AbstractDataplanePluginConfig.StorageProviderType.AWS;
    // Currently the code short circuits this call, leaving this in as lenient in case the order of
    // operations changes
    Mockito.lenient()
        .when(optionManager.getOption(DATAPLANE_AZURE_STORAGE_ENABLED))
        .thenReturn(false);

    assertThatCode(() -> nessiePluginConfig.validateAzureStorageProviderEnabled(optionManager))
        .doesNotThrowAnyException();
  }

  @Test
  public void testAzureStorageProviderKeyEnabled() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.storageProvider = AbstractDataplanePluginConfig.StorageProviderType.AZURE;
    when(optionManager.getOption(DATAPLANE_AZURE_STORAGE_ENABLED)).thenReturn(true);

    assertThatCode(() -> nessiePluginConfig.validateAzureStorageProviderEnabled(optionManager))
        .doesNotThrowAnyException();
  }

  @Test
  public void testAzureFilesystemConfig() {
    final String azureStorageAccount = "SomeStorageAccount";
    final String azureRootPath = "SomeRootPath";

    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.storageProvider = AbstractDataplanePluginConfig.StorageProviderType.AZURE;
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
    final String azureAccessKey = "SomeAccessKey";

    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.storageProvider = AbstractDataplanePluginConfig.StorageProviderType.AZURE;
    nessiePluginConfig.azureAuthenticationType = AzureAuthenticationType.ACCESS_KEY;
    nessiePluginConfig.azureAccessKey = () -> azureAccessKey;

    assertThat(nessiePluginConfig.getProperties())
        .containsAll(
            Arrays.asList(
                new Property("dremio.azure.credentialsType", "ACCESS_KEY"),
                new Property("dremio.azure.key", "dremio+" + azureAccessKey),
                new Property(
                    "fs.azure.sharedkey.signer.type",
                    "org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials")));
  }

  @Test
  public void testAzureAadConfig() {
    final String azureApplicationId = "SomeApplicationId";
    final String azureClientSecret = "SomeClientSecret";
    final String azureOAuthTokenEndpoint = "SomeOAuthTokenEndpoint";

    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.storageProvider = AbstractDataplanePluginConfig.StorageProviderType.AZURE;
    nessiePluginConfig.azureAuthenticationType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;
    nessiePluginConfig.azureApplicationId = azureApplicationId;
    nessiePluginConfig.azureClientSecret = () -> azureClientSecret;
    nessiePluginConfig.azureOAuthTokenEndpoint = azureOAuthTokenEndpoint;

    assertThat(nessiePluginConfig.getProperties())
        .containsAll(
            Arrays.asList(
                new Property("dremio.azure.credentialsType", "AZURE_ACTIVE_DIRECTORY"),
                new Property("dremio.azure.clientId", azureApplicationId),
                new Property("dremio.azure.clientSecret", "dremio+" + azureClientSecret),
                new Property("dremio.azure.tokenEndpoint", azureOAuthTokenEndpoint)));
  }

  @Test
  public void testNessiePluginConfigIncludesS3APerfOptimizations() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.credentialType = AWSAuthenticationType.NONE;

    List<Property> expected =
        ImmutableList.of(
            new Property(Constants.CREATE_FILE_STATUS_CHECK, "false"),
            new Property(
                Constants.DIRECTORY_MARKER_POLICY, Constants.DIRECTORY_MARKER_POLICY_KEEP));
    assertThat(nessiePluginConfig.getProperties()).containsAll(expected);
  }

  @Test
  public void testNessiePluginConfigCanOverrideS3APerfOptimizations() {
    List<Property> overrides =
        ImmutableList.of(
            new Property(Constants.CREATE_FILE_STATUS_CHECK, "true"),
            new Property(
                Constants.DIRECTORY_MARKER_POLICY, Constants.DIRECTORY_MARKER_POLICY_DELETE));
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.credentialType = AWSAuthenticationType.NONE;
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
