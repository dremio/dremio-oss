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

import static com.dremio.exec.store.DataplanePluginOptions.NESSIE_PLUGIN_ENABLED;
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
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.NessieConfiguration;

import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.InvalidSpecificationVersionException;
import com.dremio.exec.store.InvalidURLException;
import com.dremio.exec.store.SemanticVersionParserException;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.options.OptionManager;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.UsernameAwareNessieClientImpl;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.users.UserService;

public class TestNessiePluginConfig {

  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private SabotContext sabotContext;

  @Mock
  private UserService userService;

  @Mock
  private OptionManager optionManager;

  @Mock
  private NessieApiV2 nessieApiV2;

  @Mock
  private NessieConfiguration nessieConfiguration;

  @Mock
  private NessieClient nessieClient;

  private static final String SOURCE_NAME = "testNessieSource";

  @Test
  public void testAWSCredentialsProviderWithAccessKey() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.awsAccessKey = "test-access-key";
    nessiePluginConfig.awsAccessSecret = "test-secret-key";
    nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;
    List<Property> awsProviderProperties = new ArrayList<>();

    assertThat(nessiePluginConfig.getAWSCredentialsProvider().configureCredentials(awsProviderProperties)).isEqualTo(ACCESS_KEY_PROVIDER);
  }

  @Test
  public void testAWSCredentialsProviderWithAwsProfile() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    List<Property> awsProviderProperties = new ArrayList<>();
    nessiePluginConfig.credentialType = AWSAuthenticationType.AWS_PROFILE;
    nessiePluginConfig.awsProfile = "test-awsProfile";

    assertThat(nessiePluginConfig.getAWSCredentialsProvider().configureCredentials(awsProviderProperties)).isEqualTo(AWS_PROFILE_PROVIDER);
    Property expectedProperty = new Property("com.dremio.awsProfile", nessiePluginConfig.awsProfile);
    assertThat(awsProviderProperties.get(0).name).isEqualTo(expectedProperty.name);
    assertThat(awsProviderProperties.get(0).value).isEqualTo(expectedProperty.value);
  }

  @Test
  public void testAWSCredentialsProviderWithNoneAuthentication() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    List<Property> awsProviderProperties = new ArrayList<>();
    nessiePluginConfig.credentialType = AWSAuthenticationType.NONE;
    assertThat(nessiePluginConfig.getAWSCredentialsProvider().configureCredentials(awsProviderProperties)).isEqualTo(NONE_PROVIDER);
  }

  @Test
  public void testEmptyAWSAccessKey() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.awsAccessKey = "";
    nessiePluginConfig.awsAccessSecret = "test-secret-key";
    nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;
    List<Property> awsProviderProperties = new ArrayList<>();

    assertThatThrownBy(() -> nessiePluginConfig.getAWSCredentialsProvider().configureCredentials(awsProviderProperties))
      .hasMessageContaining("Failure creating S3 connection. You must provide AWS Access Key and AWS Access Secret.");
  }

  @Test
  public void testAWSCredentialsProviderWithEC2Metadata() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    List<Property> awsProviderProperties = new ArrayList<>();
    nessiePluginConfig.credentialType = AWSAuthenticationType.EC2_METADATA;
    assertThat(nessiePluginConfig.getAWSCredentialsProvider().configureCredentials(awsProviderProperties)).isEqualTo(EC2_METADATA_PROVIDER);
  }

  @Test
  public void testAWSCredentialsProviderWithEC2MetadataAndIAMRole() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    List<Property> awsProviderProperties = new ArrayList<>();
    nessiePluginConfig.assumedRoleARN = "test-assume-role-arn";
    nessiePluginConfig.credentialType = AWSAuthenticationType.EC2_METADATA;
    assertThat(nessiePluginConfig.getAWSCredentialsProvider().configureCredentials(awsProviderProperties)).isEqualTo(ASSUME_ROLE_PROVIDER);
    Property expectedProperty = new Property("fs.s3a.assumed.role.arn", nessiePluginConfig.assumedRoleARN);
    assertThat(awsProviderProperties.get(0).name).isEqualTo(expectedProperty.name);
    assertThat(awsProviderProperties.get(0).value).isEqualTo(expectedProperty.value);
  }

  @Test
  public void testMissingBearerToken() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = "http://localhost:19120/";
    nessiePluginConfig.awsAccessKey = "test-access-key";
    nessiePluginConfig.awsAccessSecret = "test-secret-key";
    nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.nessieAuthType = NessieAuthType.BEARER;

    assertThatThrownBy(() -> nessiePluginConfig.validateNessieAuthSettings("test_nessie_source"))
      .hasMessageContaining("bearer token provided is empty");
  }

  @Test
  public void testEmptyBearerToken() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieAccessToken = "";
    nessiePluginConfig.nessieEndpoint = "http://localhost:19120/";
    nessiePluginConfig.awsAccessKey = "test-access-key";
    nessiePluginConfig.awsAccessSecret = "test-secret-key";
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
    nessiePluginConfig.awsAccessSecret = "test-secret-key";
    nessiePluginConfig.credentialType = AWSAuthenticationType.ACCESS_KEY;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.nessieAuthType = null;

    assertThatThrownBy(() -> nessiePluginConfig.validateNessieAuthSettings("test_nessie_source"))
      .hasMessageContaining("Invalid Nessie Auth type");
  }

  @Test
  public void testEncryptConnectionWithSecureAsFalse() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    Property expectedProperty = new Property(SECURE_CONNECTIONS, "false");
    nessiePluginConfig.secure = false;
    Optional<Property> property = nessiePluginConfig.encryptConnection();

    assertThat(property.get().name).isEqualTo(expectedProperty.name);
    assertThat(property.get().value).isEqualTo(expectedProperty.value);
  }

  @Test
  public void testEncryptConnectionWithSecureAsTrue() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    Property expectedProperty = new Property(SECURE_CONNECTIONS, "true");
    nessiePluginConfig.secure = true;
    Optional<Property> property = nessiePluginConfig.encryptConnection();

    assertThat(property.get().name).isEqualTo(expectedProperty.name);
    assertThat(property.get().value).isEqualTo(expectedProperty.value);
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
    when(sabotContext.getUserService()).thenReturn(userService);

    //Currently NessiePlugin is using the wrapper UsernameAwareNessieClientImpl (on top of NessieClient).
    //This is basically to test the correct NessieClient instance used from NessiePlugin.
    //If there are multiple wrappers used for each service (like one for coordinator and another for executor), then
    //this might break
    assertThat(nessiePluginConfig.getNessieClient("NESSIE_SOURCE", sabotContext)).isInstanceOf(UsernameAwareNessieClientImpl.class);
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

    assertThatThrownBy(() -> nessiePluginConfig.getNessieConfig(nessieApiV2, "NESSIE_SOURCE"))
      .isInstanceOf(InvalidURLException.class)
      .hasMessageContaining("Make sure that Nessie endpoint URL [http://invalid/v0] is valid");
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

    assertThatCode(() ->  nessiePluginConfig.validateNessieSpecificationVersionHelper("2.0.0"))
      .doesNotThrowAnyException();
  }

  @Test
  public void testInvalidLowerNessieSpecificationVersionFor0_58() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();

    assertThatCode(() ->  nessiePluginConfig.validateNessieSpecificationVersionHelper("2.0.0-beta.1"))
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
  public void testInValidRootPathDuringSetup() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = "http://localhost:19120/";

    when(sabotContext.getUserService()).thenReturn(userService);

    assertThatThrownBy(() -> nessiePluginConfig.newPlugin(sabotContext, "NESSIE_SOURCE", null))
      .hasMessageContaining("Invalid AWS Root Path.");
  }

  @Test
  public void testValidAWSRootPath() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    assertThat(nessiePluginConfig.isValidAWSRootPath("bucket-name")).isTrue();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/bucket-name")).isTrue();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/bucket-name/")).isTrue();
    assertThat(nessiePluginConfig.isValidAWSRootPath("bucket-name/folder/path")).isTrue();
    assertThat(nessiePluginConfig.isValidAWSRootPath("bucket-name/folder/path/")).isTrue();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/bucket-name/folder/path")).isTrue();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/bucket-name/folder/path/")).isTrue();
  }

  @Test
  public void testInvalidAWSRootPath() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("//")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/   ")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("  /   ")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("  ")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/s pace/")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/\\\\\"/")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("UPPERCASE")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("$%_INVALID")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/bucket-name/folder/path///")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/bucket-name//folder/path/")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("'/bucket-name/folder/path/")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("\"/bucket-name/folder/path/")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/bucket-n'ame/folder/path/")).isFalse();
    assertThat(nessiePluginConfig.isValidAWSRootPath("/bucket-\"name/folder/path/")).isFalse();
  }

  @Test
  public void testHealthyGetStateCall() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    when(nessieClient.getDefaultBranch()).thenReturn(ResolvedVersionContext.ofBranch(
      "testBranch", "2b3a38be1df114556a019986dcfbfedda593925f"));
    when(nessieClient.getNessieApi()).thenReturn(nessieApiV2);
    when(nessieApiV2.getConfig()).thenReturn(nessieConfiguration);
    when(nessieConfiguration.getSpecVersion()).thenReturn(MINIMUM_NESSIE_SPECIFICATION_VERSION);

    assertThat(nessiePluginConfig.getState(nessieClient, SOURCE_NAME, sabotContext)).isEqualTo(SourceState.GOOD);
  }

  @Test
  public void testUnHealthyGetStateCall() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    when(nessieClient.getDefaultBranch()).thenThrow(new UnAuthenticatedException());

    SourceState sourceState = nessiePluginConfig.getState(nessieClient, SOURCE_NAME, sabotContext);
    assertThat(sourceState.getStatus()).isEqualTo(SourceState.SourceStatus.bad);
    assertThat(sourceState.getSuggestedUserAction()).contains("Make sure that the token is valid and not expired");
  }
}
