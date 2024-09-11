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
import static com.dremio.plugins.dataplane.NessiePluginConfigConstants.MINIMUM_NESSIE_SPECIFICATION_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.InvalidNessieApiVersionException;
import com.dremio.exec.store.InvalidSpecificationVersionException;
import com.dremio.exec.store.InvalidURLException;
import com.dremio.exec.store.SemanticVersionParserException;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.nessiemetadata.cache.NessieDataplaneCacheProvider;
import com.dremio.nessiemetadata.cache.NessieDataplaneCaffeineCacheProvider;
import com.dremio.options.OptionManager;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.dataplane.store.AbstractDataplanePluginConfig.StorageProviderType;
import com.dremio.service.namespace.SourceState;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.NessieApiCompatibilityException;
import org.projectnessie.model.NessieConfiguration;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TestNessiePlugin {
  @Mock private NessieConfiguration nessieConfiguration;
  @Mock private NessieClient nessieClient;
  @Mock private SabotContext sabotContext;
  @Mock private OptionManager optionManager;
  @Mock private NessieApiV2 nessieApiV2;
  @Mock private StoragePluginId storagePluginId;
  @Mock private NessiePluginConfig nessiePluginConfig;
  private final NessieDataplaneCacheProvider cacheProvider =
      new NessieDataplaneCaffeineCacheProvider();
  private NessiePlugin nessiePlugin;
  private static final String SOURCE_NAME = "testNessieSource";

  @BeforeEach
  public void setup() {
    when(sabotContext.getOptionManager()).thenReturn(optionManager);
    doReturn(DATAPLANE_ICEBERG_METADATA_CACHE_EXPIRE_AFTER_ACCESS_MINUTES.getDefault().getNumVal())
        .when(optionManager)
        .getOption(DATAPLANE_ICEBERG_METADATA_CACHE_EXPIRE_AFTER_ACCESS_MINUTES);
    doReturn(DATAPLANE_ICEBERG_METADATA_CACHE_SIZE_ITEMS.getDefault().getNumVal())
        .when(optionManager)
        .getOption(DATAPLANE_ICEBERG_METADATA_CACHE_SIZE_ITEMS);
    doReturn(BYPASS_DATAPLANE_CACHE.getDefault().getBoolVal())
        .when(optionManager)
        .getOption(BYPASS_DATAPLANE_CACHE);

    nessiePlugin =
        new NessiePlugin(
            nessiePluginConfig,
            sabotContext,
            SOURCE_NAME,
            () -> storagePluginId,
            nessieClient,
            cacheProvider,
            null);
  }

  @Test
  public void testMissingBearerToken() {
    NessieAuthType nessieAuthType = NessieAuthType.BEARER;
    SecretRef nessieAccessToken = SecretRef.of("");
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieAccessToken = nessieAccessToken;
    nessiePluginConfig.nessieAuthType = nessieAuthType;
    assertThatThrownBy(
            () -> nessiePlugin.validateNessieAuthSettings(SOURCE_NAME, nessiePluginConfig))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("bearer token provided is empty");
  }

  @Test
  public void testInvalidNessieAuthType() {
    NessieAuthType nessieAuthType = null;
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieAuthType = nessieAuthType;
    assertThatThrownBy(
            () -> nessiePlugin.validateNessieAuthSettings(SOURCE_NAME, nessiePluginConfig))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid Nessie Auth type");
  }

  @Test
  public void testHealthyGetStateCall() {
    setUpGetState();
    when(nessieClient.getDefaultBranch())
        .thenReturn(
            ResolvedVersionContext.ofBranch(
                "testBranch", "2b3a38be1df114556a019986dcfbfedda593925f"));
    when(nessieClient.getNessieApi()).thenReturn(nessieApiV2);
    when(nessieApiV2.getConfig()).thenReturn(nessieConfiguration);
    when(nessieConfiguration.getSpecVersion()).thenReturn(MINIMUM_NESSIE_SPECIFICATION_VERSION);

    assertThat(nessiePlugin.getState(nessieClient, SOURCE_NAME, sabotContext))
        .isEqualTo(SourceState.GOOD);
  }

  @Test
  public void testUnHealthyGetStateCall() {
    setUpGetState();
    when(nessieClient.getDefaultBranch()).thenThrow(new UnAuthenticatedException());

    SourceState sourceState = nessiePlugin.getState(nessieClient, SOURCE_NAME, sabotContext);
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
    setUpGetState();
    when(nessieClient.getNessieApi()).thenReturn(nessieApiV2);
    when(nessieApiV2.getConfig()).thenThrow(NessieApiCompatibilityException.class);

    SourceState sourceState = nessiePlugin.getState(nessieClient, SOURCE_NAME, sabotContext);
    assertThat(sourceState.getStatus()).isEqualTo(SourceState.SourceStatus.bad);
    assertThat(sourceState.getMessages())
        .contains(
            new SourceState.Message(
                SourceState.MessageLevel.ERROR,
                (String.format("Could not connect to [%s].", SOURCE_NAME))));
    assertThat(sourceState.getSuggestedUserAction()).contains("Invalid API version.");
  }

  @Test
  public void testValidateNessieAuthSettingsCallDuringPluginStart() {
    nessiePlugin = spy(nessiePlugin);
    // Act
    try {
      nessiePlugin.start();
    } catch (Exception e) {
      // ignoring this exception as this happened due to super.start() which needs extra config
      // probably
      // This call is to verify if testValidateNessieAuthSettingsCallDuringPluginStart gets called
    }

    // Assert
    verify(nessiePlugin).validateNessieAuthSettings(SOURCE_NAME, nessiePluginConfig);
  }

  private void setUpGetState() {
    when(nessiePluginConfig.getStorageProvider()).thenReturn(StorageProviderType.AWS);
    doReturn(true).when(optionManager).getOption(DATAPLANE_AWS_STORAGE_ENABLED);
    when(sabotContext.getOptionManager()).thenReturn(optionManager);
  }

  @Nested
  class NessieConfig {
    @Test
    public void testInvalidNessieSpecificationVersion() {
      assertThatThrownBy(() -> nessiePlugin.validateNessieSpecificationVersionHelper("x.y.z"))
          .isInstanceOf(SemanticVersionParserException.class)
          .hasMessageContaining("Cannot parse Nessie specification version");
    }

    @Test
    public void testInvalidNessieEndpointURL() {
      when(nessiePluginConfig.getNessieEndpoint()).thenReturn("http://invalid/v0");
      when(nessieApiV2.getConfig()).thenThrow(IllegalArgumentException.class);

      assertThatThrownBy(() -> nessiePlugin.getNessieConfig(nessieApiV2))
          .isInstanceOf(InvalidURLException.class)
          .hasMessageContaining("Make sure that Nessie endpoint URL [http://invalid/v0] is valid");
    }

    @Test
    public void testIncompatibleNessieApiInEndpointURL() {
      when(nessiePluginConfig.getNessieEndpoint()).thenReturn("http://invalid/v0");
      when(nessieApiV2.getConfig()).thenThrow(NessieApiCompatibilityException.class);

      assertThatThrownBy(() -> nessiePlugin.getNessieConfig(nessieApiV2))
          .isInstanceOf(InvalidNessieApiVersionException.class)
          .hasMessageContaining("Invalid API version.");
    }

    @Test
    public void testInvalidLowerNessieSpecificationVersion() {
      assertThatThrownBy(() -> nessiePlugin.validateNessieSpecificationVersionHelper("1.0.0"))
          .isInstanceOf(InvalidSpecificationVersionException.class)
          .hasMessageContaining("Nessie Server should comply with Nessie specification version");
    }

    @Test
    public void testValidEquivalentNessieSpecificationVersion() {
      assertDoesNotThrow(() -> nessiePlugin.validateNessieSpecificationVersionHelper("2.0.0"));
    }

    @Test
    public void testValidGreaterNessieSpecificationVersion() {
      assertDoesNotThrow(() -> nessiePlugin.validateNessieSpecificationVersionHelper("2.0.1"));
    }

    @Test
    public void testInvalidLowerNessieSpecificationVersionFor0_58() {
      assertThatThrownBy(
              () -> nessiePlugin.validateNessieSpecificationVersionHelper("2.0.0-beta.1"))
          .isInstanceOf(InvalidSpecificationVersionException.class)
          .hasMessageContaining("Nessie Server should comply with Nessie specification version");
    }

    @Test
    public void testInvalidNullNessieSpecificationVersion() {
      assertThatThrownBy(() -> nessiePlugin.validateNessieSpecificationVersionHelper(null))
          .isInstanceOf(InvalidSpecificationVersionException.class)
          .hasMessageContaining("Nessie Server should comply with Nessie specification version")
          .hasMessageContaining("Also make sure that Nessie endpoint URL is valid.");
    }
  }

  @Nested
  class Flags {
    @Test
    public void testValidatePluginEnabled() {
      when(sabotContext.getOptionManager()).thenReturn(optionManager);
      when(optionManager.getOption(NESSIE_PLUGIN_ENABLED)).thenReturn(true);

      assertDoesNotThrow(() -> nessiePlugin.validatePluginEnabled(sabotContext));
    }

    @Test
    public void testValidatePluginDisabled() {
      when(sabotContext.getOptionManager()).thenReturn(optionManager);
      when(optionManager.getOption(NESSIE_PLUGIN_ENABLED)).thenReturn(false);

      assertThatThrownBy(() -> nessiePlugin.validatePluginEnabled(sabotContext))
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Nessie Source is not supported");
    }

    @Test
    public void testAwsStorageProviderKeyEnabled() {
      when(nessiePluginConfig.getStorageProvider()).thenReturn(StorageProviderType.AWS);
      when(optionManager.getOption(DATAPLANE_AWS_STORAGE_ENABLED)).thenReturn(true);

      assertDoesNotThrow(() -> nessiePlugin.validateStorageProviderTypeEnabled(optionManager));
    }

    @Test
    public void testAwsStorageProviderKeyDisabled() {
      when(nessiePluginConfig.getStorageProvider()).thenReturn(StorageProviderType.AWS);
      when(optionManager.getOption(DATAPLANE_AWS_STORAGE_ENABLED)).thenReturn(false);

      assertThatThrownBy(() -> nessiePlugin.validateStorageProviderTypeEnabled(optionManager))
          .isInstanceOf(UserException.class)
          .hasMessageContaining("AWS storage provider type is not supported");
    }

    @Test
    public void testAzureStorageProviderKeyEnabled() {
      when(nessiePluginConfig.getStorageProvider()).thenReturn(StorageProviderType.AZURE);
      when(optionManager.getOption(DATAPLANE_AZURE_STORAGE_ENABLED)).thenReturn(true);

      assertDoesNotThrow(() -> nessiePlugin.validateStorageProviderTypeEnabled(optionManager));
    }

    @Test
    public void testAzureStorageProviderKeyDisabled() {
      when(nessiePluginConfig.getStorageProvider()).thenReturn(StorageProviderType.AZURE);
      when(optionManager.getOption(DATAPLANE_AZURE_STORAGE_ENABLED)).thenReturn(false);

      assertThatThrownBy(() -> nessiePlugin.validateStorageProviderTypeEnabled(optionManager))
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Azure storage provider type is not supported");
    }

    @Test
    public void testOtherStorageProviderKeyDisabledButAwsEnabled() {
      when(nessiePluginConfig.getStorageProvider()).thenReturn(StorageProviderType.AWS);
      when(optionManager.getOption(DATAPLANE_AWS_STORAGE_ENABLED)).thenReturn(true);
      // Currently the code short circuits this call, leaving this in as lenient in case the order
      // of operations changes
      Mockito.lenient()
          .when(optionManager.getOption(DATAPLANE_AZURE_STORAGE_ENABLED))
          .thenReturn(false);
      // Currently the code short circuits this call, leaving this in as lenient in case the order
      // of operations changes
      Mockito.lenient()
          .when(optionManager.getOption(DATAPLANE_GCS_STORAGE_ENABLED))
          .thenReturn(false);

      assertDoesNotThrow(() -> nessiePlugin.validateStorageProviderTypeEnabled(optionManager));
    }

    @Test
    public void testGcsStorageProviderKeyEnabled() {
      when(nessiePluginConfig.getStorageProvider()).thenReturn(StorageProviderType.GOOGLE);
      when(optionManager.getOption(DATAPLANE_GCS_STORAGE_ENABLED)).thenReturn(true);

      assertDoesNotThrow(() -> nessiePlugin.validateStorageProviderTypeEnabled(optionManager));
    }

    @Test
    public void testGcsStorageProviderKeyDisabled() {
      when(nessiePluginConfig.getStorageProvider()).thenReturn(StorageProviderType.GOOGLE);
      when(optionManager.getOption(DATAPLANE_GCS_STORAGE_ENABLED)).thenReturn(false);

      assertThatThrownBy(() -> nessiePlugin.validateStorageProviderTypeEnabled(optionManager))
          .isInstanceOf(UserException.class)
          .hasMessageContaining("Google storage provider type is not supported");
    }
  }

  @Test
  public void testMissingOauthURI() {
    NessieAuthType nessieAuthType = NessieAuthType.OAUTH2;
    SecretRef clientSecret = SecretRef.of("");
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.oauth2ClientId = "testClientId";
    nessiePluginConfig.oauth2ClientSecret = clientSecret;
    nessiePluginConfig.oauth2TokenEndpointURI = null;
    nessiePluginConfig.nessieAuthType = nessieAuthType;
    assertThatThrownBy(
            () -> nessiePlugin.validateNessieAuthSettings(SOURCE_NAME, nessiePluginConfig))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("OAuth2 URI must not be null");
  }

  @Test
  public void testMissingClientId() {
    NessieAuthType nessieAuthType = NessieAuthType.OAUTH2;
    SecretRef clientSecret = SecretRef.of("");

    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.oauth2ClientId = null;
    nessiePluginConfig.oauth2ClientSecret = clientSecret;
    nessiePluginConfig.oauth2TokenEndpointURI = String.valueOf(URI.create("http://test.com"));
    nessiePluginConfig.nessieAuthType = nessieAuthType;
    assertThatThrownBy(
            () -> nessiePlugin.validateNessieAuthSettings(SOURCE_NAME, nessiePluginConfig))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("OAuth2 ClientId must not be null");
  }

  @Test
  public void testMissingClientSecret() {
    NessieAuthType nessieAuthType = NessieAuthType.OAUTH2;
    SecretRef clientSecret = SecretRef.of("");

    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.oauth2ClientId = "testClientId";
    nessiePluginConfig.oauth2ClientSecret = null;
    nessiePluginConfig.oauth2TokenEndpointURI = String.valueOf(URI.create("http://test.com"));
    nessiePluginConfig.nessieAuthType = nessieAuthType;
    assertThatThrownBy(
            () -> nessiePlugin.validateNessieAuthSettings(SOURCE_NAME, nessiePluginConfig))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("OAuth2 ClientSecret must not be null");
  }
}
