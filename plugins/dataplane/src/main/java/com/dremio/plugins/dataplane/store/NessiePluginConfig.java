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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.nessiemetadata.cache.NessieDataplaneCaffeineCacheProvider;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClientImpl;
import com.dremio.plugins.UsernameAwareNessieClientImpl;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;
import java.net.URI;
import java.util.List;
import javax.inject.Provider;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticationProvider;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticatorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Connection configuration for Nessie source Plugin. */
@SourceType(value = "NESSIE", label = "Nessie", uiConfig = "nessie-layout.json")
public class NessiePluginConfig extends AbstractDataplanePluginConfig {
  private static final Logger logger = LoggerFactory.getLogger(NessiePluginConfig.class);

  @Tag(1)
  @DisplayMetadata(label = "Nessie endpoint URL")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String nessieEndpoint;

  @Tag(2)
  @Secret
  @DisplayMetadata(label = "Bearer token")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public SecretRef nessieAccessToken;

  // Tags 3 through 7 are defined in AbstractDataplanePluginConfig

  @Tag(8)
  @DisplayMetadata(label = "Authentication method")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public AWSAuthenticationType credentialType;

  // Tags 9 through 12 are defined in AbstractDataplanePluginConfig

  // Tag 13 is reserved

  @Tag(14)
  @DisplayMetadata(label = "Nessie authentication type")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public NessieAuthType nessieAuthType = NessieAuthType.BEARER;

  @Tag(15)
  @DisplayMetadata(label = "AWS profile")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String awsProfile;

  @Tag(16)
  @DisplayMetadata(label = "Encrypt connection")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public boolean secure = true;

  // Tags 17 through 31 are defined in AbstractDataplanePluginConfig
  // Tags 32 through 36 are defined in AbstractBagpipePluginConfig
  @Tag(37)
  @DisplayMetadata(label = "Client ID")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String oauth2ClientId;

  @Tag(38)
  @Secret
  @DisplayMetadata(label = "Client Secret")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public SecretRef oauth2ClientSecret;

  @Tag(39)
  @DisplayMetadata(label = "OAuth2 Token Endpoint")
  @NotMetadataImpacting
  public String oauth2TokenEndpointURI;

  @Override
  public DataplanePlugin newPlugin(
      SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    final NessieClient nessieClient = getNessieClient(name, context);

    return new NessiePlugin(
        this,
        context,
        name,
        pluginIdProvider,
        nessieClient,
        new NessieDataplaneCaffeineCacheProvider(),
        null);
  }

  @Override
  public List<Property> getProperties() {
    List<Property> properties = super.getProperties();
    properties.addAll(
        NessiePluginUtils.getCredentialProperties(
            secure,
            getStorageProvider(),
            credentialType,
            awsAccessKey,
            awsAccessSecret,
            assumedRoleARN,
            awsProfile));
    return properties;
  }

  // TODO: DX-92705: Refactor to use a compile time enum to avoid the switch statement when moving
  // out of the NessiePluginConfig
  @VisibleForTesting
  NessieClient getNessieClient(String name, SabotContext sabotContext) {
    switch (nessieAuthType) {
      case BEARER:
        return getNessieClientWithBearerToken(name, sabotContext);
      case NONE:
        return getNessieClientWithoutAuthentication(name, sabotContext);
      case OAUTH2:
        return getNessieClientWithOAuth2Token(name, sabotContext);
      default:
        throw new UnsupportedOperationException();
    }
  }

  private NessieClient getNessieClientWithBearerToken(String name, SabotContext sabotContext) {
    NessieClientImpl nessieClient =
        new NessieClientImpl(
            getNessieRestClient(name, nessieEndpoint, nessieAccessToken),
            sabotContext.getOptionManager());
    return new UsernameAwareNessieClientImpl(nessieClient, sabotContext.getUserService());
  }

  private NessieClient getNessieClientWithoutAuthentication(
      String name, SabotContext sabotContext) {
    NessieClientImpl nessieClient =
        new NessieClientImpl(
            getNessieRestClient(name, nessieEndpoint, null), sabotContext.getOptionManager());
    return new UsernameAwareNessieClientImpl(nessieClient, sabotContext.getUserService());
  }

  private NessieClient getNessieClientWithOAuth2Token(String name, SabotContext sabotContext) {
    NessieClientImpl nessieClient =
        new NessieClientImpl(
            getNessieRestClient(
                name, nessieEndpoint, oauth2TokenEndpointURI, oauth2ClientId, oauth2ClientSecret),
            sabotContext.getOptionManager());
    return new UsernameAwareNessieClientImpl(nessieClient, sabotContext.getUserService());
  }

  String getNessieEndpoint() {
    return nessieEndpoint;
  }

  @Override
  protected NessieApiV2 getNessieRestClient(
      String name, String nessieEndpoint, SecretRef nessieAccessToken) {
    final NessieClientBuilder builder =
        NessieClientBuilder.createClientBuilder("HTTP", null).withUri(URI.create(nessieEndpoint));

    if (!SecretRef.isNullOrEmpty(nessieAccessToken)) {
      builder.withAuthentication(new SecureBearerAuthentication(nessieAccessToken));
    }

    try {
      return builder.withTracing(true).withApiCompatibilityCheck(true).build(NessieApiV2.class);
    } catch (IllegalArgumentException e) {
      throw UserException.resourceError(e)
          .message(
              "Unable to create source [%s], " + "%s must be a valid http or https address",
              name, nessieEndpoint)
          .build(logger);
    }
  }

  // TODO: DX-92705: Move to the NessiePlugins and consolidate with the above method that overrides
  // method AbstractDataplanePluginConfig.getNessieRestClient
  private NessieApiV2 getNessieRestClient(
      String name,
      String nessieEndpoint,
      String oauth2TokenEndpointURI,
      String oauth2ClientId,
      SecretRef oauth2ClientSecret) {
    NessieClientBuilder builder = null;
    try {
      builder =
          NessieClientBuilder.createClientBuilder("HTTP", null).withUri(URI.create(nessieEndpoint));
      final OAuth2AuthenticatorConfig oauth2AuthenticatorConfig =
          OAuth2AuthenticatorConfig.builder()
              .clientId(oauth2ClientId)
              .clientSecret(oauth2ClientSecret.get())
              .tokenEndpoint(URI.create(oauth2TokenEndpointURI))
              .build();

      builder.withAuthentication(OAuth2AuthenticationProvider.create(oauth2AuthenticatorConfig));
      return builder.withTracing(true).withApiCompatibilityCheck(true).build(NessieApiV2.class);
    } catch (IllegalArgumentException e) {
      throw UserException.resourceError(e)
          .message(
              "Unable to create source [%s], " + "%s must be a valid http or https address",
              name, nessieEndpoint)
          .build(logger);
    } catch (UnsupportedOperationException e) { // thrown by oAuth2ClientSecret.get()
      throw UserException.resourceError(e)
          .message(
              "Unable to create or access source [%s], " + "OAuth2 credentials are not valid", name)
          .build(logger);
    }
  }

  @Override
  public String getSourceTypeName() {
    return "Nessie";
  }
}
