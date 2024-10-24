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

import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_OAUTH_CLIENT_AUTH_ENABLED;
import static com.dremio.exec.store.DataplanePluginOptions.NESSIE_PLUGIN_ENABLED;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.server.SabotContext;
import com.dremio.nessiemetadata.cache.NessieDataplaneCacheProvider;
import com.dremio.nessiemetadata.storeprovider.NessieDataplaneCacheStoreProvider;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClientImpl;
import com.dremio.plugins.UsernameAwareNessieClientImpl;
import com.dremio.service.namespace.SourceState;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URI;
import javax.inject.Provider;
import org.jetbrains.annotations.Nullable;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticationProvider;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticatorConfig;

public class NessiePlugin extends DataplanePlugin {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(NessiePlugin.class);
  private static final String SERVER_NAME = "Nessie";

  private final String name;
  private final NessiePluginConfig pluginConfig;
  private final NessieClient nessieClient;

  public NessiePlugin(
      NessiePluginConfig pluginConfig,
      SabotContext context,
      String name,
      Provider<StoragePluginId> idProvider,
      NessieDataplaneCacheProvider cacheProvider,
      @Nullable NessieDataplaneCacheStoreProvider nessieDataplaneCacheStoreProvider) {
    super(
        pluginConfig, context, name, idProvider, cacheProvider, nessieDataplaneCacheStoreProvider);
    this.name = name;
    this.pluginConfig = pluginConfig;
    this.nessieClient = getNessieClient(name, context, pluginConfig);
  }

  @Override
  public void start() throws IOException {
    validateNessieAuthSettings(name, pluginConfig);
    super.start();
  }

  @VisibleForTesting
  protected void validateNessieAuthSettings(String name, NessiePluginConfig nessiePluginConfig) {
    if (nessiePluginConfig.nessieAuthType == null) {
      throw UserException.resourceError()
          .message("Unable to create source [%s], " + "Invalid Nessie Auth type", name)
          .build(LOGGER);
    }
    switch (nessiePluginConfig.nessieAuthType) {
      case BEARER:
        if (SecretRef.isNullOrEmpty(nessiePluginConfig.nessieAccessToken)) {
          throw UserException.resourceError()
              .message("Unable to create source [%s], " + "bearer token provided is empty", name)
              .build(LOGGER);
        }
        break;
      case OAUTH2:
        {
          if (nessiePluginConfig.oauth2TokenEndpointURI == null) {
            throw UserException.resourceError()
                .message("Unable to create source [%s], " + "OAuth2 URI must not be null", name)
                .build(LOGGER);
          } else if (nessiePluginConfig.oauth2ClientId == null) {
            throw UserException.resourceError()
                .message(
                    "Unable to create source [%s], " + "OAuth2 ClientId must not be null", name)
                .build(LOGGER);
          } else if (nessiePluginConfig.oauth2ClientSecret == null) {
            throw UserException.resourceError()
                .message(
                    "Unable to create source [%s], " + "OAuth2 ClientSecret must not be null", name)
                .build(LOGGER);
          }
          break;
        }
      case NONE:
        // Nothing to check for NONE type auth
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public SourceState getState(NessieClient nessieClient, String name, SabotContext context) {
    return NessiePluginUtils.getState(
        nessieClient, name, pluginConfig.getNessieEndpoint(), SERVER_NAME);
  }

  @Override
  public void validatePluginEnabled(SabotContext context) {
    if (!context.getOptionManager().getOption(NESSIE_PLUGIN_ENABLED)) {
      throw UserException.unsupportedError()
          .message("Nessie Source is not supported.")
          .buildSilently();
    }
    if (pluginConfig.nessieAuthType == NessieAuthType.OAUTH2
        && !context.getOptionManager().getOption(DATAPLANE_OAUTH_CLIENT_AUTH_ENABLED)) {
      throw UserException.unsupportedError()
          .message("Nessie source with OAuth2 authentication is not supported.")
          .buildSilently();
    }
  }

  @Override
  public void validateConnectionToNessieRepository(
      NessieClient nessieClient, String name, SabotContext context) {
    NessiePluginUtils.validateConnectionToNessieRepository(nessieClient);
  }

  @Override
  public void validateNessieSpecificationVersion(NessieClient nessieClient) {
    NessiePluginUtils.validateNessieSpecificationVersion(
        nessieClient, pluginConfig.getNessieEndpoint(), SERVER_NAME);
  }

  @VisibleForTesting
  NessieClient getNessieClient(
      String name, SabotContext sabotContext, NessiePluginConfig pluginConfig) {
    NessieApiV2 nessieApiV2;
    switch (pluginConfig.getNessieAuthType()) {
      case BEARER:
        nessieApiV2 =
            getNessieRestClientWithBearer(
                name, pluginConfig.getNessieEndpoint(), pluginConfig.getNessieAccessToken());
        break;
      case NONE:
        nessieApiV2 = getNessieRestClientWithNoAuth(name, pluginConfig.getNessieEndpoint());
        break;
      case OAUTH2:
        nessieApiV2 =
            getNessieRestClientWithOAuth2(
                name,
                pluginConfig.getNessieEndpoint(),
                pluginConfig.getOauth2TokenEndpointURI(),
                pluginConfig.getOauth2ClientId(),
                pluginConfig.getOauth2ClientSecret());
        break;
      default:
        throw new UnsupportedOperationException();
    }
    NessieClient nessieClient = new NessieClientImpl(nessieApiV2, sabotContext.getOptionManager());
    if (sabotContext.isCoordinator()) {
      return new UsernameAwareNessieClientImpl(nessieClient, sabotContext.getUserService());
    } else {
      // DO NOT USE UsernameAwareNessieClientImpl (as Executor doesn't have UserService)
      // IcebergNessieVersionedTableOperations is wrapped up with UserContext to pass the userId
      // for
      // talking to Nessie and userName is supplied too via the param which will be picked by
      // NessieClient#commitOperationHelper from param (if supplied) so we don't need
      // UsernameAwareNessieClientImpl
      return nessieClient;
    }
  }

  private NessieApiV2 getNessieRestClientWithBearer(
      String name, String nessieEndpoint, SecretRef nessieAccessToken) {
    NessieAuthentication nessieAuthentication = new SecureBearerAuthentication(nessieAccessToken);
    return NessiePluginUtils.getNessieRestClient(name, nessieEndpoint, nessieAuthentication);
  }

  private NessieApiV2 getNessieRestClientWithNoAuth(String name, String nessieEndpoint) {
    return NessiePluginUtils.getNessieRestClient(name, nessieEndpoint, null);
  }

  private NessieApiV2 getNessieRestClientWithOAuth2(
      String name,
      String nessieEndpoint,
      String oauth2TokenEndpointURI,
      String oauth2ClientId,
      SecretRef oauth2ClientSecret) {
    try {
      final OAuth2AuthenticatorConfig oauth2AuthenticatorConfig =
          OAuth2AuthenticatorConfig.builder()
              .clientId(oauth2ClientId)
              .clientSecret(oauth2ClientSecret.get())
              .tokenEndpoint(URI.create(oauth2TokenEndpointURI))
              .build();
      NessieAuthentication nessieAuthentication =
          OAuth2AuthenticationProvider.create(oauth2AuthenticatorConfig);
      return NessiePluginUtils.getNessieRestClient(name, nessieEndpoint, nessieAuthentication);
    } catch (UnsupportedOperationException e) { // thrown by oAuth2ClientSecret.get()
      throw UserException.resourceError(e)
          .message(
              "Unable to create or access source [%s], " + "OAuth2 credentials are not valid", name)
          .build(LOGGER);
    }
  }

  @Override
  public NessieClient getNessieClient() {
    return nessieClient;
  }
}
