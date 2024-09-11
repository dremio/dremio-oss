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
import static com.dremio.plugins.dataplane.NessiePluginConfigConstants.MINIMUM_NESSIE_SPECIFICATION_VERSION;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.ConnectionRefusedException;
import com.dremio.exec.store.HttpClientRequestException;
import com.dremio.exec.store.InvalidNessieApiVersionException;
import com.dremio.exec.store.InvalidSpecificationVersionException;
import com.dremio.exec.store.InvalidURLException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.SemanticVersionParserException;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.nessiemetadata.cache.NessieDataplaneCacheProvider;
import com.dremio.nessiemetadata.storeprovider.NessieDataplaneCacheStoreProvider;
import com.dremio.plugins.NessieClient;
import com.dremio.service.namespace.SourceState;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import javax.inject.Provider;
import org.apache.parquet.SemanticVersion;
import org.jetbrains.annotations.Nullable;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.auth.oauth2.OAuth2Exception;
import org.projectnessie.client.http.NessieApiCompatibilityException;
import org.projectnessie.model.NessieConfiguration;

public class NessiePlugin extends DataplanePlugin {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(NessiePlugin.class);

  private final String name;
  private final NessiePluginConfig pluginConfig;

  public NessiePlugin(
      NessiePluginConfig pluginConfig,
      SabotContext context,
      String name,
      Provider<StoragePluginId> idProvider,
      NessieClient nessieClient,
      NessieDataplaneCacheProvider cacheProvider,
      @Nullable NessieDataplaneCacheStoreProvider nessieDataplaneCacheStoreProvider) {
    super(
        pluginConfig,
        context,
        name,
        idProvider,
        nessieClient,
        cacheProvider,
        nessieDataplaneCacheStoreProvider);
    this.name = name;
    this.pluginConfig = pluginConfig;
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
    validateStorageProviderTypeEnabled(context.getOptionManager());
    try {
      this.validateConnectionToNessieRepository(nessieClient, name, context);
      this.validateNessieSpecificationVersion(nessieClient, name);
    } catch (NoDefaultBranchException e) {
      return SourceState.badState(
          "Check your Nessie server",
          String.format(
              "Could not connect to [%s]. No default branch exists in the Nessie server.", name));
    } catch (UnAuthenticatedException e) {
      return SourceState.badState(
          "Make sure that the token is valid and not expired",
          String.format(
              "Could not connect to [%s]. Unable to authenticate to the Nessie server.", name));
    } catch (ConnectionRefusedException e) {
      return SourceState.badState(
          "Make sure that the Nessie server is up and running",
          String.format(
              "Could not connect to [%s]. Connection refused while connecting to the Nessie Server.",
              name));
    } catch (InvalidURLException e) {
      return SourceState.badState(
          String.format(
              "Make sure that Nessie endpoint URL [%s] is valid.",
              pluginConfig.getNessieEndpoint()),
          String.format("Could not connect to [%s].", name));
    } catch (InvalidSpecificationVersionException e) {
      return SourceState.badState(
          String.format(
              "Nessie Server should comply with Nessie specification version %s or later. Also make sure that Nessie endpoint URL is valid.",
              MINIMUM_NESSIE_SPECIFICATION_VERSION),
          String.format("Could not connect to [%s].", name));
    } catch (SemanticVersionParserException e) {
      return SourceState.badState(
          String.format(
              "Nessie Server should comply with Nessie specification version %s or later.",
              MINIMUM_NESSIE_SPECIFICATION_VERSION),
          String.format(
              "Could not connect to [%s]. Cannot parse Nessie specification version.", name));
    } catch (InvalidNessieApiVersionException e) {
      return SourceState.badState(
          String.format(
              "Invalid API version. Make sure that Nessie endpoint URL [%s] has a valid API version.",
              pluginConfig.getNessieEndpoint()),
          String.format("Could not connect to [%s].", name));
    } catch (HttpClientRequestException e) {
      if (e.getCause() instanceof OAuth2Exception) {
        return SourceState.badState(
            "Make sure that the Oauth2 ClientID, Client Secret and OAuth2 token endpoint URI are valid.",
            String.format("Could not connect to [%s].", name));
      }
    } catch (Exception e) {
      // For any unknowns
      return SourceState.badState(
          "Check your settings, credentials and Nessie server",
          String.format("Could not connect to [%s].", name));
    }
    return SourceState.GOOD;
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
    nessieClient.getDefaultBranch();
  }

  @Override
  public void validateNessieSpecificationVersion(NessieClient nessieClient, String name) {
    NessieApiV2 nessieApi = nessieClient.getNessieApi();
    NessieConfiguration nessieConfiguration = getNessieConfig(nessieApi);
    validateNessieSpecificationVersionHelper(nessieConfiguration.getSpecVersion());
  }

  @VisibleForTesting
  NessieConfiguration getNessieConfig(NessieApiV2 nessieApiV2) {
    try {
      return nessieApiV2.getConfig();
    } catch (NessieApiCompatibilityException e) {
      throw new InvalidNessieApiVersionException(
          e,
          "Invalid API version. "
              + "Make sure that Nessie endpoint URL [%s] has a valid API version. Expected version is 2.",
          pluginConfig.getNessieEndpoint());
    } catch (OAuth2Exception e) {
      throw new UnAuthenticatedException(
          e,
          "Make sure that the Oauth2 ClientID, Client Secret and OAuth2 token endpoint URI are valid.");
    } catch (Exception e) {
      // IllegalArgumentException, HttpClientException and NessieServiceException are seen when we
      // provide wrong urls in the Nessie endpoint
      throw new InvalidURLException(
          e, "Make sure that Nessie endpoint URL [%s] is valid.", pluginConfig.getNessieEndpoint());
    }
  }

  @VisibleForTesting
  void validateNessieSpecificationVersionHelper(String specificationVersion) {
    if (specificationVersion == null) {
      // This happens when you are using the older server, or you are trying to pass the v1 endpoint
      // for supported OSS Nessie sever (which supports v2)
      throw new InvalidSpecificationVersionException(
          "Nessie Server should comply with Nessie specification version %s or later."
              + " Also make sure that Nessie endpoint URL is valid.",
          MINIMUM_NESSIE_SPECIFICATION_VERSION);
    } else {
      int result;
      try {
        result =
            SemanticVersion.parse(specificationVersion)
                .compareTo(SemanticVersion.parse(MINIMUM_NESSIE_SPECIFICATION_VERSION));
      } catch (SemanticVersion.SemanticVersionParseException ex) {
        throw new SemanticVersionParserException(
            ex,
            "Cannot parse Nessie specification version %s. "
                + "Nessie Server should comply with Nessie specification version %s or later.",
            specificationVersion,
            MINIMUM_NESSIE_SPECIFICATION_VERSION);
      }
      if (result < 0) {
        throw new InvalidSpecificationVersionException(
            "Nessie Server should comply with Nessie specification version %s or later."
                + " Also make sure that Nessie endpoint URL is valid.",
            MINIMUM_NESSIE_SPECIFICATION_VERSION);
      }
    }
  }
}
