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
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.AWS_PROFILE_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.EC2_METADATA_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.NONE_PROVIDER;
import static com.dremio.plugins.dataplane.NessiePluginConfigConstants.MINIMUM_NESSIE_SPECIFICATION_VERSION;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import org.apache.parquet.SemanticVersion;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.auth.BearerAuthenticationProvider;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.http.NessieApiCompatibilityException;
import org.projectnessie.model.NessieConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.ConnectionRefusedException;
import com.dremio.exec.store.InvalidNessieApiVersionException;
import com.dremio.exec.store.InvalidSpecificationVersionException;
import com.dremio.exec.store.InvalidURLException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.SemanticVersionParserException;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClientImpl;
import com.dremio.plugins.UsernameAwareNessieClientImpl;
import com.dremio.plugins.azure.AzureStorageFileSystem;
import com.dremio.plugins.util.awsauth.AWSCredentialsConfigurator;
import com.dremio.service.namespace.SourceState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import io.protostuff.Tag;

/**
 * Connection configuration for Nessie source Plugin.
 */
@SourceType(value = "NESSIE", label = "Nessie (Preview)", uiConfig = "nessie-layout.json")
public class NessiePluginConfig extends AbstractDataplanePluginConfig {
  private static final Logger logger = LoggerFactory.getLogger(NessiePluginConfig.class);

  @Tag(1)
  @DisplayMetadata(label = "Nessie endpoint URL")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not metadata impacting
  public String nessieEndpoint;

  @Tag(2)
  @Secret
  @DisplayMetadata(label = "Bearer token")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not metadata impacting
  public String nessieAccessToken;

  // Tags 3 through 7 are defined in AbstractDataplanePluginConfig

  @Tag(8)
  @DisplayMetadata(label = "Authentication method")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not metadata impacting
  public AWSAuthenticationType credentialType = AWSAuthenticationType.ACCESS_KEY;

  // Tags 9 through 12 are defined in AbstractDataplanePluginConfig

  // Tag 13 is reserved

  @Tag(14)
  @DisplayMetadata(label = "Nessie authentication type")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not metadata impacting
  public NessieAuthType nessieAuthType = NessieAuthType.BEARER;

  @Tag(15)
  @DisplayMetadata(label = "AWS profile")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not metadata impacting
  public String awsProfile;

  @Tag(16)
  @DisplayMetadata(label = "Encrypt connection")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not metadata impacting
  public boolean secure = true;

  // Tags 17 through 24 are defined in AbstractDataplanePluginConfig

  @Override
  public DataplanePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    final NessieClient nessieClient = getNessieClient(name, context);

    return new DataplanePlugin(this, context, name, pluginIdProvider, nessieClient);
  }

  @Override
  public List<Property> getProperties() {
    final List<Property> properties = new ArrayList<>(super.getProperties());

    switch (getStorageProvider()) {
      case AWS:
        properties.add(new Property(SECURE_CONNECTIONS, Boolean.toString(secure)));
        final AWSCredentialsConfigurator awsCredentialsProvider = getAWSCredentialsProvider();
        final String awsProvider = awsCredentialsProvider.configureCredentials(properties);
        properties.add(new Property(AWS_CREDENTIALS_PROVIDER, awsProvider));
        break;
      case AZURE:
        properties.add(new Property(AzureStorageFileSystem.SECURE, Boolean.toString(secure)));
        // Other setup already done as part of AbstractDataplanePluginConfig
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + getStorageProvider());
    }

    return properties;
  }

  @Override
  public String getConnection() {
    // Allow local filesystem usage if no authentication provided
    if ((getStorageProvider() == StorageProviderType.AWS
        && Strings.isNullOrEmpty(awsAccessKey)
        && Strings.isNullOrEmpty(awsAccessSecret)) ||
      (getStorageProvider() == StorageProviderType.AZURE
        && Strings.isNullOrEmpty(azureAccessKey))) {
      return "file:///";
    }

    return super.getConnection();
  }

  @Override
  public void validatePluginEnabled(SabotContext context) {
    if (!context.getOptionManager().getOption(NESSIE_PLUGIN_ENABLED)) {
      throw UserException.unsupportedError()
        .message("Nessie Source is not supported.")
        .buildSilently();
    }
  }

  @Override
  public void validateConnectionToNessieRepository(NessieClient nessieClient, String name, SabotContext context) {
    nessieClient.getDefaultBranch();
  }

  @VisibleForTesting
  NessieClient getNessieClient(String name, SabotContext sabotContext) {
    NessieClientImpl nessieClient = new NessieClientImpl(
      getNessieRestClient(name, nessieEndpoint, nessieAccessToken),
      sabotContext.getOptionManager());
    return new UsernameAwareNessieClientImpl(nessieClient, sabotContext.getUserService());
  }

  @Override
  public void validateNessieAuthSettings(String name) {
    if (nessieAuthType == null) {
      throw UserException.resourceError().message("Unable to create source [%s], " +
        "Invalid Nessie Auth type", name).build(logger);
    }
    switch (nessieAuthType) {
      case BEARER:
        if (Strings.isNullOrEmpty(nessieAccessToken)) {
          throw UserException.resourceError().message("Unable to create source [%s], " +
            "bearer token provided is empty", name).build(logger);
        }
        break;
      case NONE:
        // Nothing to check for NONE type auth
        break;
      default:
        throw new UnsupportedOperationException();
    }
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
        throw new InvalidNessieApiVersionException(e, "Invalid API version. " +
          "Make sure that Nessie endpoint URL [%s] has a valid API version. Expected version is 2.", nessieEndpoint);
      } catch (Exception e) {
        //IllegalArgumentException, HttpClientException and NessieServiceException are seen when we provide wrong urls in the Nessie endpoint
        throw new InvalidURLException(e, "Make sure that Nessie endpoint URL [%s] is valid.", nessieEndpoint);
      }
  }

  @VisibleForTesting
  void validateNessieSpecificationVersionHelper(String specificationVersion) {
    if (specificationVersion == null) {
      // This happens when you are using the older server, or you are trying to pass the v1 endpoint for supported OSS Nessie sever (which supports v2)
      throw new InvalidSpecificationVersionException("Nessie Server should comply with Nessie specification version %s or later." +
        " Also make sure that Nessie endpoint URL is valid.", MINIMUM_NESSIE_SPECIFICATION_VERSION);
    } else {
      int result;
      try {
        result = SemanticVersion.parse(specificationVersion).compareTo(SemanticVersion.parse(MINIMUM_NESSIE_SPECIFICATION_VERSION));
      } catch (SemanticVersion.SemanticVersionParseException ex) {
        throw new SemanticVersionParserException(ex, "Cannot parse Nessie specification version %s. " +
          "Nessie Server should comply with Nessie specification version %s or later.", specificationVersion, MINIMUM_NESSIE_SPECIFICATION_VERSION);
      }
      if (result < 0) {
        throw new InvalidSpecificationVersionException("Nessie Server should comply with Nessie specification version %s or later." +
          " Also make sure that Nessie endpoint URL is valid.", MINIMUM_NESSIE_SPECIFICATION_VERSION);
      }
    }
  }

  @VisibleForTesting
  AWSCredentialsConfigurator getAWSCredentialsProvider(){
    AWSCredentialsConfigurator awsCredentialsConfigurator = getPrimaryAWSCredentialsProvider();
    return wrapAssumedRoleToProvider(awsCredentialsConfigurator);
  }

  private AWSCredentialsConfigurator getPrimaryAWSCredentialsProvider() {
    switch (credentialType) {
      case ACCESS_KEY:
        return properties -> getAccessKeyProvider(properties, awsAccessKey, awsAccessSecret);
      case AWS_PROFILE:
        return properties -> {
          if (awsProfile != null) {
            properties.add(new Property("com.dremio.awsProfile", awsProfile));
          }
          return AWS_PROFILE_PROVIDER;
        };
      case EC2_METADATA:
        return properties -> EC2_METADATA_PROVIDER;
      case NONE:
        return properties -> NONE_PROVIDER;
      default:
        throw new UnsupportedOperationException("Failure creating S3 connection. Unsupported credential type:" + credentialType);
    }
  }
  @Override
  protected NessieApiV2 getNessieRestClient(String name, String nessieEndpoint, String nessieAccessToken) {
    final HttpClientBuilder builder = HttpClientBuilder.builder()
      .withUri(URI.create(nessieEndpoint));

    if (!Strings.isNullOrEmpty(nessieAccessToken)) {
      builder.withAuthentication(BearerAuthenticationProvider.create(nessieAccessToken));
    }

    try {
      return builder
        .withTracing(true)
        .withApiCompatibilityCheck(true)
        .build(NessieApiV2.class);
    } catch (IllegalArgumentException e) {
      throw UserException.resourceError().message("Unable to create source [%s], " +
        "%s must be a valid http or https address", name, nessieEndpoint).build(logger);
    }
  }

  @Override
  public SourceState getState(NessieClient nessieClient, String name, SabotContext context) {
    validateAzureStorageProviderEnabled(context.getOptionManager());
    try {
      this.validateConnectionToNessieRepository(nessieClient, name, context);
      this.validateNessieSpecificationVersion(nessieClient, name);
    } catch (NoDefaultBranchException e){
      return SourceState.badState( "Check your Nessie server",
        String.format("Could not connect to [%s]. No default branch exists in the Nessie server.", name));
    } catch (UnAuthenticatedException e) {
      return SourceState.badState("Make sure that the token is valid and not expired",
        String.format("Could not connect to [%s]. Unable to authenticate to the Nessie server.", name));
    } catch (ConnectionRefusedException e) {
      return SourceState.badState("Make sure that the Nessie server is up and running",
        String.format("Could not connect to [%s]. Connection refused while connecting to the Nessie Server." , name));
    } catch (InvalidURLException e) {
      return SourceState.badState(String.format("Make sure that Nessie endpoint URL [%s] is valid.", nessieEndpoint),
        String.format("Could not connect to [%s].", name));
    } catch (InvalidSpecificationVersionException e) {
      return SourceState.badState(String.format("Nessie Server should comply with Nessie specification version %s or later. Also make sure that Nessie endpoint URL is valid.", MINIMUM_NESSIE_SPECIFICATION_VERSION),
        String.format("Could not connect to [%s].", name));
    } catch (SemanticVersionParserException e) {
      return SourceState.badState(String.format("Nessie Server should comply with Nessie specification version %s or later.", MINIMUM_NESSIE_SPECIFICATION_VERSION),
        String.format("Could not connect to [%s]. Cannot parse Nessie specification version.", name));
    } catch (InvalidNessieApiVersionException e) {
      return SourceState.badState(String.format("Invalid API version. Make sure that Nessie endpoint URL [%s] has a valid API version.", nessieEndpoint),
        String.format("Could not connect to [%s].", name));
    } catch (Exception e) {
      //For any unknowns
      return SourceState.badState("Check your settings, credentials and Nessie server",
        String.format("Could not connect to [%s]." , name));
    }
    return SourceState.GOOD;
  }

  @Override
  public String getSourceTypeName() {
    return "Nessie";
  }
}
