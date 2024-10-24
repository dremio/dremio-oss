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
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.ACCESS_KEY_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.ASSUME_ROLE_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.AWS_PROFILE_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.EC2_METADATA_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.NONE_PROVIDER;
import static com.dremio.plugins.dataplane.NessiePluginConfigConstants.MINIMUM_NESSIE_SPECIFICATION_VERSION;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.store.ConnectionRefusedException;
import com.dremio.exec.store.HttpClientRequestException;
import com.dremio.exec.store.InvalidNessieApiVersionException;
import com.dremio.exec.store.InvalidSpecificationVersionException;
import com.dremio.exec.store.InvalidURLException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.SemanticVersionParserException;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.azure.AzureStorageFileSystem;
import com.dremio.plugins.util.awsauth.AWSCredentialsConfigurator;
import com.dremio.service.namespace.SourceState;
import com.google.common.base.Strings;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.parquet.SemanticVersion;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.auth.oauth2.OAuth2Exception;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.NessieApiCompatibilityException;
import org.projectnessie.client.rest.NessieServiceException;
import org.projectnessie.model.NessieConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessiePluginUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessiePluginUtils.class);

  public static List<Property> getCredentialProperties(
      boolean secure,
      AbstractDataplanePluginConfig.StorageProviderType storageProviderType,
      AWSAuthenticationType credentialType,
      String awsAccessKey,
      SecretRef awsAccessSecret,
      String assumedRoleARN,
      String awsProfile) {
    final List<Property> properties = new ArrayList<>();

    switch (storageProviderType) {
      case AWS:
        properties.add(new Property(SECURE_CONNECTIONS, Boolean.toString(secure)));
        final AWSCredentialsConfigurator awsCredentialsProvider =
            getAwsCredentialsProvider(
                credentialType, awsAccessKey, awsAccessSecret, assumedRoleARN, awsProfile);
        final String awsProvider = awsCredentialsProvider.configureCredentials(properties);
        properties.add(new Property(AWS_CREDENTIALS_PROVIDER, awsProvider));
        break;
      case AZURE:
        properties.add(new Property(AzureStorageFileSystem.SECURE, Boolean.toString(secure)));
        // Other setup already done as part of AbstractDataplanePluginConfig
        break;
      case GOOGLE:
        // Setup already done as part of AbstractDataplanePluginConfig
        break;
      default:
        throw new IllegalArgumentException(
            "Unexpected storage provider type: " + storageProviderType);
    }

    return properties;
  }

  private static AWSCredentialsConfigurator getAwsCredentialsProvider(
      AWSAuthenticationType credentialType,
      String awsAccessKey,
      SecretRef awsAccessSecret,
      String assumedRoleARN,
      String awsProfile) {
    AWSCredentialsConfigurator awsCredentialsConfigurator =
        getPrimaryAwsCredentialsProvider(credentialType, awsAccessKey, awsAccessSecret, awsProfile);
    return wrapAssumedRoleToProvider(awsCredentialsConfigurator, assumedRoleARN);
  }

  private static AWSCredentialsConfigurator getPrimaryAwsCredentialsProvider(
      AWSAuthenticationType credentialType,
      String awsAccessKey,
      SecretRef awsAccessSecret,
      String awsProfile) {
    if (credentialType == null) {
      throw UserException.validationError()
          .message(
              "Failure creating an S3 connection. You must provide an authentication method [credentialType].")
          .build(LOGGER);
    }

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
        throw new UnsupportedOperationException(
            "Failure creating an S3 connection. Unsupported credential type:" + credentialType);
    }
  }

  public static AWSCredentialsConfigurator wrapAssumedRoleToProvider(
      AWSCredentialsConfigurator configurator, String assumedRoleARN) {
    return properties -> {
      String mainAWSCredProvider = configurator.configureCredentials(properties);
      if (hasAssumedRoleARN(assumedRoleARN) && !NONE_PROVIDER.equals(mainAWSCredProvider)) {
        properties.add(new Property(Constants.ASSUMED_ROLE_ARN, assumedRoleARN));
        properties.add(
            new Property(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER, mainAWSCredProvider));
        mainAWSCredProvider = ASSUME_ROLE_PROVIDER;
      }
      return mainAWSCredProvider;
    };
  }

  protected static String getAccessKeyProvider(
      List<Property> properties, String accessKey, SecretRef accessSecret) {
    if (Strings.isNullOrEmpty(accessKey) || SecretRef.isNullOrEmpty(accessSecret)) {
      throw UserException.validationError()
          .message(
              "Failure creating an S3 connection. You must provide an AWS access key and AWS access secret [awsAccessKey, awsAccessSecret].")
          .build(LOGGER);
    }
    properties.add(new Property(Constants.ACCESS_KEY, accessKey));
    properties.add(
        new Property(
            Constants.SECRET_KEY, SecretRef.toConfiguration(accessSecret, DREMIO_SCHEME_PREFIX)));
    return ACCESS_KEY_PROVIDER;
  }

  private static boolean hasAssumedRoleARN(String assumedRoleARN) {
    return !Strings.isNullOrEmpty(assumedRoleARN);
  }

  protected static NessieApiV2 getNessieRestClient(
      String name, String nessieEndpoint, NessieAuthentication nessieAuthentication) {
    final NessieClientBuilder builder =
        NessieClientBuilder.createClientBuilder("HTTP", null).withUri(URI.create(nessieEndpoint));

    if (nessieAuthentication != null) {
      builder.withAuthentication(nessieAuthentication);
    }

    try {
      return builder.withTracing(true).withApiCompatibilityCheck(true).build(NessieApiV2.class);
    } catch (IllegalArgumentException e) {
      throw UserException.resourceError(e)
          .message(
              "Unable to create source [%s], " + "%s must be a valid http or https address",
              name, nessieEndpoint)
          .build(LOGGER);
    }
  }

  protected static SourceState getState(
      NessieClient nessieClient, String name, String nessieEndpoint, String serverName) {
    try {
      validateConnectionToNessieRepository(nessieClient);
      validateNessieSpecificationVersion(nessieClient, nessieEndpoint, serverName);
    } catch (NoDefaultBranchException e) {
      logExceptionDetail(name, nessieEndpoint, "NoDefaultBranch", e);
      return SourceState.badState(
          String.format("Check your %s server", serverName),
          String.format(
              "Could not connect to [%s]. No default branch exists in the %s server.",
              name, serverName));
    } catch (UnAuthenticatedException e) {
      logExceptionDetail(name, nessieEndpoint, "Unauthenticated", e);
      return SourceState.badState(
          "Make sure that the token is valid and not expired",
          String.format(
              "Could not connect to [%s]. Unable to authenticate to the %s server.",
              name, serverName));
    } catch (ConnectionRefusedException e) {
      logExceptionDetail(name, nessieEndpoint, "ConnectionRefused", e);
      return SourceState.badState(
          String.format("Make sure that the %s server is up and running", serverName),
          String.format(
              "Could not connect to [%s]. Connection refused while connecting to the %s Server.",
              name, serverName));
    } catch (InvalidURLException e) {
      logExceptionDetail(name, nessieEndpoint, "InvalidURL", e);
      return SourceState.badState(
          String.format("Make sure that Nessie endpoint URL [%s] is valid.", nessieEndpoint),
          String.format("Could not connect to [%s].", name));
    } catch (InvalidSpecificationVersionException e) {
      logExceptionDetail(name, nessieEndpoint, "InvalidSpecificationVersion", e);
      return SourceState.badState(
          String.format(
              "%s Server should comply with Nessie specification version %s or later. Also make sure that Nessie endpoint URL is valid.",
              serverName, MINIMUM_NESSIE_SPECIFICATION_VERSION),
          String.format("Could not connect to [%s].", name));
    } catch (SemanticVersionParserException e) {
      logExceptionDetail(name, nessieEndpoint, "SemanticVersionParser", e);
      return SourceState.badState(
          String.format(
              "%s Server should comply with Nessie specification version %s or later.",
              serverName, MINIMUM_NESSIE_SPECIFICATION_VERSION),
          String.format(
              "Could not connect to [%s]. Cannot parse Nessie specification version.", name));
    } catch (InvalidNessieApiVersionException e) {
      logExceptionDetail(name, nessieEndpoint, "InvalidNessieApiVersion", e);
      return SourceState.badState(
          String.format(
              "Invalid API version. Make sure that Nessie endpoint URL [%s] has a valid API version.",
              nessieEndpoint),
          String.format("Could not connect to [%s].", name));
    } catch (HttpClientRequestException e) {
      logExceptionDetail(name, nessieEndpoint, "HTTP client request exception", e);
      if (e.getCause() instanceof OAuth2Exception) {
        return SourceState.badState(
            "Make sure that the Oauth2 ClientID, Client Secret and OAuth2 token endpoint URI are valid.",
            String.format("Could not connect to [%s].", name));
      }
    } catch (Exception e) {
      // For any unknowns
      logExceptionDetail(name, nessieEndpoint, "Unexpected exception", e);
      return SourceState.badState(
          "Check your settings, credentials and Nessie server",
          String.format("Could not connect to [%s].", name));
    }
    return SourceState.GOOD;
  }

  private static void logExceptionDetail(
      String name, String nessieEndpoint, String exceptionSummary, Exception e) {
    LOGGER.error(
        "Exception [{}] while validating connection from source {}: to Nessie server : {}",
        exceptionSummary,
        name,
        nessieEndpoint,
        e);
  }

  private static NessieConfiguration getNessieConfig(
      NessieApiV2 nessieApiV2, String nessieEndpoint) {
    try {
      return nessieApiV2.getConfig();
    } catch (NessieApiCompatibilityException e) {
      throw new InvalidNessieApiVersionException(
          e,
          "Invalid API version. "
              + "Make sure that Nessie endpoint URL [%s] has a valid API version. Expected version is 2.",
          nessieEndpoint);
    } catch (OAuth2Exception e) {
      throw new UnAuthenticatedException(
          e,
          "Make sure that the Oauth2 ClientID, Client Secret and OAuth2 token endpoint URI are valid.");
    } catch (IllegalArgumentException | HttpClientException | NessieServiceException e) {
      throw new InvalidURLException(
          e, "Make sure that Nessie endpoint URL [%s] is valid.", nessieEndpoint);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static void validateConnectionToNessieRepository(NessieClient nessieClient) {
    nessieClient.getDefaultBranch();
  }

  protected static void validateNessieSpecificationVersion(
      NessieClient nessieClient, String nessieEndpoint, String serverName) {
    NessieApiV2 nessieApi = nessieClient.getNessieApi();
    NessieConfiguration nessieConfiguration = getNessieConfig(nessieApi, nessieEndpoint);
    String specificationVersion = nessieConfiguration.getSpecVersion();
    if (specificationVersion == null) {
      // This happens when you are using the older server, or you are trying to pass the v1 endpoint
      // for supported OSS Nessie sever (which supports v2)
      throw new InvalidSpecificationVersionException(
          "%s Server should comply with Nessie specification version %s or later."
              + " Also make sure that Nessie endpoint URL is valid.",
          serverName, MINIMUM_NESSIE_SPECIFICATION_VERSION);
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
                + "%s Server should comply with Nessie specification version %s or later.",
            specificationVersion,
            serverName,
            MINIMUM_NESSIE_SPECIFICATION_VERSION);
      }
      if (result < 0) {
        throw new InvalidSpecificationVersionException(
            "%s Server should comply with Nessie specification version %s or later."
                + " Also make sure that Nessie endpoint URL is valid.",
            serverName, MINIMUM_NESSIE_SPECIFICATION_VERSION);
      }
    }
  }
}
