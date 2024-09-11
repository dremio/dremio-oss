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
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.plugins.azure.AzureStorageFileSystem;
import com.dremio.plugins.util.awsauth.AWSCredentialsConfigurator;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.s3a.Constants;
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
}
