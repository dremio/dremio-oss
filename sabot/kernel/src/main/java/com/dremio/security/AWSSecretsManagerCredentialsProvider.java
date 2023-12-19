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
package com.dremio.security;

import java.io.IOException;
import java.net.URI;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.aws.SharedInstanceProfileCredentialsProvider;
import com.dremio.services.credentials.CredentialsException;
import com.dremio.services.credentials.CredentialsProvider;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

/**
 * Support for AWS Secrets Manager
 */
public class AWSSecretsManagerCredentialsProvider implements CredentialsProvider {
  private static final Logger logger = LoggerFactory.getLogger(AWSSecretsManagerCredentialsProvider.class);

  private enum Arn {arn, aws, secretsManager, region, accountId, secret, secretName}

  private static final String AWS_CURRENT = "AWSCURRENT";
  private static final String ARN = "arn";
  private static final String AWS = "aws";
  private static final String SECRETS_MANAGER = "secretsmanager";
  private static final String SECRET = "secret";
  private static final String COLON_DELIMITER = ":";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public boolean isSupported(URI pattern) {
    final String scheme = pattern.getScheme().toLowerCase(Locale.ROOT);
    return ARN.equals(scheme);
  }

  @Override
  public String lookup(URI secretURI) throws IllegalArgumentException, CredentialsException {
    Preconditions.checkArgument(secretURI != null, "Invalid secret URI passed");

    final String secretArn = secretURI.toString();
    Preconditions.checkState(!secretArn.isEmpty(), "Invalid secret arn during secret info construction");

    final String[] arnTokens = secretArn.split(COLON_DELIMITER);
    Preconditions.checkState(isValidARN(arnTokens), "Invalid secret ARN passed");

    final String secret = getSecret(arnTokens);
    return extractCredentials(secret);
  }

  private static String getSecret(String[] arnTokens) throws CredentialsException {
    String region = arnTokens[Arn.region.ordinal()];
    String secretName = getSecretName(arnTokens[Arn.secretName.ordinal()]);

    /*
     * Currently, dremio would support access of the secrets manager with base role assigned
     * to EC2 machine. This will be further enhanced, once we have more requirements on it.
     */
    AwsCredentialsProvider awsCredentialsProvider = new SharedInstanceProfileCredentialsProvider();
    GetSecretValueRequest secretValueRequest = GetSecretValueRequest.builder().secretId(secretName)
      .versionStage(AWS_CURRENT).build();

    try (final SecretsManagerClient secretsManagerClient = SecretsManagerClient.builder()
      .region(Region.of(region))
      .credentialsProvider(awsCredentialsProvider)
      .build()) {
      final GetSecretValueResponse secretValueResponse = secretsManagerClient.getSecretValue(secretValueRequest);
      return (secretValueResponse.secretString() != null) ?
        secretValueResponse.secretString() : secretValueResponse.secretBinary().toString();
    } catch (SdkException e) {
      logger.debug("Unable to retrieve secret for secret {} as {}", secretName, e.getMessage());
      throw new CredentialsException(e.getMessage(), e);
    }
  }

  private static String getSecretName(String secret) {
    int hyphenIndex = secret.lastIndexOf("-");
    return (hyphenIndex != -1) ? secret.substring(0, hyphenIndex) : secret;
  }

  /*
   * Do some preliminary format check.
   * ARN format : "arn:aws:secretsmanager:<region>:<account-id-number>:secret:secret_name"
   */
  private static boolean isValidARN(String[] arnTokens) {
    return arnTokens.length == 7 &&
      arnTokens[Arn.arn.ordinal()].equals(ARN) &&
      arnTokens[Arn.aws.ordinal()].equals(AWS) &&
      arnTokens[Arn.secretsManager.ordinal()].equals(SECRETS_MANAGER) &&
      arnTokens[Arn.secret.ordinal()].equals(SECRET);
  }

  /*
   * Aws secret manager secrets can be stored in json or plain text.
   * First, try parsing the json object and if it fails, return the secret as it is.
   */
  private static String extractCredentials(String secret) {
    try {
      return MAPPER.readValue(secret, PasswordCredentials.class)
        .getPassword();
    } catch (IOException e) {
      logger.debug("Secret might not be in json format, Returning secret as is");
    }
    return secret;
  }

  /**
   * Password credentials contains only password.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static final class PasswordCredentials {
    @JsonProperty("password")
    private final String password;

    public PasswordCredentials(@JsonProperty("password") String password) {
      this.password = password;
    }

    public String getPassword() {
      return password;
    }
  }
}
