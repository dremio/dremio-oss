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

import com.dremio.aws.SharedInstanceProfileCredentialsProvider;
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
public class AwsSecretsManager implements Vault {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AwsSecretsManager.class);
  private static final String AWS_CURRENT = "AWSCURRENT";
  private static final String ARN = "arn";
  private static final String AWS = "aws";
  private static final String SECRETS_MANAGER = "secretsmanager";
  private static final String SECRET = "secret";
  private static final String COLON_DELIMITER = ":";

  private enum Arn { arn, aws, secretsManager, region, accountId, secret, secretName }

  private final ObjectMapper objectMapper;

  public AwsSecretsManager() {
    objectMapper = new ObjectMapper();
  }

  @Override
  public Credentials getCredentials(URI secretURI) throws IOException {
    Preconditions.checkArgument(secretURI != null, "Invalid secret uri passed");
    AwsSecretManagerSecretInfo secretInfo = constructSecretInfo(secretURI);
    Preconditions.checkArgument(secretInfo != null, "Invalid secret info passed");

    final String secretArn = secretInfo.getSecretArn();
    Preconditions.checkArgument(secretArn != null && !secretArn.isEmpty(), "Invalid arn for the secret");

    String[] arnTokens = secretArn.split(COLON_DELIMITER);
    Preconditions.checkState(isValidARN(arnTokens), "Invalid secret ARN passed");

    String secret = getSecret(arnTokens);
    return extractCredentials(secret);
  }

  private String getSecret(String[] arnTokens) throws IOException {
    String region = arnTokens[Arn.region.ordinal()];
    String secretName = getSecretName(arnTokens[Arn.secretName.ordinal()]);

    /*
     * Currently, dremio would support access of the secrets manager with base role assigned
     * to EC2 machine. This will be further enhanced, once we have more requirements on it.
     */
    AwsCredentialsProvider awsCredentialsProvider = getAwsCredentials();
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
      throw new IOException(e.getMessage(), e);
    }
  }

  private String getSecretName(String secret) {
    int hyphenIndex = secret.lastIndexOf("-");
    return (hyphenIndex != -1) ? secret.substring(0, hyphenIndex) : secret;
  }

  /*
   * Do some preliminary format check.
   * ARN format : "arn:aws:secretsmanager:<region>:<account-id-number>:secret:secret_name"
   */
  private boolean isValidARN(String[] arnTokens) {
    return arnTokens.length == 7 && arnTokens[Arn.arn.ordinal()].equals(ARN) && arnTokens[Arn.aws.ordinal()].equals(AWS) &&
      arnTokens[Arn.secretsManager.ordinal()].equals(SECRETS_MANAGER) && arnTokens[Arn.secret.ordinal()].equals(SECRET);
  }

  private AwsCredentialsProvider getAwsCredentials() {
    return new SharedInstanceProfileCredentialsProvider();
  }

  /*
   * Aws secret manager secrets can be stored in json or plain text.
   * First, try parsing the json object and if it fails, return the secret as it is.
   */
  private Credentials extractCredentials(String secret) {
    try {
      return objectMapper.readValue(secret, PasswordCredentials.class);
    } catch (IOException e) {
      logger.debug("Secret might not be in json format, Returning secret as is");
    }
    return new PasswordCredentials(secret);
  }

  private AwsSecretManagerSecretInfo constructSecretInfo(URI secretURI) {
    String secretArn = secretURI.toString();
    Preconditions.checkState(secretArn != null && !secretArn.isEmpty(), "Invalid secret arn during secret info construction");
    return new AwsSecretManagerSecretInfo(secretArn);
  }
}
