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

import com.dremio.aws.SharedInstanceProfileCredentialsProvider;
import com.dremio.services.credentials.CredentialsException;
import com.dremio.services.credentials.CredentialsProvider;
import com.dremio.telemetry.api.metrics.MeterProviders;
import com.dremio.telemetry.api.metrics.TimerUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Timer.ResourceSample;
import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

/** Support for AWS Secrets Manager */
public class AWSSecretsManagerCredentialsProvider implements CredentialsProvider {
  private static final Logger logger =
      LoggerFactory.getLogger(AWSSecretsManagerCredentialsProvider.class);

  private static final Supplier<ResourceSample> LOOKUP_TIMER =
      MeterProviders.newTimerResourceSampleSupplier(
          "credentials_service.credentials_provider.aws_secrets_manager.lookup",
          "Time taken to look up credential values from AWS Secrets Manager");

  private enum Arn {
    arn,
    aws,
    secretsManager,
    region,
    accountId,
    secret,
    secretName
  }

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
    Preconditions.checkState(
        !secretArn.isEmpty(), "Invalid secret arn during secret info construction");

    final String[] arnTokens = secretArn.split(COLON_DELIMITER);
    Preconditions.checkState(isValidARN(arnTokens), "Invalid secret ARN passed");

    final String secret =
        TimerUtils.timedExceptionThrowingOperation(
            LOOKUP_TIMER.get(), () -> getSecret(secretArn, arnTokens[Arn.region.ordinal()]));
    final String credentialValue = extractCredentials(secret);
    if (Strings.isNullOrEmpty(credentialValue)) {
      throw new CredentialsException(
          String.format(
              "Retrieved secret value for secret reference '%s' is not valid. Expected a non-empty "
                  + "string value mapped to the 'password' (case-sensitive) JSON key, or for the "
                  + "secret value to be the plaintext credential string",
              secretURI));
    }

    return credentialValue;
  }

  private String getSecret(final String secretArn, final String region)
      throws CredentialsException {
    final Stopwatch stopwatch = Stopwatch.createUnstarted();
    if (logger.isDebugEnabled()) {
      stopwatch.start();
    }

    GetSecretValueRequest secretValueRequest =
        GetSecretValueRequest.builder().secretId(secretArn).versionStage(AWS_CURRENT).build();

    try (final SecretsManagerClient secretsManagerClient = buildSecretsManagerClient(region)) {
      final GetSecretValueResponse secretValueResponse =
          secretsManagerClient.getSecretValue(secretValueRequest);

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Retrieve AWS Secrets Manager secret {} took {} ms",
            secretArn,
            stopwatch.elapsed(TimeUnit.MILLISECONDS));
      }

      return (secretValueResponse.secretString() != null)
          ? secretValueResponse.secretString()
          : secretValueResponse.secretBinary().toString();
    } catch (SdkException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Failed to retrieve AWS Secrets Manager secret {} after {} ms. Reason: {}",
            secretArn,
            stopwatch.elapsed(TimeUnit.MILLISECONDS),
            e.getMessage());
      }

      throw new CredentialsException(e.getMessage(), e);
    }
  }

  @VisibleForTesting
  SecretsManagerClient buildSecretsManagerClient(final String region) {
    /*
     * Currently, dremio would support access of the secrets manager with base role assigned
     * to EC2 machine. This will be further enhanced, once we have more requirements on it.
     */
    AwsCredentialsProvider awsCredentialsProvider = new SharedInstanceProfileCredentialsProvider();
    return SecretsManagerClient.builder()
        .region(Region.of(region))
        .credentialsProvider(awsCredentialsProvider)
        .build();
  }

  /*
   * Do some preliminary format check.
   * ARN format : "arn:aws:secretsmanager:<region>:<account-id-number>:secret:secret_name"
   */
  private static boolean isValidARN(String[] arnTokens) {
    return arnTokens.length == 7
        && arnTokens[Arn.arn.ordinal()].equals(ARN)
        && arnTokens[Arn.aws.ordinal()].equals(AWS)
        && arnTokens[Arn.secretsManager.ordinal()].equals(SECRETS_MANAGER)
        && arnTokens[Arn.secret.ordinal()].equals(SECRET);
  }

  /*
   * Aws secret manager secrets can be stored in json or plain text.
   * First, try parsing the json object and if it fails, return the secret as it is.
   */
  private static String extractCredentials(String secret) {
    try {
      return MAPPER.readValue(secret, PasswordCredentials.class).getPassword();
    } catch (IOException e) {
      logger.debug("Secret might not be in json format, Returning secret as is");
    }
    return secret;
  }

  /** Password credentials contains only password. */
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
