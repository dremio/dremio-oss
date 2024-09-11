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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.services.credentials.CredentialsException;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

class AWSSecretsManagerCredentialsProviderTest {
  private AWSSecretsManagerCredentialsProvider credentialsProvider;

  @BeforeEach
  public void setUp() {
    this.credentialsProvider = new AWSSecretsManagerCredentialsProvider();
  }

  @ParameterizedTest
  @MethodSource("provideSecretArns")
  public void isSupported(final String givenSecretArn, boolean expectedIsSupported) {
    assertEquals(expectedIsSupported, credentialsProvider.isSupported(URI.create(givenSecretArn)));
  }

  private static Stream<Arguments> provideSecretArns() {
    return Stream.of(
        Arguments.of("arn:aws:secretsmanager:us-west-2:1234567890:secret:mysql-test", true),
        Arguments.of(
            "arn:aws:secretsmanager:us-east-1:1234567890:secret:secret-name/dremio-user-abdcef",
            true),
        Arguments.of("hashicorp-vault+kv-v1:///path/to/secret", false),
        Arguments.of("https://example.com", false));
  }

  @ParameterizedTest
  @MethodSource("provideSuccessfulLookupFilePaths")
  public void successfulLookup(final String givenSecretValueFileName, final String expectedValue)
      throws IOException, CredentialsException {
    AWSSecretsManagerCredentialsProvider mockCredentialsProvider = spy(credentialsProvider);
    SecretsManagerClient secretsManagerClient = mock(SecretsManagerClient.class);
    doReturn(secretsManagerClient).when(mockCredentialsProvider).buildSecretsManagerClient(any());

    final String secretValue =
        getResourceAsString(
            "security/aws-secrets-manager-secret-values/" + givenSecretValueFileName);
    final GetSecretValueResponse response = mock(GetSecretValueResponse.class);
    final String secretArn =
        "arn:aws:secretsmanager:us-west-2:1234567890:secret:test-secret-WKIa9v";
    when(secretsManagerClient.getSecretValue(
            GetSecretValueRequest.builder().secretId(secretArn).versionStage("AWSCURRENT").build()))
        .thenReturn(response);
    when(response.secretString()).thenReturn(secretValue);

    assertEquals(expectedValue, mockCredentialsProvider.lookup(URI.create(secretArn)));
    verify(mockCredentialsProvider).buildSecretsManagerClient("us-west-2");
  }

  private static Stream<Arguments> provideSuccessfulLookupFilePaths() {
    return Stream.of(
        Arguments.of("plaintext-value.txt", "password-value"),
        Arguments.of("password-key-only.json", "password-value"),
        Arguments.of("password-key-other-keys.json", "password123"));
  }

  @ParameterizedTest
  @MethodSource("provideBadSecretValueFiles")
  public void lookupMissingPasswordKey(final String givenSecretValueFileName) throws IOException {
    AWSSecretsManagerCredentialsProvider mockCredentialsProvider = spy(credentialsProvider);
    SecretsManagerClient secretsManagerClient = mock(SecretsManagerClient.class);
    doReturn(secretsManagerClient).when(mockCredentialsProvider).buildSecretsManagerClient(any());

    final String secretValue =
        getResourceAsString(
            "security/aws-secrets-manager-secret-values/" + givenSecretValueFileName);
    final GetSecretValueResponse response = mock(GetSecretValueResponse.class);
    final String secretArn =
        "arn:aws:secretsmanager:us-west-2:1234567890:secret:test-secret-WKIa9v";
    when(secretsManagerClient.getSecretValue(
            GetSecretValueRequest.builder().secretId(secretArn).versionStage("AWSCURRENT").build()))
        .thenReturn(response);
    when(response.secretString()).thenReturn(secretValue);

    assertThrows(
        CredentialsException.class, () -> mockCredentialsProvider.lookup(URI.create(secretArn)));
  }

  private static Stream<Arguments> provideBadSecretValueFiles() {
    return Stream.of(
        Arguments.of("no-password-key.json"),
        Arguments.of("password-key-wrong-case.json"),
        Arguments.of("password-empty-value.json"));
  }

  /** Load the contents of a resource file into a string. */
  public static String getResourceAsString(String resourceFilePath) throws IOException {
    URL url = Resources.getResource(resourceFilePath);
    Preconditions.checkNotNull(url, "Unable to find resource file %s.", resourceFilePath);

    try (InputStream is = url.openStream()) {
      return new BufferedReader(new InputStreamReader(is))
          .lines()
          .collect(Collectors.joining("\n"));
    }
  }
}
