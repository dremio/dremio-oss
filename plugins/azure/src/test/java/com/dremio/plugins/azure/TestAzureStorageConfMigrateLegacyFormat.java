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
package com.dremio.plugins.azure;

import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.services.credentials.CredentialsService;
import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestAzureStorageConfMigrateLegacyFormat {
  @Test
  public void testMigrateLegacyFormat_prioritizesChangedAccessKey_sourceUpdate() {
    final SecretRef expectedSecret = SecretRef.of("newSecret");

    AzureStorageConf originalConfig = new AzureStorageConf();
    originalConfig.accessKey =
        SecretRef.of("azure-key-vault+https://oldSecret", new FakeCredentialsService());
    originalConfig.credentialsType = AzureAuthenticationType.ACCESS_KEY;

    // Create copy of original config and...
    AzureStorageConf newConfig = new AzureStorageConf();
    newConfig.accessKey =
        SecretRef.of("azure-key-vault+https://oldSecret", new FakeCredentialsService());
    newConfig.credentialsType = AzureAuthenticationType.ACCESS_KEY;

    newConfig.backFillLegacyFormat(); // ...simulate user editing existing API response...
    newConfig.accessKey = expectedSecret; // ...with new accessKey added

    // Assert expected format after running back-fill
    Assertions.assertEquals(expectedSecret, newConfig.accessKey);
    Assertions.assertEquals("https://oldSecret", newConfig.accessKeyUri);
    Assertions.assertEquals(
        SharedAccessSecretType.SHARED_ACCESS_AZURE_KEY_VAULT, newConfig.sharedAccessSecretType);

    // Assert expected format after running migrateLegacyFormat
    newConfig.migrateLegacyFormat(originalConfig);

    Assertions.assertEquals(expectedSecret, newConfig.accessKey);
    Assertions.assertNull(newConfig.accessKeyUri);
    Assertions.assertNull(newConfig.sharedAccessSecretType);
  }

  @Test
  public void testMigrateLegacyFormat_prioritizesChangedClientSecret_sourceUpdate() {
    final SecretRef expectedSecret = SecretRef.of("newSecret");

    AzureStorageConf originalConfig = new AzureStorageConf();
    originalConfig.clientSecret =
        SecretRef.of("azure-key-vault+https://oldSecret", new FakeCredentialsService());
    originalConfig.credentialsType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

    // Create copy of original config and...
    AzureStorageConf newConfig = new AzureStorageConf();
    newConfig.clientSecret =
        SecretRef.of("azure-key-vault+https://oldSecret", new FakeCredentialsService());
    newConfig.credentialsType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

    newConfig.backFillLegacyFormat(); // ...simulate user editing existing API response...
    newConfig.clientSecret = expectedSecret; // ...with new clientSecret added

    // Assert expected format after running back-fill
    Assertions.assertEquals(expectedSecret, newConfig.clientSecret);
    Assertions.assertEquals("https://oldSecret", newConfig.clientSecretUri);
    Assertions.assertEquals(
        AzureActiveDirectorySecretType.AZURE_ACTIVE_DIRECTORY_KEY_VAULT,
        newConfig.azureADSecretType);

    // Assert expected format after running migrateLegacyFormat
    newConfig.migrateLegacyFormat(originalConfig);

    Assertions.assertEquals(expectedSecret, newConfig.clientSecret);
    Assertions.assertNull(newConfig.clientSecretUri);
    Assertions.assertNull(newConfig.azureADSecretType);
  }

  @Test
  public void testMigrateLegacyFormat_usesChangedAccessKeyUri_sourceUpdate() {
    AzureStorageConf originalConfig = new AzureStorageConf();
    originalConfig.accessKey = SecretRef.of("oldSecret", new FakeCredentialsService());
    originalConfig.credentialsType = AzureAuthenticationType.ACCESS_KEY;

    // Create copy of original config and...
    AzureStorageConf newConfig = new AzureStorageConf();
    newConfig.accessKey = SecretRef.of("oldSecret", new FakeCredentialsService());
    newConfig.credentialsType = AzureAuthenticationType.ACCESS_KEY;

    newConfig.backFillLegacyFormat(); // ...simulate user editing existing API response...
    newConfig.accessKeyUri = "azure.newSecret"; // ...with new accessKeyUri and legacy ENUM
    newConfig.sharedAccessSecretType = SharedAccessSecretType.SHARED_ACCESS_AZURE_KEY_VAULT;

    // Assert expected format after running migrateLegacyFormat
    newConfig.migrateLegacyFormat(originalConfig);

    Assertions.assertEquals(
        SecretRef.of("azure-key-vault+https://azure.newSecret"), newConfig.accessKey);
    Assertions.assertNull(newConfig.accessKeyUri);
    Assertions.assertNull(newConfig.sharedAccessSecretType);
  }

  @Test
  public void testMigrateLegacyFormat_usesChangedClientSecretUri_sourceUpdate() {
    AzureStorageConf originalConfig = new AzureStorageConf();
    originalConfig.clientSecret = SecretRef.of("oldSecret", new FakeCredentialsService());
    originalConfig.credentialsType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

    // Create copy of original config and...
    AzureStorageConf newConfig = new AzureStorageConf();
    newConfig.clientSecret = SecretRef.of("oldSecret", new FakeCredentialsService());
    newConfig.credentialsType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

    newConfig.backFillLegacyFormat(); // ...simulate user editing existing API response...
    newConfig.clientSecretUri = "azure.newSecret"; // ...with new clientSecretUri and legacy ENUM
    newConfig.azureADSecretType = AzureActiveDirectorySecretType.AZURE_ACTIVE_DIRECTORY_KEY_VAULT;

    // Assert expected format after running migrateLegacyFormat
    newConfig.migrateLegacyFormat(originalConfig);

    Assertions.assertEquals(
        SecretRef.of("azure-key-vault+https://azure.newSecret"), newConfig.clientSecret);
    Assertions.assertNull(newConfig.clientSecretUri);
    Assertions.assertNull(newConfig.azureADSecretType);
  }

  @Test
  public void testMigrateLegacyFormat_doesNothingWhenAccessKeyInUse_sourceCreation() {
    final SecretRef expectedSecret = SecretRef.of("blarg");

    AzureStorageConf config = new AzureStorageConf();
    config.accessKey = expectedSecret;
    config.sharedAccessSecretType = SharedAccessSecretType.SHARED_ACCESS_SECRET_KEY;
    config.credentialsType = AzureAuthenticationType.ACCESS_KEY;

    Assertions.assertFalse(config.migrateLegacyFormat());

    Assertions.assertEquals(expectedSecret, config.accessKey);
    Assertions.assertNull(config.accessKeyUri);
    Assertions.assertNull(config.clientSecret);
  }

  @Test
  public void testMigrateLegacyFormat_doesNothingWhenClientSecretInUse_sourceCreation() {
    final SecretRef expectedSecret = SecretRef.of("blarg");

    AzureStorageConf config = new AzureStorageConf();
    config.clientSecret = expectedSecret;
    config.azureADSecretType = AzureActiveDirectorySecretType.AZURE_ACTIVE_DIRECTORY_SECRET_KEY;
    config.credentialsType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

    Assertions.assertFalse(config.migrateLegacyFormat());

    Assertions.assertEquals(expectedSecret, config.clientSecret);
    Assertions.assertNull(config.clientSecretUri);
    Assertions.assertNull(config.accessKey);
  }

  @ParameterizedTest
  @MethodSource("provideVaultUriIO")
  public void testMigrateLegacyFormat_addsNewFormatWhenAccessKeyUriInUse_sourceCreation(
      String inputVaultUri, String expectedOutputVaultUri) {
    final SecretRef expectedSecret = SecretRef.of(expectedOutputVaultUri);

    AzureStorageConf config = new AzureStorageConf();
    config.accessKeyUri = inputVaultUri;
    config.sharedAccessSecretType = SharedAccessSecretType.SHARED_ACCESS_AZURE_KEY_VAULT;
    config.credentialsType = AzureAuthenticationType.ACCESS_KEY;

    // Assert accessKeyUri repacked into accessKey.
    Assertions.assertTrue(config.migrateLegacyFormat());
    Assertions.assertEquals(expectedSecret, config.accessKey);
    Assertions.assertNull(config.accessKeyUri);
    Assertions.assertNull(config.sharedAccessSecretType);

    // Assert Running a 2nd time returns false.
    Assertions.assertFalse(config.migrateLegacyFormat());
  }

  @ParameterizedTest
  @MethodSource("provideVaultUriIO")
  public void testMigrateLegacyFormat_addsNewFormatWhenClientSecretUriInUse_sourceCreation(
      String inputVaultUri, String expectedOutputVaultUri) {
    final SecretRef expectedSecret = SecretRef.of(expectedOutputVaultUri);

    AzureStorageConf config = new AzureStorageConf();
    config.clientSecretUri = inputVaultUri;
    config.azureADSecretType = AzureActiveDirectorySecretType.AZURE_ACTIVE_DIRECTORY_KEY_VAULT;
    config.credentialsType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

    // Assert clientSecretUri repacked into clientSecret.
    Assertions.assertTrue(config.migrateLegacyFormat());
    Assertions.assertEquals(expectedSecret, config.clientSecret);
    Assertions.assertNull(config.clientSecretUri);
    Assertions.assertNull(config.azureADSecretType);

    // Assert Running a 2nd time returns false.
    Assertions.assertFalse(config.migrateLegacyFormat());
  }

  public static Stream<Arguments> provideVaultUriIO() {
    // Arguments of: input vaultUri, expected output vaultUri
    return Stream.of(
        Arguments.of(
            "dremio+azure-key-vault+https://some.azure/vault",
            "azure-key-vault+https://some.azure/vault"),
        Arguments.of(
            "azure-key-vault+https://some.azure/vault", "azure-key-vault+https://some.azure/vault"),
        Arguments.of("https://some.azure/vault", "azure-key-vault+https://some.azure/vault"),
        Arguments.of("some.azure/vault", "azure-key-vault+https://some.azure/vault"));
  }

  private static final class FakeCredentialsService implements CredentialsService {
    @Override
    public String lookup(String pattern) {
      return pattern;
    }

    @Override
    public boolean isSupported(URI uri) {
      return true;
    }

    @Override
    public void start() {}

    @Override
    public void close() {}
  }
}
