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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.services.credentials.CredentialsService;
import org.junit.Before;
import org.junit.Test;

public class TestAzureStorageConfBackFillLegacyFormat {

  private CredentialsService credentialsService;

  @Before
  public void before() {
    credentialsService = mock(CredentialsService.class);
    when(credentialsService.isSupported(any())).thenReturn(true);
  }

  @Test
  public void testBackFillLegacyFormat_doesNothingWhenAccessKeyNotAzureKV() {
    final SecretRef originalSecret = SecretRef.of("arn:blarg", credentialsService);

    AzureStorageConf config = new AzureStorageConf();
    config.accessKey = originalSecret;
    config.credentialsType = AzureAuthenticationType.ACCESS_KEY;

    config.backFillLegacyFormat();

    assertEquals(originalSecret, config.accessKey);
    assertNull(config.accessKeyUri);
  }

  @Test
  public void testBackFillLegacyFormat_doesNothingWhenClientSecretNotAzureKV() {
    final SecretRef originalSecret = SecretRef.of("arn:blarg", credentialsService);

    AzureStorageConf config = new AzureStorageConf();
    config.clientSecret = originalSecret;
    config.credentialsType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

    config.backFillLegacyFormat();

    assertEquals(originalSecret, config.clientSecret);
    assertNull(config.clientSecretUri);
  }

  @Test
  public void testEncryptedSecret() {
    final SecretRef originalSecret = SecretRef.of("system:encryptedSecret", credentialsService);

    AzureStorageConf config = new AzureStorageConf();
    config.accessKey = originalSecret;
    config.credentialsType = AzureAuthenticationType.ACCESS_KEY;

    config.backFillLegacyFormat();

    // backfill should do nothing
    assertEquals(originalSecret, config.accessKey);
    assertNull(config.accessKeyUri);
  }

  @Test
  public void testBackFillLegacyFormat_backFillsWhenAccessKeyUsingAzureKV() {
    final SecretRef originalSecret =
        SecretRef.of("azure-key-vault+https://blarg", credentialsService);

    AzureStorageConf config = new AzureStorageConf();
    config.accessKey = originalSecret;
    config.credentialsType = AzureAuthenticationType.ACCESS_KEY;

    config.backFillLegacyFormat();

    assertEquals(originalSecret, config.accessKey);
    assertEquals(
        config.sharedAccessSecretType, SharedAccessSecretType.SHARED_ACCESS_AZURE_KEY_VAULT);
    assertEquals(config.accessKeyUri, "https://blarg");
  }

  @Test
  public void testBackFillLegacyFormat_backFillsWhenClientSecretUsingAzureKV() {
    final SecretRef originalSecret =
        SecretRef.of("azure-key-vault+https://blarg", credentialsService);

    AzureStorageConf config = new AzureStorageConf();
    config.clientSecret = originalSecret;
    config.credentialsType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;

    config.backFillLegacyFormat();

    assertEquals(originalSecret, config.clientSecret);
    assertEquals(
        config.azureADSecretType, AzureActiveDirectorySecretType.AZURE_ACTIVE_DIRECTORY_KEY_VAULT);
    assertEquals(config.clientSecretUri, "https://blarg");
  }
}
