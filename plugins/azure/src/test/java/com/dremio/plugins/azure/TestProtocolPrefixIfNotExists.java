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

import org.junit.Assert;
import org.junit.Test;

public class TestProtocolPrefixIfNotExists {

  @Test
  public void testProtocolPrefix1() {
    String input = "dremio+azure-key-vault+https://<vault>/secrets/<name>";
    final String updatedStringSA = testProtocolPrefixForSharedAccessKey(input);
    Assert.assertEquals(updatedStringSA, input);
    final String updatedStringAAD = testProtocolPrefixForActiveDirectory(input);
    Assert.assertEquals(updatedStringAAD, input);
  }

  @Test
  public void testProtocolPrefix2() {
    String input = "https://<vault>/secrets/<name>";
    final String updatedStringSA = testProtocolPrefixForSharedAccessKey(input);
    Assert.assertEquals(updatedStringSA, input);
    final String updatedStringAAD = testProtocolPrefixForActiveDirectory(input);
    Assert.assertEquals(updatedStringAAD, input);
  }

  @Test
  public void testProtocolPrefix3() {
    String input = "HtTpS://<vault>/secrets/<name>";
    String updatedStringSA = testProtocolPrefixForSharedAccessKey(input);
    Assert.assertEquals(updatedStringSA, input);
    final String updatedStringAAD = testProtocolPrefixForActiveDirectory(input);
    Assert.assertEquals(updatedStringAAD, input);
  }

  @Test
  public void testProtocolPrefix4() {
    String input = "HtTpS<vault>/secrets/<name>";
    String updatedStringSA = testProtocolPrefixForSharedAccessKey(input);
    Assert.assertEquals(updatedStringSA, "https://HtTpS<vault>/secrets/<name>");
    final String updatedStringAAD = testProtocolPrefixForActiveDirectory(input);
    Assert.assertEquals(updatedStringAAD, "https://HtTpS<vault>/secrets/<name>");
  }

  @Test
  public void testProtocolPrefix5() {
    String input = "<vault>/secrets/<name>";
    final String updatedStringSA = testProtocolPrefixForSharedAccessKey(input);
    Assert.assertEquals(updatedStringSA, "https://<vault>/secrets/<name>");
    final String updatedStringAAD = testProtocolPrefixForActiveDirectory(input);
    Assert.assertEquals(updatedStringAAD, "https://<vault>/secrets/<name>");
  }

  /**
   * Tests the prependProtocolIfNotExist method injected inside the getter method for Shared Access
   * Key Vault URI
   *
   * @return the AzureStorageConf API Field 'accessKeyUri' with the prepended protocol (If not
   *     already present) Parameter - vaultUri: Vault URI raw user input
   */
  private String testProtocolPrefixForSharedAccessKey(String vaultUri) {
    AzureStorageConf config = new AzureStorageConf();
    config.accessKeyUri = vaultUri;
    return config.getAccessKeyUri();
  }

  /**
   * Tests the prependProtocolIfNotExist method injected inside the getter method for Azure Active
   * Directory Vault URI
   *
   * @return the AzureStorageConf API Field 'clientSecretUri' with the prepended protocol (If not
   *     already present) parameter - vaultUri: Vault URI raw user input
   */
  private String testProtocolPrefixForActiveDirectory(String vaultUri) {
    AzureStorageConf config = new AzureStorageConf();
    config.clientSecretUri = vaultUri;
    return config.getClientSecretUri();
  }
}
