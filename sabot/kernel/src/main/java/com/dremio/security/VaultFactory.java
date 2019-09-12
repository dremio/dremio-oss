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
import java.util.HashMap;
import java.util.Map;

/**
 * VaultFactory initializes all the vaults.
 */
public final class VaultFactory {

  public enum VaultScheme {
    arn
  }

  private final Map<VaultScheme, Vault> vaultMap = new HashMap<>();

  /*
   * Extensible to pass the configuration and vaults will be initialised
   * with respective configuration.
   */
  void initialize() {
    createAwsSecretManagerVault();
  }

  private void createAwsSecretManagerVault() {
    vaultMap.put(VaultScheme.arn, new AwsSecretsManager());
  }

  Vault getVault(String vaultScheme) throws IOException {
    VaultScheme vtype;
    try {
      vtype = VaultScheme.valueOf(vaultScheme);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid vault type encountered", e);
    }
    return vaultMap.get(vtype);
  }
}
