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

import javax.inject.Inject;

import com.google.common.base.Preconditions;

/**
 * CredentialsService is responsible for creating and managing the secret vaults.
 * It also exposes api to get the credentials from the secret vaults.
 */
public class CredentialsService {

  /**
   * Configuration required for all vaults will be passed on, as part of constructor.
   * For now, no configuration is required to instantiate vaults.
   */
  private final VaultFactory vaultFactory;
  @Inject
  public CredentialsService() {
    vaultFactory = new VaultFactory();
    vaultFactory.initialize();
  }

  public Credentials getCredentials(URI secretURI) throws IOException {
    Preconditions.checkArgument(secretURI != null, "Invalid secretURI passed");
    String scheme = secretURI.getScheme();
    Preconditions.checkArgument(scheme != null && !scheme.isEmpty(), "Invalid scheme passed");
    Vault vault = vaultFactory.getVault(scheme);
    return vault.getCredentials(secretURI);
  }
}
