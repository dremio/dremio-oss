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
package com.dremio.services.credentials;

import com.dremio.config.DremioConfig;

/**
 * Local Cipher for {@link com.dremio.dac.cmd.Encrypt} command secrets. These secrets are manually
 * encrypted and strictly limited (locally) to the node they were encrypted on.
 */
public class LocalCipher extends AbstractCipher {
  private final DremioConfig config;
  private final CredentialsService credentialsService;

  private static final String KEYSTORE_FILENAME = "credentials.p12";

  public LocalCipher(DremioConfig config, CredentialsService credentialsService) {
    this.config = config;
    this.credentialsService = credentialsService;
  }

  @Override
  public DremioConfig getConfig() {
    return config;
  }

  @Override
  public CredentialsService getCredentialsService() {
    return credentialsService;
  }

  @Override
  public String getKeystoreFilename() {
    return KEYSTORE_FILENAME;
  }

  @Override
  protected String getScheme() {
    return LocalSecretCredentialsProvider.SECRET_PROVIDER_SCHEME;
  }
}
