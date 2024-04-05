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
import javax.inject.Inject;

/**
 * The default, primary cipher for all out-of-the-box secrets across Dremio (i.e. in-line source
 * secret encryption, first-class managed secrets, etc.) These secrets live on and can only be
 * accessed directly via the master coordinator.
 */
public class SystemCipher extends AbstractCipher {

  private final DremioConfig config;
  private final CredentialsService credentialsService;

  private static final String KEYSTORE_FILENAME = "system.p12";

  @Inject
  public SystemCipher(DremioConfig config, CredentialsService credentialsService) {
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
    return SystemSecretCredentialsProvider.SECRET_PROVIDER_SCHEME;
  }
}
