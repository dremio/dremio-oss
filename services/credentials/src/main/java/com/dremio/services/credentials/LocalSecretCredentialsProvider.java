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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.google.inject.Inject;

/**
 * Secrets provider for @{link com.dremio.dac.cmd.Encrypt} command secrets that decrypts on lookuo.
 * "Local" since these secrets live exclusively on the node they were encrypted on. Handles the
 * lifecycle of the local secrets store.
 */
public class LocalSecretCredentialsProvider extends AbstractSimpleCredentialsProvider implements CredentialsProvider {

  public static final String SECRET_PROVIDER_SCHEME = "secret";

  private final LocalCipher localSecretsStore;

  /**
   * Only use it for standalone programs or in test contexts.
   */
  public static LocalSecretCredentialsProvider of(DremioConfig config, ScanResult scanResult) {
    SimpleCredentialsService service = SimpleCredentialsService.newInstance(config, scanResult);
    return Objects.requireNonNull(service.findProvider(LocalSecretCredentialsProvider.class));
  }

  @Inject
  public LocalSecretCredentialsProvider(DremioConfig config, CredentialsService credentialsService) {
    super(SECRET_PROVIDER_SCHEME);
    this.localSecretsStore = new LocalCipher(config, credentialsService);
  }

  @Override
  protected String doLookup(URI uri) throws CredentialsException {
    return localSecretsStore.decrypt(uri.getSchemeSpecificPart());
  }

  /**
   * Encrypts a secret
   *
   * @param secret the secret to encrypt
   * @return the secret URI
   * @throws SecretCredentialsException if the secret cannot be encrypted
   */
  public URI encrypt(String secret) throws CredentialsException {
    try {
      return new URI("secret", localSecretsStore.encrypt(secret), null);
    } catch (URISyntaxException e) {
      throw new SecretCredentialsException("Cannot encode secret into an URI");
    }
  }

}
