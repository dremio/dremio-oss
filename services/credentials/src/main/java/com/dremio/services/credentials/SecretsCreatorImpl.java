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

import static com.dremio.services.credentials.SystemSecretCredentialsProvider.SECRET_PROVIDER_SCHEME;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 * The primarily SecretsCreator implementation for in-line encryption on master/leader coordinator.
 */
public class SecretsCreatorImpl implements SecretsCreator {
  private final Provider<Cipher> systemCipher;

  @Inject
  public SecretsCreatorImpl(Provider<Cipher> systemCipher) {
    this.systemCipher = systemCipher;
  }

  @Override
  public boolean isEncrypted(String secret) {
    try {
      systemCipher.get().decrypt(secret);
      return true;
    } catch (CredentialsException | IllegalArgumentException | NoSuchElementException e) {
      return false;
    }
  }

  @Override
  public URI encrypt(String secret) throws CredentialsException {
    try {
      return new URI(SECRET_PROVIDER_SCHEME, systemCipher.get().encrypt(secret), null);
    } catch (URISyntaxException e) {
      throw new CredentialsException("Cannot encode secret into an URI");
    }
  }
}
