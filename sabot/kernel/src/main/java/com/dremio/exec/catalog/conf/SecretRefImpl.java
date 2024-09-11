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
package com.dremio.exec.catalog.conf;

import static com.dremio.exec.catalog.conf.ConnectionConf.USE_EXISTING_SECRET_VALUE;

import com.dremio.services.credentials.CredentialsException;
import com.dremio.services.credentials.SecretsCreator;
import com.google.common.base.Strings;
import java.net.URI;
import java.util.Optional;

/**
 * The primary, default SecretRef. Custom Protostuff ser/de logic is provided by {@link
 * SecretRefImplDelegate}. Secrets retrieved from SecretRefProto must be resolved with
 * CredentialsService.
 */
public class SecretRefImpl extends AbstractSecretRef implements Encryptable {

  public SecretRefImpl(String secret) {
    super(secret);
  }

  @Override
  public String get() {
    if (getCredentialsService() == null) {
      throw new IllegalArgumentException("CredentialsService not available to lookup secret");
    }
    try {
      return getCredentialsService().lookup(secret);
    } catch (CredentialsException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param secretsCreator a SecretCreator that wil always encrypt the password by the system
   * @return true if any secret(s) have been encrypted. False if no plain-text secret to encrypt and
   *     no error occurs.
   */
  @Override
  public synchronized boolean encrypt(SecretsCreator secretsCreator) throws CredentialsException {
    if (Strings.isNullOrEmpty(secret) || USE_EXISTING_SECRET_VALUE.equals(secret)) {
      return false;
    }
    final Optional<URI> encrypted = secretsCreator.encrypt(secret);
    encrypted.ifPresent(value -> this.secret = value.toString());
    return encrypted.isPresent();
  }
}
