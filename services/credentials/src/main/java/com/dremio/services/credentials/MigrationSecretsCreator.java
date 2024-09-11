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
import java.util.Optional;

/**
 * SecretsCreator for migration required to support SecretRef, encrypted secrets, and combined
 * plaintext and secret uri fields (24.x to 25.x). If secret is already isEncrypted, avoid double
 * encryption. Otherwise, encodes the secret as a data credential (regardless of its status as a uri
 * or plaintext) and passes to the delegate for encryption.
 */
public class MigrationSecretsCreator implements SecretsCreator {

  private final SecretsCreator delegate;

  public MigrationSecretsCreator(SecretsCreator delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean isEncrypted(String secret) {
    return delegate.isEncrypted(secret);
  }

  @Override
  public Optional<URI> encrypt(String secret) throws CredentialsException {
    // Encrypt everything that isn't already encrypted.
    try {
      final URI uri = CredentialsServiceUtils.safeURICreate(secret);
      // Check if already encrypted. isSupport is redundant but helps avoid extra decrypt call in
      // isEncrypted
      if (CredentialsServiceUtils.isEncryptedCredentials(uri)) {
        if (isEncrypted(uri.getSchemeSpecificPart())) {
          return Optional.empty();
        }
      }
    } catch (IllegalArgumentException ignored) {
      // Not a URI
    }
    return delegate.encrypt(CredentialsServiceUtils.encodeAsDataURI(secret).toString());
  }
}
