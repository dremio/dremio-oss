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
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.CredentialsServiceUtils;
import com.dremio.services.credentials.SecretsCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;

/**
 * A container for a secret. Useful to receives custom Protostuff ser/de logic and ensure compliance
 * with source-level, in-line secret encryption.
 */
@FunctionalInterface
@JsonDeserialize(as = SecretRefImpl.class)
public interface SecretRef extends Supplier<String> {
  /**
   * The empty instance, acts roughly equivalent to an empty string. Allowed to freely retrieve the
   * empty value.
   */
  SecretRef EMPTY =
      new SecretRef() {
        @Override
        public String get() {
          return "";
        }

        @Override
        public boolean equals(Object obj) {
          if (obj instanceof AbstractSecretRef) {
            return ((AbstractSecretRef) obj).getRaw().isEmpty();
          } else if (obj instanceof SecretRef) {
            return ((SecretRef) obj).get().isEmpty();
          }
          return super.equals(obj);
        }

        @Override
        public int hashCode() {
          return "".hashCode();
        }
      };

  SecretRef EXISTING_VALUE =
      new SecretRef() {
        @Override
        public String get() {
          return ConnectionConf.USE_EXISTING_SECRET_VALUE;
        }

        @Override
        public boolean equals(Object obj) {
          if (obj instanceof AbstractSecretRef) {
            return ConnectionConf.USE_EXISTING_SECRET_VALUE.equals(
                ((AbstractSecretRef) obj).getRaw());
          } else if (obj instanceof SecretRef) {
            return ConnectionConf.USE_EXISTING_SECRET_VALUE.equals(((SecretRef) obj).get());
          }
          return super.equals(obj);
        }

        @Override
        public int hashCode() {
          return ConnectionConf.USE_EXISTING_SECRET_VALUE.hashCode();
        }
      };

  /** Get the resolved Secret */
  @Override
  String get();

  /** Is the SecretRef empty */
  static boolean isEmpty(SecretRef secretRef) {
    return secretRef.equals(EMPTY);
  }

  /** Get an empty SecretRef */
  static SecretRef empty() {
    return EMPTY;
  }

  /** Is the SecretRef null or empty */
  static boolean isNullOrEmpty(SecretRef secretRef) {
    return secretRef == null || isEmpty(secretRef);
  }

  /** Create a SecretRef out of given secret */
  static SecretRef of(String secret, CredentialsService credentialsService) {
    if (secret == null) {
      return null;
    } else if (secret.isEmpty()) {
      return EMPTY;
    } else if (USE_EXISTING_SECRET_VALUE.equals(secret)) {
      return EXISTING_VALUE;
    }
    return new SecretRefImpl(secret).decorateSecrets(credentialsService);
  }

  /** Create a SecretRef out of given secret and decorate it with given CredentialsService */
  @VisibleForTesting
  static SecretRef of(String secret) {
    return SecretRef.of(secret, null);
  }

  /**
   * Retrieve the safe String representation of SecretRef. Returns URIs or redacted plaintext
   * secrets. This helper should NEVER perform SecretRef resolution and should be safe to use even
   * if the input SecretRef is not decorated with CredentialsService.
   */
  static String getDisplayString(SecretRef input) {
    if (input == null) {
      return null;
    }
    if (SecretRef.isEmpty(input)) {
      return SecretRef.EMPTY.get();
    }
    if (SecretRef.EXISTING_VALUE.equals(input) || !(input instanceof RepresentableByURI)) {
      return SecretRef.EXISTING_VALUE.get();
    }

    final URI uri = ((RepresentableByURI) input).getURI();

    // Mask plaintext or encrypted secrets
    if (uri == null || CredentialsServiceUtils.isEncryptedCredentials(uri)) {
      return SecretRef.EXISTING_VALUE.get();
    }

    return uri.toString();
  }

  /**
   * Extracts the URI form of this SecretRef iff this SecretRef can be represented by a URI.
   * Otherwise, returns null. Encrypted secrets are considered URIs for this purpose.
   */
  static URI getURI(SecretRef input) {
    if (!(input instanceof RepresentableByURI)) {
      return null;
    }
    return ((RepresentableByURI) input).getURI();
  }

  /**
   * Encrypt the secret found inside this SecretRef if supported. This mutates the SecretRef and is
   * NOT idempotent; invoke with caution.
   *
   * @param secretsCreator a SecretCreator that wil always encrypt the password by the system
   * @return true if any secret(s) have been encrypted. False if no plain-text secret to encrypt and
   *     no error occurs.
   */
  static boolean encrypt(SecretRef input, SecretsCreator secretsCreator)
      throws CredentialsException {
    if (!(input instanceof Encryptable)) {
      return false;
    }
    return ((Encryptable) input).encrypt(secretsCreator);
  }

  /**
   * Extracts representation to use in property maps (ie hadoop configuration). Both encrypted
   * secrets, and vault references are retrieved as URIs, otherwise resolved secret value is used.
   * Adds configuration specific prefix.
   */
  static String toConfiguration(SecretRef input, String prefix) {
    if (isNullOrEmpty(input) || SecretRef.EXISTING_VALUE.equals(input)) {
      return "";
    }

    final URI uri = getURI(input);

    if (uri != null) {
      return StringUtils.prependIfMissingIgnoreCase(uri.toString(), prefix);
    }

    return input.get();
  }
}
