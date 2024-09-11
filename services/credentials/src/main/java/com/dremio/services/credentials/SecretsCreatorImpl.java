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

import com.google.common.base.Stopwatch;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The primary SecretsCreator implementation for in-line encryption on master/leader coordinator.
 */
public class SecretsCreatorImpl implements SecretsCreator {
  private static final Logger logger = LoggerFactory.getLogger(SecretsCreatorImpl.class);
  private final Provider<Cipher> systemCipher;
  private final Provider<CredentialsService> credentialsService;

  @Inject
  public SecretsCreatorImpl(
      Provider<Cipher> systemCipher, Provider<CredentialsService> credentialsService) {
    this.systemCipher = systemCipher;
    this.credentialsService = credentialsService;
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
  public Optional<URI> encrypt(String secret) throws CredentialsException {
    final Stopwatch watch = Stopwatch.createUnstarted();
    if (logger.isDebugEnabled()) {
      watch.start();
    }
    final Optional<URI> encrypted;
    try {
      encrypted = doEncrypt(secret);
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Secret encryption failed after {} ms", watch.elapsed(TimeUnit.MILLISECONDS));
      }
      throw e;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Secret encryption took {} ms", watch.elapsed(TimeUnit.MILLISECONDS));
    }
    return encrypted;
  }

  public Optional<URI> doEncrypt(String secret) throws CredentialsException {
    // If URI, check if data credentials
    URI uri;
    try {
      uri = CredentialsServiceUtils.safeURICreate(secret);
      // If data credentials, resolve and then encrypt
      if (CredentialsServiceUtils.isDataCredentials(uri)) {
        final String encryptMe = CredentialsServiceUtils.decodeDataURI(uri);
        return Optional.of(encryptWithCipher(encryptMe));
      }
    } catch (IllegalArgumentException e) {
      uri = null;
    }

    // If secret is a URI, has a scheme, and is supported by CredentialsService, do not encrypt
    if (uri != null && uri.getScheme() != null && credentialsService.get().isSupported(uri)) {
      return Optional.empty();
    }
    return Optional.of(encryptWithCipher(secret));
  }

  private URI encryptWithCipher(String secret) throws CredentialsException {
    try {
      return new URI(SECRET_PROVIDER_SCHEME, systemCipher.get().encrypt(secret), null);
    } catch (URISyntaxException e) {
      throw new CredentialsException("Cannot encode secret into an URI");
    }
  }
}
