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

import com.dremio.common.DeferredException;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.Service;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The primary {@link CredentialsService} implementation for all nodes. Handles routing between
 * leader and non-leader nodes. Handles the life cycle of underlying {@link CredentialsProvider}'s.
 */
public final class CredentialsServiceImpl implements CredentialsService {

  private static final Logger logger = LoggerFactory.getLogger(CredentialsServiceImpl.class);

  private final Set<CredentialsProvider> providers;

  /** Internal for Guice Injection */
  @Inject
  private CredentialsServiceImpl(final Set<CredentialsProvider> providers) {
    this.providers = ImmutableSet.copyOf(Preconditions.checkNotNull(providers));
  }

  @Override
  public void start() throws Exception {
    for (CredentialsProvider provider : providers) {
      if (!(provider instanceof Service)) {
        continue;
      }
      ((Service) provider).start();
    }
  }

  @Override
  public void close() throws Exception {
    try (final DeferredException deferred = new DeferredException()) {
      for (CredentialsProvider provider : providers) {
        if (!(provider instanceof AutoCloseable)) {
          continue;
        }
        try {
          ((AutoCloseable) provider).close();
        } catch (Exception e) {
          deferred.addException(e);
        }
      }
    }
  }

  @Override
  public String lookup(String pattern) throws IllegalArgumentException, CredentialsException {
    URI uri;
    try { // backward compatibility for uri-like patterns
      uri = CredentialsServiceUtils.safeURICreate(pattern);
    } catch (IllegalArgumentException e) {
      logger.debug("Defaulting to original pattern", e);
      return pattern;
    }
    final String scheme = uri.getScheme();
    // backward compatibility for plaintext used in the ldap bindPassword field
    if (scheme == null) {
      return pattern;
    }
    for (CredentialsProvider provider : providers) {
      if (provider.isSupported(uri)) {
        logger.trace("Credentials lookup using {}", provider.getClass());
        return provider.lookup(uri);
      }
    }
    throw new UnsupportedOperationException(
        "Unable to find a suitable credentials provider for " + scheme);
  }

  @Override
  public boolean isSupported(URI uri) {
    if (uri == null) {
      return false;
    }
    for (CredentialsProvider provider : providers) {
      if (provider.isSupported(uri)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Find a credential provider by class
   *
   * @param <C> the credential provider type
   * @param clazz the credential provider
   * @return the credential provider if it exists, or {@code null} otherwise
   */
  @VisibleForTesting
  public <C extends CredentialsProvider> C findProvider(Class<C> clazz) {
    for (CredentialsProvider provider : providers) {
      if (clazz.isInstance(provider)) {
        return clazz.cast(provider);
      }
    }
    return null;
  }

  /**
   * Create a CredentialsService for admin commands and tests. This CredentialsService cannot make
   * remote calls. TODO: Pull this out of this class and not specific to the Impl
   */
  public static CredentialsServiceImpl newInstance(DremioConfig config, ScanResult result) {
    return newInstance(config, result.getImplementations(CredentialsProvider.class));
  }

  @VisibleForTesting
  public static CredentialsServiceImpl newInstance(
      DremioConfig config, Set<Class<? extends CredentialsProvider>> providers) {
    final Injector injector =
        Guice.createInjector(
            new CredentialsProviderModule(config, providers), new SystemCipherModule());
    return injector.getInstance(CredentialsServiceImpl.class);
  }

  /** For DacDaemonModule */
  public static CredentialsService newInstance(
      DremioConfig config,
      ScanResult result,
      Provider<OptionManager> optionManager,
      Provider<Cipher> cipher,
      Provider<ManagedChannel> channelToLeader) {
    final Injector injector =
        Guice.createInjector(
            new CredentialsProviderModule(
                config, result.getImplementations(CredentialsProvider.class)),
            new CredentialsProviderContextModule(config, optionManager, channelToLeader),
            new CipherModule(cipher));
    return injector.getInstance(CredentialsService.class);
  }
}
