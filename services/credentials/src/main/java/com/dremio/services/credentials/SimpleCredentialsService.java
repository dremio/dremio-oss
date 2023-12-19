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
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.DeferredException;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.service.Service;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

/**
 * CredentialsProvider management.
 */
public class SimpleCredentialsService implements CredentialsService {
  private static final Logger logger = LoggerFactory.getLogger(SimpleCredentialsService.class);


  static SimpleCredentialsService newInstance(DremioConfig config, ScanResult result) {
    return newInstance(config, result.getImplementations(CredentialsProvider.class));
  }

  /**
   * Create a new credentials service using the provided credentials provider classes
   *
   * @param config the Dremio configuration
   * @param providerClasses a set of credentials provider class to use
   * @return the credentials service instance
   */
  @VisibleForTesting
  public static SimpleCredentialsService newInstance(DremioConfig config, Set<Class<? extends CredentialsProvider>> providerClasses) {
    Injector injector = Guice.createInjector(new CredentialsProviderModule(config, providerClasses));
    return injector.getInstance(SimpleCredentialsService.class);
  }

  private final Set<CredentialsProvider> providers;

  @Inject
  SimpleCredentialsService(Set<CredentialsProvider> providers) {
    this.providers = ImmutableSet.copyOf(providers);
  }

  @Override
  public void start() throws Exception {
    for (CredentialsProvider provider: providers) {
      if (!(provider instanceof Service)) {
        continue;
      }

      ((Service) provider).start();
    }

  }

  @Override
  public void close() throws Exception {
    try (final DeferredException deferred = new DeferredException()) {
      for (CredentialsProvider provider: providers) {
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
    URI uri = CredentialsServiceUtils.safeURICreate(pattern);
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
    throw new UnsupportedOperationException("Unable to find a suitable credentials provider for " + scheme);
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
        return (C) provider;
      }
    }

    return null;
  }

  /**
   * Guice module that provides bindings for config and contents used in
   * {@link CredentialsProvider}.
   */
  private static final class CredentialsProviderModule extends AbstractModule {
    private final DremioConfig config;
    private final Set<Class<? extends CredentialsProvider>> providerClasses;

    private CredentialsProviderModule(DremioConfig config, Set<Class<? extends CredentialsProvider>> providerClasses) {
      this.config = config;
      this.providerClasses = providerClasses;
    }

    @Override
    protected void configure() {
      bind(DremioConfig.class).toInstance(config);
      bind(CredentialsService.class).to(SimpleCredentialsService.class).in(Scopes.SINGLETON);
      Multibinder<CredentialsProvider> providerBinder = Multibinder.newSetBinder(binder(), CredentialsProvider.class);
      for (Class<? extends CredentialsProvider> credentialsProvider : providerClasses) {
        providerBinder.addBinding().to(credentialsProvider);
      }
    }
  }
}
