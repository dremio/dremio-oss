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

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.services.credentials.proto.CredentialsServiceGrpc.CredentialsServiceBlockingStub;
import com.dremio.services.credentials.proto.LookupRequest;
import com.dremio.services.credentials.proto.LookupResponse;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import io.grpc.StatusRuntimeException;

/**
 * Credentials Service for executors
 */
@Options
public class ExecutorCredentialsService extends SimpleCredentialsService {
  private static final Logger logger = LoggerFactory.getLogger(ExecutorCredentialsService.class);
  public static final TypeValidators.BooleanValidator REMOTE_LOOKUP_ENABLED
    = new TypeValidators.BooleanValidator("services.credentials.exec.remote_lookup.enabled", false);

  private final Provider<CredentialsServiceBlockingStub> credentialsServiceBlockingStub;
  private final Provider<OptionManager> optionManager;

  private final Set<CredentialsProvider> providers;

  @Inject
  public ExecutorCredentialsService(
    final Set<CredentialsProvider> providers,
    Provider<CredentialsServiceBlockingStub> credentialsServiceBlockingStub,
    Provider<OptionManager> optionManager
  ) {
    super(providers);
    this.credentialsServiceBlockingStub = credentialsServiceBlockingStub;
    this.providers = providers;
    this.optionManager = optionManager;
  }

  private boolean isRemoteLookupEnabled() {
    return optionManager.get().getOption(REMOTE_LOOKUP_ENABLED);
  }

  @Override
  public String lookup(String pattern) throws CredentialsException {
    URI uri = CredentialsServiceUtils.safeURICreate(pattern);
    final String scheme = uri.getScheme();

    // backward compatibility for plaintext used in the ldap bindPassword field
    if (scheme == null) {
      return pattern;
    }

    for (CredentialsProvider provider : providers) {
      if (provider.isSupported(uri)) {
        if (provider.getClass().isAnnotationPresent(RemoteRestricted.class)) {
          logger.trace("Credentials lookup using {}", provider.getClass());
          return provider.lookup(uri);
        }

        // perform remote lookup if it is enabled
        if (isRemoteLookupEnabled()) {
          logger.trace("Remote credentials lookup using {}", provider.getClass());
          return remoteLookup(pattern);
        } else {
          logger.trace("Credentials lookup using {}", provider.getClass());
          return provider.lookup(uri);
        }
      }
    }
    throw new UnsupportedOperationException("Unable to find a suitable credentials provider for " + scheme);
  }

  private String remoteLookup(final String pattern) throws IllegalArgumentException, CredentialsException {
    try {
      final LookupRequest lookupRequest = LookupRequest.newBuilder()
        .setPattern(pattern)
        .build();
      final LookupResponse lookupResponse = credentialsServiceBlockingStub.get().lookup(lookupRequest);
      return lookupResponse.getSecret();
    } catch (StatusRuntimeException sre) {
      switch (sre.getStatus().getCode()) {
        case INVALID_ARGUMENT:
          throw new IllegalArgumentException(sre);
        case INTERNAL:
          throw new CredentialsException(sre.getMessage(), sre);
        default:
          throw new RuntimeException(sre);
      }
    }
  }

  public static ExecutorCredentialsService newInstance(
    DremioConfig config,
    ScanResult result,
    Provider<CredentialsServiceBlockingStub> credentialsServiceBlockingStub,
    Provider<OptionManager> optionManager
  ) {
    final Injector injector = Guice.createInjector(new CredentialsProviderModule(
      config, result.getImplementations(CredentialsProvider.class), credentialsServiceBlockingStub, optionManager));
    return injector.getInstance(ExecutorCredentialsService.class);
  }

  /**
   * Guice module that provides bindings for config and contents used in
   * {@link CredentialsProvider}.
   */
  private static final class CredentialsProviderModule extends AbstractModule {
    private final DremioConfig config;
    private final Set<Class<? extends CredentialsProvider>> providerClasses;
    private final Provider<CredentialsServiceBlockingStub> credentialsServiceStub;
    private final Provider<OptionManager> optionManager;

    private CredentialsProviderModule(
      DremioConfig config,
      Set<Class<? extends CredentialsProvider>> providerClasses,
      Provider<CredentialsServiceBlockingStub> credentialsServiceStub,
      Provider<OptionManager> optionManager
    ) {
      this.config = config;
      this.providerClasses = providerClasses;
      this.credentialsServiceStub = credentialsServiceStub;
      this.optionManager = optionManager;
    }

    @Override
    protected void configure() {
      bind(DremioConfig.class).toInstance(config);
      bind(CredentialsServiceBlockingStub.class).toProvider(credentialsServiceStub);
      bind(OptionManager.class).toProvider(optionManager);
      bind(CredentialsService.class).to(ExecutorCredentialsService.class).in(Scopes.SINGLETON);
      Multibinder<CredentialsProvider> providerBinder = Multibinder.newSetBinder(binder(), CredentialsProvider.class);
      for (Class<? extends CredentialsProvider> credentialsProvider : providerClasses) {
        providerBinder.addBinding().to(credentialsProvider);
      }
    }
  }
}
