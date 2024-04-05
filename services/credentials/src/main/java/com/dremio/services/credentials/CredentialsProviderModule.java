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
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import java.util.Set;

/**
 * Primary Guice Module for configuring {@link CredentialsProvider}. Contains minimum binding
 * necessary for a usable CredentialsService. Establishes {@link CredentialsProviderContext} and
 * {@link Cipher} as optional binds, defaulting context to NoOp and Cipher to an internally managed
 * SystemCipher.
 */
final class CredentialsProviderModule extends AbstractModule {
  private final DremioConfig config;
  private final Set<Class<? extends CredentialsProvider>> providers;

  CredentialsProviderModule(
      DremioConfig config, Set<Class<? extends CredentialsProvider>> providers) {
    this.config = config;
    this.providers = providers;
  }

  @Override
  protected void configure() {
    bind(DremioConfig.class).toInstance(config);
    bind(CredentialsService.class).to(CredentialsServiceImpl.class).in(Scopes.SINGLETON);
    OptionalBinder.newOptionalBinder(binder(), CredentialsProviderContext.class)
        .setDefault()
        .to(NoopCredentialsProviderContext.class);
    OptionalBinder.newOptionalBinder(binder(), Cipher.class).setDefault().to(NoopCipher.class);
    Multibinder<CredentialsProvider> providerBinder =
        Multibinder.newSetBinder(binder(), CredentialsProvider.class);
    for (Class<? extends CredentialsProvider> credentialsProvider : providers) {
      providerBinder.addBinding().to(credentialsProvider);
    }
  }
}
