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
import com.dremio.options.OptionManager;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Names;
import io.grpc.ManagedChannel;
import java.util.function.BooleanSupplier;
import javax.inject.Provider;

/**
 * A supplementary Module for binding {@link CredentialsProviderContext} that is necessary for
 * remote calls.
 */
final class CredentialsProviderContextModule extends AbstractModule {
  private final DremioConfig config;
  private final Provider<OptionManager> optionManager;
  private final Provider<ManagedChannel> channelToLeader;

  CredentialsProviderContextModule(
      DremioConfig config,
      Provider<OptionManager> optionManager,
      Provider<ManagedChannel> channelToLeader) {
    this.config = config;
    this.optionManager = optionManager;
    this.channelToLeader = channelToLeader;
  }

  @Provides
  @Leader
  public BooleanSupplier getLeader() {
    return () ->
        config.getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL)
            && config.getBoolean(DremioConfig.ENABLE_MASTER_BOOL);
  }

  @Provides
  @RemoteEnabled
  public BooleanSupplier getRemoteEnabled() {
    return () -> CredentialsServiceUtils.isRemoteLookupEnabled(optionManager.get());
  }

  @Override
  protected void configure() {
    bind(ManagedChannel.class)
        .annotatedWith(Names.named("channelToLeader"))
        .toProvider(channelToLeader);
    OptionalBinder.newOptionalBinder(binder(), CredentialsProviderContext.class)
        .setBinding()
        .to(CredentialsProviderContextImpl.class);
  }
}
