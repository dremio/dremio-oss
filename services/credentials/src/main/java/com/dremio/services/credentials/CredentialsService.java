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

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.Service;
import com.dremio.service.SingletonRegistry;
import com.dremio.services.credentials.proto.CredentialsServiceGrpc.CredentialsServiceBlockingStub;
import com.google.common.base.Preconditions;

/**
 * CredentialsService operations
 */
public interface CredentialsService extends Service {

  /**
   * Returns the secret from the pattern through loaded providers
   *
   * @param pattern
   * @return a string represents the secret indicated by the pattern
   */
  String lookup(String pattern) throws IllegalArgumentException, CredentialsException;

  /**
   * Return a credentials service with a DremioConfig
   *
   * @param config a valid dremio config
   * @return a credentials service instance
   * @throws NullPointerException if {@code config} is null
   */
  static CredentialsService newInstance(DremioConfig config, ScanResult result) {
    Preconditions.checkNotNull(config, "requires DremioConfig"); // per method contract
    return SimpleCredentialsService.newInstance(config, result);
  };

  /**
   * Return a credentials service with a DremioConfig. For coordinators or hybrid
   * c+e, use {@link SimpleCredentialsService}, otherwise {@link ExecutorCredentialsService}
   * that allows remote lookups.
   *
   * @param config a valid dremio config
   * @param registry DACDaemon registry
   * @return a credentials service instance
   * @throws NullPointerException if {@code config} is null
   */
   static CredentialsService newInstance(DremioConfig config, final SingletonRegistry registry, ScanResult result) {
     Preconditions.checkNotNull(config, "requires DremioConfig"); // per method contract
     if (config.getBoolean(DremioConfig.ENABLE_EXECUTOR_BOOL)
       && !config.getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL)) {
       return ExecutorCredentialsService.newInstance(
         config, result,
         registry.provider(CredentialsServiceBlockingStub.class),
         registry.provider(OptionManager.class));
     }
     return SimpleCredentialsService.newInstance(config, result);
   }
}
