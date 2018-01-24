/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.daemon;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.SourceToStoragePluginConfig;
import com.dremio.service.SingletonRegistry;

/**
 * Helper class to manage DAC registries
 */
public interface DACModule {
  void bootstrap(final Runnable shutdownHook, final SingletonRegistry bootstrapRegistry, ScanResult scanResult, DACConfig dacConfig, boolean isMaster);

  void build(final SingletonRegistry bootstrapRegistry, final SingletonRegistry registry, ScanResult scanResult,
      DACConfig dacConfig, boolean isMaster, SourceToStoragePluginConfig configurator);
}
