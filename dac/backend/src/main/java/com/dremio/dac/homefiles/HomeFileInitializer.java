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
package com.dremio.dac.homefiles;

import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.service.BindingProvider;
import com.dremio.service.Initializer;

/**
 * Initializer to create home filesystem plugin and associated filesystems and directories.
 */
public class HomeFileInitializer implements Initializer<Void> {

  @Override
  public Void initialize(BindingProvider provider) throws Exception {
    HomeFileConfig config = provider.lookup(HomeFileConfig.class);
    StoragePluginRegistry registry = provider.lookup(StoragePluginRegistry.class);
    registry.createOrUpdate(HomeFileConfig.HOME_PLUGIN_NAME, new HomeFileSystemPluginConfig(config), true);
    return null;
  }

}
