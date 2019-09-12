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
package com.dremio.exec.store.hive.pf4j;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginLoader;

import com.dremio.config.DremioConfig;

/**
 * Customized plugin manager to create a classloader that extracts native libraries before loading them from a plugin
 * bundle.
 */
public class NativeLibPluginManager extends DefaultPluginManager {

  private static final String PLUGINS_PATH_DEV_MODE = "../plugins";

  @Override
  protected PluginLoader createPluginLoader() {
    return new NativeLibJarPluginLoader(this);
  }

  @Override
  protected Path createPluginsRoot() {
    final Path pluginsPath = this.isDevelopment() ? Paths.get(PLUGINS_PATH_DEV_MODE) :
      DremioConfig.getPluginsRootPath().resolve("connectors");
    return pluginsPath;
  }
}
