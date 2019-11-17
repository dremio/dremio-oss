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

import org.pf4j.JarPluginLoader;
import org.pf4j.PluginClassLoader;
import org.pf4j.PluginDescriptor;
import org.pf4j.PluginManager;

/**
 * Customized plugin loader to create a classloader that extracts native libraries before loading them from a plugin
 * bundle.
 */
public class NativeLibJarPluginLoader extends JarPluginLoader {
  public NativeLibJarPluginLoader(PluginManager pluginManager) {
    super(pluginManager);
  }

  @Override
  public ClassLoader loadPlugin(Path pluginPath, PluginDescriptor pluginDescriptor) {
    PluginClassLoader pluginClassLoader = new NativeLibPluginClassLoader(pluginPath, this.pluginManager, pluginDescriptor, this.getClass().getClassLoader());
    pluginClassLoader.addFile(pluginPath.toFile());
    return pluginClassLoader;
  }
}
