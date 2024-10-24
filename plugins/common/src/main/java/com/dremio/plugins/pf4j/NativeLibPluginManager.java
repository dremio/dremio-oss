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
package com.dremio.plugins.pf4j;

import com.dremio.common.AutoCloseables;
import com.dremio.config.DremioConfig;
import com.dremio.options.OptionResolver;
import java.net.URLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.pf4j.CompoundPluginRepository;
import org.pf4j.DefaultPluginManager;
import org.pf4j.DevelopmentPluginRepository;
import org.pf4j.JarPluginRepository;
import org.pf4j.PluginLoader;
import org.pf4j.PluginRepository;

/**
 * Customized plugin manager to create a classloader that extracts native libraries before loading
 * them from a plugin bundle.
 */
public class NativeLibPluginManager extends DefaultPluginManager {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(NativeLibPluginManager.class);

  static {
    /*
     * Disable URLConnection caching, for more information see DX-91617.
     *
     * <p>{@code NativeLibPluginClassLoader}'s parent {@code URLClassLoader} caches the loaded JARs
     * via {@code JarFileFactory}. This caching can have unexpected consequences when concurrently
     * loading and closing classloaders because the shared JARs could be still in use by other
     * classloaders.
     *
     * <p>In DX-91617 {@code ManagedStoragePlugin.replacePlugin()} method was called at the same
     * time multiple times when the plugin was not present. This triggered multiple plugin creation.
     * In this method only the first plugin creation is preserved and all the other unnecessarily
     * created plugin instances will be closed. All these plugin instances have their own instance
     * of a {@code NativeLibPluginClassLoader} but with caching enabled, those will share the same
     * {@code URLJarFile} instance (as they all read from the same plugin jar file, e.g.
     * hive3-plugin jar). Therefore, invoking close() on one of the CLs, and by extension on the
     * shared {@code URLJarFile}, will also close all related InputStream instances (see {@code
     * ZipFile$CleanableResource}), even if some of them have been opened by other class loaders.
     * This can result in unexpected StreamClosedException errors in seemingly unrelated {@code
     * NativeLibPluginClassLoader} instances.
     */
    URLConnection.setDefaultUseCaches("jar", false);
  }

  private static final String PLUGINS_PATH_DEV_MODE = "../plugins";

  private final OptionResolver optionResolver;

  public NativeLibPluginManager(OptionResolver optionResolver) {
    this.optionResolver = optionResolver;
  }

  @Override
  protected PluginLoader createPluginLoader() {
    return new NativeLibJarPluginLoader(this, optionResolver);
  }

  @Override
  protected List<Path> createPluginsRoot() {
    final Path pluginsPath =
        this.isDevelopment()
            ? Paths.get(PLUGINS_PATH_DEV_MODE)
            : DremioConfig.getPluginsRootPath().resolve("connectors");
    return Collections.singletonList(pluginsPath);
  }

  @Override
  public void stopPlugins() {
    try {
      int pluginsCount = this.getPlugins().size();
      List<NativeLibPluginClassLoader> nativeLibPluginClassLoaderList = new ArrayList<>();
      for (int i = 0; i < pluginsCount; i++) {
        nativeLibPluginClassLoaderList.add(
            (NativeLibPluginClassLoader) this.getPlugins().get(i).getPluginClassLoader());
      }
      AutoCloseables.close(nativeLibPluginClassLoaderList);
    } catch (Exception e) {
      logger.warn("Failed to close NativeLibPluginClassLoader", e);
    }
    super.stopPlugins();
  }

  @Override
  protected PluginRepository createPluginRepository() {
    // Omit the DefaultPluginRepository in the base implementation as we only want to load the
    // plugin JARs as plugins,
    // not subdirectories which include dependencies for the specific plugins.
    return (new CompoundPluginRepository())
        .add(new DevelopmentPluginRepository(this.getPluginsRoot()), this::isDevelopment)
        .add(new JarPluginRepository(this.getPluginsRoot()), this::isNotDevelopment);
  }
}
