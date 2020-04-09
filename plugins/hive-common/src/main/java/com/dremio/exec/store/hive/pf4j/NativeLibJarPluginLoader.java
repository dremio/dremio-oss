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

import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.pf4j.JarPluginLoader;
import org.pf4j.PluginClassLoader;
import org.pf4j.PluginDescriptor;
import org.pf4j.PluginManager;
import org.slf4j.Logger;

/**
 * Customized plugin loader to create a classloader that extracts native libraries before loading them from a plugin
 * bundle.
 */
public class NativeLibJarPluginLoader extends JarPluginLoader {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(NativeLibJarPluginLoader.class);

  public NativeLibJarPluginLoader(PluginManager pluginManager) {
    super(pluginManager);
  }

  @Override
  public ClassLoader loadPlugin(Path pluginPath, PluginDescriptor pluginDescriptor) {
    final PluginClassLoader pluginClassLoader = new NativeLibPluginClassLoader(pluginPath, this.pluginManager, pluginDescriptor, this.getClass().getClassLoader());
    pluginClassLoader.addFile(pluginPath.toFile());

    // Add the subdirectory for any customer added dependencies.
    final Path dependencyPath = pluginPath.getParent().resolve(pluginDescriptor.getPluginId() + ".d");
    try (final DirectoryStream<Path> files = Files.newDirectoryStream(dependencyPath)) {
      for (final Path file : files) {
        final URL fileUrl = file.toUri().toURL();
        logger.debug("Loaded dependency for {}: {}", pluginDescriptor.getPluginId(), fileUrl.toString());
        pluginClassLoader.addURL(fileUrl);
      }
    } catch (NoSuchFileException nfe) {
      // Do nothing, the subdirectory doesn't exist.
    } catch (DirectoryIteratorException | IOException e) {
      logger.warn(String.format("Unable to add dependency directory for plugin %s", pluginDescriptor.getPluginId()), e);
    }

    return pluginClassLoader;
  }
}
