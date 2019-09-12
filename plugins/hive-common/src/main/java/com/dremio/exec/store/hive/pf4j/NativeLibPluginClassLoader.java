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
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.pf4j.PluginClassLoader;
import org.pf4j.PluginDescriptor;
import org.pf4j.PluginManager;
import org.pf4j.util.FileUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Customized plugin classloader that extracts native libraries before loading them from a plugin
 * bundle.
 */
public class NativeLibPluginClassLoader extends PluginClassLoader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NativeLibPluginClassLoader.class);

  private final Path pluginPath;
  private volatile Path tempDirectory;

  public NativeLibPluginClassLoader(Path pluginPath, PluginManager pluginManager,
                                    PluginDescriptor pluginDescriptor, ClassLoader parent) {
    super(pluginManager, pluginDescriptor, parent);
    this.pluginPath = pluginPath;
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    // Overrode ClassLoader - getResources to prevent getting parent's resources recursively when
    // loading org.apache.hadoop.fs.FileSystem FileSystem implementations
    switch(name) {
    case "META-INF/services/org.apache.hadoop.fs.FileSystem":
    case "META-INF/services/org.apache.hadoop.io.compress.CompressionCodec":
      return findResources(name);

    default:
      return super.getResources(name);
    }
  }

  @Override
  protected String findLibrary(String libname) {
    try {
      extractLibrariesToTempDirIfNeeded();
    } catch (IOException ex) {
      logger.error("Error creating temporary directory", ex);
      return super.findLibrary(libname);
    }

    Preconditions.checkState(tempDirectory != null, "Native libraries must already be extracted.");

    // Find the particular library that caller is trying to load.
    final String mappedName = System.mapLibraryName(libname);
    final Path realFile = Paths.get(tempDirectory.toString(), "PF4J-INF", "native-libs", mappedName);
    if (Files.exists(realFile)) {
      return realFile.toAbsolutePath().toString();
    }

    return super.findLibrary(libname);
  }

  @Override
  public void close() throws IOException {
    try {
      synchronized (this) {
        if (tempDirectory != null) {
          // This method recursively deletes the directory tree passed in.
          FileUtils.delete(tempDirectory);
          tempDirectory = null;
        }
      }
    } catch (IOException ex) {
      logger.error("Error deleting temporary directory for native libraries {}", tempDirectory, ex);
    }

    super.close();
  }

  private synchronized void extractLibrariesToTempDirIfNeeded() throws IOException {
    if (tempDirectory == null) {
      try (final ZipFile pluginAsZip = new ZipFile(pluginPath.toFile())) {
        // Create temp-directory/PF4J-INF/native-libs directory.
        tempDirectory = Files.createTempDirectory(null);
        Files.createDirectories(Paths.get(tempDirectory.toString(), "PF4J-INF", "native-libs"));

        // Get the list of files stored in PF4J-INF/native-libs and copy them to the temp directory.
        final Enumeration<? extends ZipEntry> entries = pluginAsZip.entries();
        while (entries.hasMoreElements()) {
          final ZipEntry entry = entries.nextElement();
          if (entry.isDirectory() || !entry.getName().startsWith("PF4J-INF/native-libs")) {
            continue;
          }
          validateZipDirectory(tempDirectory, entry);
          // Create any sub-directories the resource might need.
          final Path resourceFullTempDirPath = Paths.get(tempDirectory.toString(), entry.getName());
          try (InputStream libraryStream = pluginAsZip.getInputStream(entry)) {
            Files.copy(libraryStream, resourceFullTempDirPath);
          }
        }
      }
    }
  }

  @VisibleForTesting
  static void validateZipDirectory(Path tempDirectory, final ZipEntry entry) throws IOException {
    final Path destinationPath = tempDirectory.resolve(entry.getName()).normalize();
    if (!destinationPath.startsWith(tempDirectory)) {
      throw new IOException(String.format("JAR entry %s is outside of the target directory %s. ", entry.getName(), tempDirectory));
    }
  }
}
