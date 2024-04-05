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
package com.dremio.provision.yarn;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class loader for Dremio Daemon Bundled Jar used in Yarn provisioning.
 *
 * <p>Look up for native libraries under the provided path instead of relying on {@code
 * java.library.path} system property
 */
public class BundledDaemonClassLoader extends URLClassLoader {

  private static final Logger logger = LoggerFactory.getLogger(BundledDaemonClassLoader.class);
  private final List<Path> nativeLibraryPaths;

  protected BundledDaemonClassLoader(URL[] urls, ClassLoader parent, List<Path> nativeLibraryPaths)
      throws IOException {
    super(urls, parent);

    // List of directories to look for native libraries
    this.nativeLibraryPaths = nativeLibraryPaths;
  }

  @Override
  public synchronized String findLibrary(String libname) {
    final String nativeLibName = System.mapLibraryName(libname);
    logger.info("Attempting to load {} library (as {})", libname, nativeLibName);

    for (Path path : nativeLibraryPaths) {
      final Path libPath = path.resolve(nativeLibName);
      if (!Files.exists(libPath)) {
        logger.debug(
            "Library {} (as {}) not found under {}.",
            libname,
            nativeLibName,
            libPath.toAbsolutePath());
        continue;
      }

      logger.info("Found library {} under {}.", libname, libPath.toAbsolutePath());
      return libPath.toAbsolutePath().toString();
    }

    logger.info(
        "Library {} (as {}) not found under {}.", libname, nativeLibName, nativeLibraryPaths);
    return null;
  }
}
