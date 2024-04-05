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
package com.dremio.dac.server.liveness;

import com.dremio.common.liveness.LiveHealthMonitor;
import com.google.common.annotations.VisibleForTesting;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Check if the classpath has been altered during the JVM lifetime */
public final class ClasspathHealthMonitor implements LiveHealthMonitor {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ClasspathHealthMonitor.class);
  private Set<Path> necessaryJarFoldersSet;

  /**
   * Creates a new instance using the current context classloader
   *
   * <p>Creates a list of entries to track by extracting the list of files and directories
   * comprising the classloader, and will ignore non existing entries, files which are not readable,
   * and directories which are not executable.
   *
   * <p>Check if the entries do still exists and are still accessible each time {@code
   * ClasspathHealthMonitor#isHealthy()} is invoked
   *
   * @return a new instance
   */
  public static ClasspathHealthMonitor newInstance() {
    URL[] urls = ((URLClassLoader) (Thread.currentThread().getContextClassLoader())).getURLs();
    return newInstance(
        Arrays.stream(urls).map(URL::getFile).filter(jar -> !jar.isEmpty()).map(Paths::get));
  }

  @VisibleForTesting
  static ClasspathHealthMonitor newInstance(Stream<Path> classpath) {
    Set<Path> paths =
        classpath
            .filter(path -> Files.exists(path)) // Confirm that files exist
            .filter(
                path ->
                    Files.isDirectory(path)
                        || Files.isReadable(
                            path)) // If the file is not a directory, confirm it can be read
            .map(
                path ->
                    Files.isDirectory(path)
                        ? path
                        : path.getParent()) // If the file is not a directory, track its parent
            .filter(Files::isExecutable) // Check that the directories are executable
            .collect(Collectors.toSet());

    return new ClasspathHealthMonitor(paths);
  }

  private ClasspathHealthMonitor(Set<Path> classpath) {
    necessaryJarFoldersSet = classpath;
  }

  @Override
  public boolean isHealthy() {
    logger.debug("Checking jars {}", necessaryJarFoldersSet.size());

    for (Path jarFolderPath : necessaryJarFoldersSet) {
      if (!Files.isExecutable(jarFolderPath)) {
        logger.error("Jar: {} does not exist or it is not readable!", jarFolderPath);
        return false;
      }
    }

    logger.debug("All necessary jars exist and are readable.");
    return true;
  }
}
