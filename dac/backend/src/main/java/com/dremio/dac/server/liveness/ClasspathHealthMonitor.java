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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.dremio.common.liveness.LiveHealthMonitor;

/**
 * When YARN provisioning is used, this health monitor checks
 * whether all the jars in the classpath exist and are readable
 */
public class ClasspathHealthMonitor implements LiveHealthMonitor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClasspathHealthMonitor.class);
  private Set<String> necessaryJarFoldersSet;

  public ClasspathHealthMonitor() {
    URL[] urls = ((URLClassLoader) (Thread.currentThread().getContextClassLoader())).getURLs();
    List<String> jars = Arrays.stream(urls).map(URL::getFile)
                              .filter(jar -> !jar.isEmpty())
                              .collect(Collectors.toList());
    necessaryJarFoldersSet = jars.stream().map(jar -> new File(jar).getParent())
                                 .filter(folder -> folder != null && !folder.isEmpty())
                                 .collect(Collectors.toSet());
  }

  public ClasspathHealthMonitor(Set<String> classpath) {
    necessaryJarFoldersSet = classpath;
  }

  @Override
  public boolean isHealthy() {
    logger.debug("Checking jars {}", necessaryJarFoldersSet.size());

    for(String jarFolderPath : necessaryJarFoldersSet) {
      File jarFolder = new File(jarFolderPath);
      boolean folderAccessible = false;
      try {
        folderAccessible = Files.isExecutable(jarFolder.toPath());
      } catch (SecurityException se) {
        logger.error("SecurityException while checking folder: {}", jarFolderPath, se);
      }
      if (!folderAccessible) {
        logger.error("Jar: {} does not exist or it is not readable!", jarFolderPath);
        return false;
      }
    }

    logger.debug("All necessary jars exist and are readable.");
    return true;
  }
}
