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
package com.dremio.common.scanner;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * main class to integrate classpath scanning in the build.
 * @see BuildTimeScan#main(String[])
 */
public class BuildTimeScan {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BuildTimeScan.class);
  private static final String REGISTRY_FILE = "META-INF/dremio-module-scan/registry.json";

  private static final ObjectMapper mapper = new ObjectMapper().enable(INDENT_OUTPUT);
  private static final ObjectReader reader = mapper.readerFor(ScanResult.class);
  private static final ObjectWriter writer = mapper.writerFor(ScanResult.class);

  /**
   * @return paths that have the prescanned registry file in them
   */
  static Collection<URL> getPrescannedPaths() {
    return ClassPathScanner.forResource(REGISTRY_FILE, true);
  }

  /**
   * loads all the prescanned resources from classpath
   * @return the result of the previous scan
   */
  static ScanResult load() {
    return loadExcept(null);
  }

  /**
   * loads all the prescanned resources from classpath
   * (except for the target location in case it already exists)
   * @return the result of the previous scan
   */
  private static ScanResult loadExcept(URL ignored) {
    Collection<URL> preScanned = ClassPathScanner.forResource(REGISTRY_FILE, false);
    ScanResult result = null;
    for (URL u : preScanned) {
      if (ignored!= null && u.toString().startsWith(ignored.toString())) {
        continue;
      }
      try (InputStream reflections = u.openStream()) {
        ScanResult ref = reader.readValue(reflections);
        if (result == null) {
          result = ref;
        } else {
          result = result.merge(ref);
        }
      } catch (IOException e) {
        throw new RuntimeException("can't read function registry at " + u, e);
      }
    }
    if (result != null) {
      if (logger.isInfoEnabled()) {
        StringBuilder sb = new StringBuilder();
        for (URL u : preScanned) {
          sb.append('\t');
          sb.append(u.toExternalForm());
          sb.append('\n');
        }
        logger.info(format("Loaded prescanned packages %s from locations:\n%s", result.getScannedPackages(), sb));
      }
      return result;
    } else {
      return ClassPathScanner.emptyResult();
    }
  }

  private static void save(ScanResult scanResult, File file) {
    try {
      writer.writeValue(file, scanResult);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * to generate the prescan file during build
   * @param args the root path for the classes where {@link BuildTimeScan#REGISTRY_FILE} is generated
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: java {cp} " + ClassPathScanner.class.getName() + " path/to/scan");
    }
    String basePath = args[0];
    System.out.println("Scanning: " + basePath);
    File registryFile = new File(basePath, REGISTRY_FILE);
    File dir = registryFile.getParentFile();
    if ((!dir.exists() && !dir.mkdirs()) || !dir.isDirectory()) {
      throw new IllegalArgumentException("could not create dir " + dir.getAbsolutePath());
    }
    SabotConfig config = SabotConfig.create();
    // normalize
    if (!basePath.endsWith("/")) {
      basePath = basePath + "/";
    }
    if (!basePath.startsWith("/")) {
      basePath = "/" + basePath;
    }
    URL url = new URL("file:" + basePath);
    Collection<URL> markedPaths = ClassPathScanner.getMarkedPaths();
    if (!markedPaths.contains(url)) {
      throw new IllegalArgumentException(url + " not in " + markedPaths);
    }
    List<String> packagePrefixes = ClassPathScanner.getPackagePrefixes(config);
    List<String> baseClasses = ClassPathScanner.getScannedBaseClasses(config);
    List<String> scannedAnnotations = ClassPathScanner.getScannedAnnotations(config);
    ScanResult preScanned = loadExcept(url);
    ScanResult scan = ClassPathScanner.scan(
        asList(url),
        packagePrefixes,
        baseClasses,
        scannedAnnotations,
        preScanned);
    save(scan, registryFile);
  }
}
