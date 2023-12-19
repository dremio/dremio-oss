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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility to scan classpath at runtime
 *
 */
public class RunTimeScan {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunTimeScan.class);

  /** result of prescan */
  private static final ScanResult PRESCANNED = BuildTimeScan.load();

  /** urls of the locations (classes directory or jar) to scan that don't have a registry in them */
  private static final Collection<URL> NON_PRESCANNED_MARKED_PATHS = getNonPrescannedMarkedPaths();

  /**
   * @return getMarkedPaths() sans getPrescannedPaths()
   */
  static Collection<URL> getNonPrescannedMarkedPaths() {
    Collection<URL> markedPaths = ClassPathScanner.getMarkedPaths();
    markedPaths.removeAll(BuildTimeScan.getPrescannedPaths());
    return markedPaths;
  }

  private static ScanResult createFromSavedScanResults() {
    final String savedScanResults = System.getProperty("com.dremio.savedScanResults");
    if (savedScanResults == null) {
      return null;
    }

    final File scanResultsFile = new File(savedScanResults);
    if (scanResultsFile.exists()) {
      try {
        return new ObjectMapper().readValue(scanResultsFile, ScanResult.class);
      } catch (IOException e) {
        logger.warn("Unable to read scan result from {} (proceeding to slow path): {}",
          scanResultsFile.getName(), e.toString());
      }
    }
    return null;
  }

  /**
   * loads prescanned classpath info and scans for extra ones based on configuration.
   * (unless prescan is disabled with {@see ClassPathScanner#IMPLEMENTATIONS_SCAN_CACHE}=falses)
   * If ScanResult was generated at build time and is indicated by com.dremio.savedScanResults,
   * then short circuit to just load that and return
   * @param config to retrieve the packages to scan
   * @return the scan result
   */
  public static ScanResult fromPrescan(SabotConfig config) {
    final ScanResult preCreated = createFromSavedScanResults();
    if (preCreated != null) {
      return preCreated;
    }

    List<String> packagePrefixes = ClassPathScanner.getPackagePrefixes(config);
    List<String> scannedBaseClasses = ClassPathScanner.getScannedBaseClasses(config);
    List<String> scannedAnnotations = ClassPathScanner.getScannedAnnotations(config);
    if (ClassPathScanner.isScanBuildTimeCacheEnabled(config)) {
      // scan only locations that have not been scanned yet
      ScanResult runtimeScan = ClassPathScanner.scan(
          NON_PRESCANNED_MARKED_PATHS,
          packagePrefixes,
          scannedBaseClasses,
          scannedAnnotations,
          PRESCANNED);
      return runtimeScan.merge(PRESCANNED);
    } else {
      // scan everything
      return ClassPathScanner.scan(
          ClassPathScanner.getMarkedPaths(),
          packagePrefixes,
          scannedBaseClasses,
          scannedAnnotations,
          ClassPathScanner.emptyResult());
    }
  }

}
