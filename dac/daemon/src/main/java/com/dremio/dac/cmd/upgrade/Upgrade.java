/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.cmd.upgrade;

import static com.dremio.common.util.DremioVersionInfo.VERSION;
import static com.dremio.dac.util.ClusterVersionUtils.fromClusterVersion;
import static com.dremio.dac.util.ClusterVersionUtils.toClusterVersion;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.dremio.common.Version;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.dac.proto.model.source.ClusterIdentity;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.support.SupportService;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Upgrade command.<br>
 * Extracts store version and uses it to decide if upgrade is possible and which tasks should be executed.
 * If no version is found, the tool assumes it's 1.0.6 as there is no way to identify versions prior to that anyway
 */
public class Upgrade {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Upgrade.class);

  /**
   * A {@code Version} ordering ignoring qualifiers for the sake of upgrade
   */
  public static final Comparator<Version> UPGRADE_VERSION_ORDERING = Comparator
      .comparing(Version::getMajorVersion)
      .thenComparing(Version::getMinorVersion)
      .thenComparing(Version::getPatchVersion)
      .thenComparing(Version::getBuildNumber);

  private final DACConfig dacConfig;
  private final ScanResult classpathScan;
  private final boolean verbose;

  private final List<? extends UpgradeTask> upgradeTasks;
  private final Optional<Version> tasksSmallestMinVersion;
  private final Optional<Version> tasksGreatestMaxVersion;

  public Upgrade(DACConfig dacConfig, ScanResult classPathScan, boolean verbose) {
    this.dacConfig = dacConfig;
    this.classpathScan = classPathScan;
    this.verbose = verbose;

    // Get all the upgrade tasks present in the classpath, correctly ordered
    this.upgradeTasks = classPathScan.getImplementations(UpgradeTask.class).stream()
      .map(clazz -> {
        // All upgrade tasks should be valid classes accessible from Upgrade with a no-arg constructor
        try {
          final Constructor<? extends UpgradeTask> constructor = clazz.getConstructor();
          return constructor.newInstance();
        } catch (NoSuchMethodException e) {
          if (verbose) {
            System.out.printf("Ignoring class without public no-arg constructor %s.%n", clazz.getSimpleName());
          }
        } catch (InstantiationException e) {
          if (verbose && clazz != UpgradeTask.class) {
            System.out.printf("Ignoring abstract class %s.%n", clazz.getSimpleName());
          }
        } catch (IllegalAccessException e) {
          if (verbose) {
            System.out.printf("Ignoring class without public constructor %s.%n", clazz.getSimpleName());
          }
        } catch (InvocationTargetException e) {
          if (verbose) {
            System.out.printf("Ignoring class %s (failed during instantiation with message %s).%n",
                clazz.getSimpleName(), e.getTargetException().getMessage());
          }
        }
        return null;
      })
      // Filter out null values
      .filter(Objects::nonNull)
      .sorted()
      .collect(Collectors.toList());

    this.tasksSmallestMinVersion = this.upgradeTasks.stream()
        .map(UpgradeTask::getMinVersion)
        .min(UPGRADE_VERSION_ORDERING);

    this.tasksGreatestMaxVersion = upgradeTasks.stream()
        .map(UpgradeTask::getMaxVersion)
        .max(UPGRADE_VERSION_ORDERING);

  }

  protected DACConfig getDACConfig() {
    return dacConfig;
  }

  @VisibleForTesting
  List<? extends UpgradeTask> getUpgradeTasks() {
    return upgradeTasks;
  }

  /**
   * The smallest version of all the tasks' minimum versions
   */
  public Optional<Version> getTasksSmallestMinVersion() {
    return tasksSmallestMinVersion;
  }

  /**
   * The greatest version of all the tasks' max versions
   */
  public Optional<Version> getTasksGreatestMaxVersion() {
    return tasksGreatestMaxVersion;
  }

  private static Version retrieveStoreVersion(ClusterIdentity identity) {
    final Version storeVersion = fromClusterVersion(identity.getVersion());
    return storeVersion != null ? storeVersion : UpgradeTask.VERSION_106;
  }

  protected void ensureUpgradeSupported(Version storeVersion) {
    // make sure we are not trying to downgrade
    Preconditions.checkState(UPGRADE_VERSION_ORDERING.compare(storeVersion, VERSION) <= 0,
      "Downgrading from version %s to %s is not supported", storeVersion, VERSION);
    tasksSmallestMinVersion.ifPresent(minVersion -> {
      // make sure we have upgrade tasks for the current KVStore version
      Preconditions.checkState(UPGRADE_VERSION_ORDERING.compare(storeVersion, minVersion) >= 0,
          "Cannot run upgrade tool on versions below %s", minVersion.getVersion());
    });
  }

  public void run() throws Exception {
    final String dbDir = dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING);
    final File dbFile = new File(dbDir);

    if (!dbFile.exists()) {
      System.out.println("No database found. Skipping upgrade");
      return;
    }

    String[] listFiles = dbFile.list();
    // An empty array means no file in the directory, so do not try to upgrade
    // A null value means dbFile is not a directory. Let the upgrade task handle it.
    if (listFiles != null && listFiles.length == 0) {
      System.out.println("No database found. Skipping upgrade");
      return;
    }

    try (final KVStoreProvider storeProvider = new LocalKVStoreProvider(classpathScan, dbDir, false, true)) {
      storeProvider.start();

      run(storeProvider);
    }
  }

  public void run(final KVStoreProvider storeProvider) throws Exception {
    final Optional<ClusterIdentity> identity = SupportService.getClusterIdentity(storeProvider);

    if (!identity.isPresent()) {
      throw UserException.validationError().message("No Cluster Identity found").build(logger);
    }

    ClusterIdentity clusterIdentity = identity.get();

    final Version kvStoreVersion = retrieveStoreVersion(clusterIdentity);
    System.out.println("KVStore version is " + kvStoreVersion.getVersion());
    ensureUpgradeSupported(kvStoreVersion);

    List<UpgradeTask> tasksToRun = new ArrayList<>();
    for(UpgradeTask task: upgradeTasks) {
      // Use upgrade comparator and do not rely on Version's one
      if (UPGRADE_VERSION_ORDERING.compare(kvStoreVersion, task.getMaxVersion()) >= 0) {
        if (verbose) {
          System.out.println("Skipping " + task);
        }
        continue;
      }
      tasksToRun.add(task);
    }

    if (!tasksToRun.isEmpty()) {
      final SabotConfig sabotConfig = dacConfig.getConfig().getSabotConfig();
      final LogicalPlanPersistence lpPersistence = new LogicalPlanPersistence(sabotConfig, classpathScan);
      final ConnectionReader connectionReader = ConnectionReader.of(classpathScan, sabotConfig);

      final UpgradeContext context = new UpgradeContext(storeProvider, lpPersistence, connectionReader);

      for (UpgradeTask task : tasksToRun) {
        System.out.println(task);
        task.upgrade(context);
      }
    }

    try {
      clusterIdentity.setVersion(toClusterVersion(VERSION));
      SupportService.updateClusterIdentity(storeProvider, clusterIdentity);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to update store version", e);
    }
  }

  public static void main(String[] args) {
    final DACConfig dacConfig = DACConfig.newConfig();
    final ScanResult classPathScan = ClassPathScanner.fromPrescan(dacConfig.getConfig().getSabotConfig());
    try {
      Upgrade upgrade = new Upgrade(dacConfig, classPathScan, true);
      upgrade.run();
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Upgrade failed " + e);
      System.exit(1);
    }
  }
}
