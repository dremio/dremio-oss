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
package com.dremio.dac.cmd.upgrade;

import static com.dremio.common.util.DremioVersionInfo.VERSION;
import static com.dremio.dac.util.ClusterVersionUtils.fromClusterVersion;
import static com.dremio.dac.util.ClusterVersionUtils.toClusterVersion;

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
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.util.DremioEdition;
import com.dremio.dac.cmd.AdminCommand;
import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.cmd.CmdUtils;
import com.dremio.dac.proto.model.source.ClusterIdentity;
import com.dremio.dac.proto.model.source.UpgradeStatus;
import com.dremio.dac.proto.model.source.UpgradeTaskRun;
import com.dremio.dac.proto.model.source.UpgradeTaskStore;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.support.BasicSupportService;
import com.dremio.dac.support.SupportService;
import com.dremio.dac.support.UpgradeStore;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Upgrade represents the upgrade command.
 *
 * Extracts store version and uses it to decide if upgrade is possible and which tasks should be executed.
 * If no version is found, the tool assumes it's 1.0.6 as there is no way to identify versions prior to that anyway
 * Adding ability to store task state in KVStore itself. It allows:
 * 1. Not to repeat task run if already run, but upgrade aborted/stopped in between
 * 2. Repeat task run is it was not successful
 */
@AdminCommand(value = "upgrade", description = "Upgrades KV store version")

public class Upgrade {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Upgrade.class);

  //Special VERSION number - which was assigned to a few customer[s] out of band.
  //Even though it shows up as 5.0.1 , its less than dremio version 4.2.0
  private static final Version VERSION_501 = new Version("5.0.1", 5, 0, 1, 0, "");

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

  /**
   * Constructs an Upgrade object.
   *  <p>
   *  Gets an upgrade tasks list, mapping  to task with class with a no-arg constructor and filtering by non abstract class.
   *  Next, filter out null values and sort upgrade tasks based on dependencies.
   *
   * @param dacConfig     a Daemon configuration instance
   * @param classPathScan a ScanResult object containing all upgrade tasks implementations
   * @param verbose       a flag which indicates if a log can be displayed
   */
  public Upgrade(DACConfig dacConfig, ScanResult classPathScan, boolean verbose) {
    this.dacConfig = dacConfig;
    this.classpathScan = classPathScan;
    this.verbose = verbose;

    // Get all the upgrade tasks present in the classpath, correctly ordered
    List<? extends UpgradeTask> allTasks = classPathScan.getImplementations(UpgradeTask.class).stream()
      .map(clazz -> {
        // All upgrade tasks should be valid classes accessible from Upgrade with a no-arg constructor
        try {
          final Constructor<? extends UpgradeTask> constructor = clazz.getConstructor();
          return constructor.newInstance();
        } catch (NoSuchMethodException e) {
          if (verbose) {
            AdminLogger.log("Ignoring class without public no-arg constructor {}.", clazz.getSimpleName());
          }
        } catch (InstantiationException e) {
          if (verbose && clazz != UpgradeTask.class) {
            AdminLogger.log("Ignoring abstract class {}.", clazz.getSimpleName());
          }
        } catch (IllegalAccessException e) {
          if (verbose) {
            AdminLogger.log("Ignoring class without public constructor {}.", clazz.getSimpleName());
          }
        } catch (InvocationTargetException e) {
          if (verbose) {
            AdminLogger.log("Ignoring class {} (failed during instantiation with message {}).",
              clazz.getSimpleName(), e.getTargetException().getMessage());
          }
        }
        return null;
      })
      // Filter out null values
      .filter(Objects::nonNull)
      .collect(Collectors.toList());

    final UpgradeTaskDependencyResolver upgradeTaskDependencyResolver =
      new UpgradeTaskDependencyResolver(allTasks);
    this.upgradeTasks = upgradeTaskDependencyResolver.topologicalTasksSort();
  }

  /**
   * Gets the Daemon configuration instance.
   *
   * @return the Daemon configuration instance
   */
  protected DACConfig getDACConfig() {
    return dacConfig;
  }

  /**
   * Gets the UpgradeTask implementation.
   *
   * @return the UpgradeTask implementation
   */
  @VisibleForTesting
  List<? extends UpgradeTask> getUpgradeTasks() {
    return upgradeTasks;
  }

  /**
   * Retrieves a stored version in cluster.
   * <p>
   * In case of the version returned of the cluster is null, will be returned the version "1.0.6".
   *
   * @param identity a ClusterIdentity instance, to obtain the cluster's information
   * @return         stored version in cluster
   */
  static Version retrieveStoreVersion(ClusterIdentity identity) {
    final Version storeVersion = fromClusterVersion(identity.getVersion());
    return storeVersion != null ? storeVersion : LegacyUpgradeTask.VERSION_106;
  }

  /**
   * Ensures that the current upgrade is supported.
   * <p>
   * Verifies if isn't trying makes a downgrade and if the stored version
   * is "2.0" or higher.
   *
   * @param storeVersion the version string to be checked
   */
  protected void ensureUpgradeSupported(Version storeVersion) {

    boolean versionDowngradeCheck = true;

    //disable the check when moving from 5.0.0/5.0.1 to 4.2.x, as its still an upgrade.
    if (storeVersion.getMajorVersion() == VERSION_501.getMajorVersion()
      && storeVersion.getMinorVersion() == VERSION_501.getMinorVersion()
      && storeVersion.getPatchVersion() <= VERSION_501.getPatchVersion()
      && VERSION.getMajorVersion() == 4
      && VERSION.getMinorVersion() == 2) {
      versionDowngradeCheck = false;
    }


    if (versionDowngradeCheck) {
      // make sure we are not trying to downgrade
      Preconditions.checkState(UPGRADE_VERSION_ORDERING.compare(storeVersion, VERSION) <= 0,
        "Downgrading from version %s to %s is not supported", storeVersion, VERSION);
    }

    // only allow upgrading from 2.0 and higher
    Preconditions.checkState(storeVersion.getMajorVersion() >= 2,
      "Upgrading from %s to %s is not supported.  Please upgrade to 2.0 first.", storeVersion, VERSION);
  }

  /**
   * Runs the upgrade command
   *
   * @throws Exception If any exceptions or errors occurs
   */
  public void run() throws Exception {
    run(true);
  }

  /**
   * Runs the upgrade command.
   *
   * @param noDBOpenRetry a flag which indicates if it should retry in case of the database can't open
   * @throws Exception If any exceptions or errors occurs
   */
  public void run(boolean noDBOpenRetry) throws Exception {
    Optional<LocalKVStoreProvider> storeOptional = CmdUtils.getKVStoreProvider(dacConfig.getConfig(), classpathScan, noDBOpenRetry, true);
    if (!storeOptional.isPresent()) {
      AdminLogger.log("No database found. Skipping upgrade");
      return;
    }
    try (final LocalKVStoreProvider storeProvider = storeOptional.get()) {
      storeProvider.start();

      run(storeProvider.asLegacy());
    }

  }

  /**
   * Validates an upgrade.
   * <p>
   * Checks if previous edition is available and Dremio edition it is upgrading to is the same.
   *
   * @param storeProvider the Provider object that contains the version to be checked if is available
   * @param curEdition    the Dremio version
   * @throws Exception If any exception or errors occurs
   */
  @VisibleForTesting
  public void validateUpgrade(final LegacyKVStoreProvider storeProvider, final String curEdition) throws Exception {
    if (!getDACConfig().isMigrationEnabled()) {
      // If the migration is disabled, validate this task.
      final ConfigurationStore configurationStore = new ConfigurationStore(storeProvider);
      final ConfigurationEntry entry = configurationStore.get(SupportService.DREMIO_EDITION);
      if (entry != null && entry.getValue() != null) {
        final String prevEdition = new String(entry.getValue().toByteArray());
        if (!Strings.isNullOrEmpty(prevEdition) && !prevEdition.equals(curEdition)) {
          throw new Exception(String.format("Illegal upgrade from %s to %s", prevEdition, curEdition));
        }
      }
    }
  }

  /**
   * Runs the upgrade command.
   *
   * @param storeProvider the Provider object that contains the version to be checked if is available
   * @throws Exception If any exception or errors occurs
   */
  public void run(final LegacyKVStoreProvider storeProvider) throws Exception {
    final Optional<ClusterIdentity> identity =
      BasicSupportService.getClusterIdentity(storeProvider);
    final UpgradeStore upgradeStore = new UpgradeStore(storeProvider);

    if (!identity.isPresent()) {
      AdminLogger.log("No cluster identity found. Skipping upgrade");
      return;
    }

    ClusterIdentity clusterIdentity = identity.get();
    validateUpgrade(storeProvider, DremioEdition.getAsString());

    final Version kvStoreVersion = retrieveStoreVersion(clusterIdentity);

    AdminLogger.log("KVStore version is {}",kvStoreVersion.getVersion());
    ensureUpgradeSupported(kvStoreVersion);

    AdminLogger.log("\nUpgrade Tasks Status before upgrade");

    AdminLogger.log(upgradeStore.toString());

    List<UpgradeTask> tasksToRun = new ArrayList<>();
    for(UpgradeTask task: upgradeTasks) {
      if (upgradeStore.isUpgradeTaskCompleted(task.getTaskUUID())) {
        if (verbose) {
          AdminLogger.log("Task: {} completed. Skipping",task);
        }
        continue;
      }
      tasksToRun.add(task);
    }

    if (!tasksToRun.isEmpty()) {
      final SabotConfig sabotConfig = dacConfig.getConfig().getSabotConfig();
      final LogicalPlanPersistence lpPersistence = new LogicalPlanPersistence(sabotConfig, classpathScan);
      final ConnectionReader connectionReader = ConnectionReader.of(classpathScan, sabotConfig);

      final UpgradeContext context = new UpgradeContext(storeProvider, lpPersistence, connectionReader, classpathScan);

      for (UpgradeTask task : tasksToRun) {
        AdminLogger.log(task.toString());
        upgradeExternal(task, context, upgradeStore, kvStoreVersion);
      }
    }

    try {
      clusterIdentity.setVersion(toClusterVersion(VERSION));
      BasicSupportService.updateClusterIdentity(storeProvider, clusterIdentity);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to update store version", e);
    }

    AdminLogger.log("\n Upgrade Tasks Status after upgrade");
    AdminLogger.log(upgradeStore.toString());

  }

  /**
   * Executes the upgrade command to upgrade with update of UpgradeStore.
   *
   * @param context         an UpgradeContext instance that contains all stores required by all upgrade tasks
   * @param kvStoreVersion  the stored version metadata
   * @throws Exception If any exception or errors occurs
   */
  static void upgradeExternal(UpgradeTask upgradeTask, UpgradeContext context,
                              UpgradeStore upgradeStore, Version kvStoreVersion) throws
    Exception {
    if (!proceedWithUpgrade(upgradeTask, upgradeStore, kvStoreVersion)) {
      return;
    }
    long startTime = System.currentTimeMillis();
    try {
      upgradeTask.upgrade(context);
    } catch (Exception e) {
      try {
        completeUpgradeTaskRun(upgradeTask, upgradeStore, startTime, UpgradeStatus.FAILED);
      } catch (Exception ex) {
        e.addSuppressed(ex);
        AdminLogger.log("Failed to update task {} state to FAILED", upgradeTask.getTaskName());
      }
      throw e;
    }
    completeUpgradeTaskRun(upgradeTask, upgradeStore, startTime, UpgradeStatus.COMPLETED);
  }
  /**
   * Checks if it should proceed with the upgrade.
   * <p>
   * Compares if max version of the task is less than current KVStore version
   * if it is true - there is no point of running this task anymore
   * record it in KVStore, otherwise we will need to proceed with upgrade.
   *
   * @param upgradeTask     an UpgradeTask implementation for all upgrade tasks
   * @param upgradeStore    a KVStore object to store Upgrade Tasks
   * @param kvStoreVersion  the stored version
   * @return a flag which indicates if it should proceed with the upgrade
   * @throws Exception If any exception or errors occurs
   */
  private static boolean proceedWithUpgrade(UpgradeTask upgradeTask, UpgradeStore upgradeStore, Version kvStoreVersion) throws
    Exception {

    // proceed with upgrade unless some legacy tasks are in the past
    int compareResult = -1;
    if (upgradeTask instanceof LegacyUpgradeTask) {
      LegacyUpgradeTask legacyTask = (LegacyUpgradeTask) upgradeTask;
      compareResult = UPGRADE_VERSION_ORDERING.compare(kvStoreVersion, legacyTask.getMaxVersion());
    }
    if (compareResult <= 0) {
      return true;
    }
    // we are past max version for which upgrade is relevant
    // just insert entry into upgrade store
    UpgradeTaskRun upgradeTaskRun = new UpgradeTaskRun()
      .setStartTime(System.currentTimeMillis())
      .setEndTime(System.currentTimeMillis())
      .setStatus(UpgradeStatus.OUTDATED);
    upgradeStore.createUpgradeTaskStoreEntry(upgradeTask.getTaskUUID(), upgradeTask.getTaskName(), upgradeTaskRun);
    return false;
  }

  /**
   * Completes the update task run to upgrade with update of UpgradeStore.
   * <p>
   * Inserts entry into UpgradeStore with task completion.
   *
   * @param upgradeTask   an UpgradeTask implementation for all upgrade tasks
   * @param upgradeStore  a KVStore object to store Upgrade Tasks
   * @param startTime     the initial time to the current upgrade task run
   * @param upgradeStatus the status of the update task
   * @return              an UpgradeTaskSTore object
   * @throws Exception If any exception or errors occurs
   */
  private static UpgradeTaskStore completeUpgradeTaskRun(
    UpgradeTask upgradeTask, UpgradeStore upgradeStore, long startTime, UpgradeStatus upgradeStatus)
    throws Exception {
    UpgradeTaskRun upgradeTaskRun = new UpgradeTaskRun()
      .setStartTime(startTime)
      .setEndTime(System.currentTimeMillis())
      .setStatus(upgradeStatus);
    return upgradeStore.addUpgradeRun(upgradeTask.getTaskUUID(), upgradeTask.getTaskName(), upgradeTaskRun);
  }

  /**
   * Executes an upgrade command.
   * <p>
   * Main function to executes the upgrade command.
   *
   * @param args a string array with the arguments to execute the main method
   */
  public static void main(String[] args) {
    final DACConfig dacConfig = DACConfig.newConfig();
    final ScanResult classPathScan = ClassPathScanner.fromPrescan(dacConfig.getConfig().getSabotConfig());
    try {
      Upgrade upgrade = new Upgrade(dacConfig, classPathScan, true);
      upgrade.run();
    } catch (Exception e) {
      AdminLogger.log("\nUpgrade failed", e);
      System.exit(1);
    }
  }
}
