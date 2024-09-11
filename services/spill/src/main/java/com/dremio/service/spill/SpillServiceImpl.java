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
package com.dremio.service.spill;

import static com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.io.DefaultTemporaryFolderManager;
import com.dremio.common.io.ExecutorId;
import com.dremio.common.io.TemporaryFolderManager;
import com.dremio.config.DremioConfig;
import com.dremio.exec.store.LocalSyncableFileSystem;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.inject.Provider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/** Implementation of the {@link SpillService} API */
public class SpillServiceImpl implements SpillService {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SpillServiceImpl.class);
  private static final String DREMIO_LOCAL_IMPL_STRING = "fs.dremio-local.impl";
  private static final String DREMIO_LOCAL_SCHEME = "dremio-local";
  private static final String LOCAL_SCHEME = "file";

  private static volatile Configuration SPILLING_CONFIG = null;
  private static final Object SPILLING_SYNC_OBJECT = new Object();

  private static final String TEMP_FOLDER_PURPOSE = "spilling";

  private static final FsPermission PERMISSIONS =
      new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private final ArrayList<String> spillDirs;
  private final SpillServiceOptions options;
  private final Provider<SchedulerService> schedulerService;
  private final Provider<NodeEndpoint> identityProvider;
  private final Provider<Iterable<NodeEndpoint>> nodesProvider;

  private TemporaryFolderManager folderManager;
  private Map<String, Path> monitoredSpillDirectoryMap;

  private long minDiskSpace;
  private double minDiskSpacePercentage;
  private long healthCheckInterval;
  private boolean healthCheckEnabled;
  private long spillSweepInterval;
  private long spillSweepThreshold;

  // NB: healthySpillDirs set by a background task, and used by users fo SpillServiceImpl
  private volatile ArrayList<String> healthySpillDirs;
  private Cancellable healthCheckTask;

  @VisibleForTesting
  public SpillServiceImpl(
      DremioConfig config,
      SpillServiceOptions options,
      final Provider<SchedulerService> schedulerService) {
    this(config, options, schedulerService, null, null);
  }

  /**
   * Create the spill service
   *
   * @param config Configuration for the spill service, containing items such as the spill path(s),
   *     number of I/O completion threads, etc.
   */
  public SpillServiceImpl(
      DremioConfig config,
      SpillServiceOptions options,
      final Provider<SchedulerService> schedulerService,
      final Provider<NodeEndpoint> identityProvider,
      final Provider<Iterable<NodeEndpoint>> nodesProvider) {
    this.spillDirs = new ArrayList<>(config.getStringList(DremioConfig.SPILLING_PATH_STRING));
    this.options = options;
    this.schedulerService = schedulerService;
    this.identityProvider = identityProvider;
    this.nodesProvider = nodesProvider;
    // Option values set at start
    minDiskSpace = 0;
    minDiskSpacePercentage = 0;
    healthCheckInterval = 0;
    spillSweepInterval = 0;
    spillSweepThreshold = 0;
    healthCheckEnabled = false;
  }

  public static Configuration getSpillingConfig() {
    if (SPILLING_CONFIG != null) {
      return SPILLING_CONFIG;
    }
    synchronized (SPILLING_SYNC_OBJECT) {
      if (SPILLING_CONFIG == null) {
        SPILLING_CONFIG = new Configuration();
        SPILLING_CONFIG.set(DREMIO_LOCAL_IMPL_STRING, LocalSyncableFileSystem.class.getName());
        SPILLING_CONFIG.set("fs.file.impl", LocalSyncableFileSystem.class.getName());
        SPILLING_CONFIG.set("fs.file.impl.disable.cache", "true");
        // If the location URI doesn't contain any schema, fall back to local.
        SPILLING_CONFIG.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        logger.info("initialized spilling config {}", SPILLING_CONFIG);
      }
      return SPILLING_CONFIG;
    }
  }

  @Override
  public void start() throws Exception {
    // TODO: Implement the following:
    // TODO: 1. global pool of compression buffers
    // TODO: 2. pool of I/O completion threads (Note: for local FS only)
    // TODO: 3. create the spill filesystem adapter

    for (String spillDir : this.spillDirs) {
      try {
        final Path spillDirPath = new Path(spillDir);
        final FileSystem fileSystem = spillDirPath.getFileSystem(getSpillingConfig());
        healthCheckEnabled =
            healthCheckEnabled || isHealthCheckEnabled(fileSystem.getUri().getScheme());
      } catch (Exception ignored) {
      }
    }

    // healthySpillDirs set at start()
    this.healthySpillDirs = Lists.newArrayList();
    this.monitoredSpillDirectoryMap = new ConcurrentHashMap<>();
    final Supplier<Set<ExecutorId>> nodesConverter =
        (nodesProvider == null) ? null : () -> convertEndpointsToId(nodesProvider);
    final Supplier<ExecutorId> identityConverter =
        (identityProvider == null) ? null : () -> convertEndpointToId(identityProvider);
    this.folderManager =
        new DefaultTemporaryFolderManager(
            identityConverter, getSpillingConfig(), nodesConverter, TEMP_FOLDER_PURPOSE);

    minDiskSpace = options.minDiskSpace();
    minDiskSpacePercentage = options.minDiskSpacePercentage();
    healthCheckInterval = options.healthCheckInterval();
    healthCheckEnabled = healthCheckEnabled && options.enableHealthCheck();
    spillSweepInterval = options.spillSweepInterval();
    spillSweepThreshold = options.spillSweepThreshold();

    folderManager.startMonitoring();

    // Create spill directories, in case it doesn't already exist
    assert healthySpillDirs.isEmpty();
    for (String spillDir : this.spillDirs) {
      try {
        final Path spillDirPath = new Path(spillDir);
        final FileSystem fileSystem = spillDirPath.getFileSystem(getSpillingConfig());
        if (fileSystem.exists(spillDirPath) || fileSystem.mkdirs(spillDirPath, PERMISSIONS)) {
          monitoredSpillDirectoryMap.put(spillDir, folderManager.createTmpDirectory(spillDirPath));
          if (healthCheckEnabled) {
            healthySpillDirs.add(spillDir);
          }
        } else {
          logger.warn(
              "Unable to find or create spill directory {} due to lack of permissions", spillDir);
        }
      } catch (Exception e) {
        logger.info(
            "Sub directory creation in spill directory {} hit a temporary error `{}` "
                + "and is not added to healthy list. Will monitor periodically",
            spillDir,
            e.getMessage());
      }
    }

    if (healthCheckEnabled) {
      healthCheckTask =
          schedulerService
              .get()
              .schedule(
                  Schedule.Builder.everyMillis(healthCheckInterval)
                      .startingAt(Instant.now())
                      .build(),
                  new SpillHealthCheckTask());
    }
  }

  @Override
  public void close() throws Exception {
    folderManager.close();
  }

  @Override
  public void makeSpillSubdirs(String id) throws UserException {
    // TODO: use only the healthy spill directories, once health checks implemented (shortly!).
    // Reviewer: if you see this code, ask Vanco to fix it!
    ArrayList<String> healthySpillDirs = this.healthySpillDirs;

    // Create spill directories for each disk.
    for (String directory : healthySpillDirs) {
      try {
        // this will not be null
        final Path tmpPath = monitoredSpillDirectoryMap.get(directory);
        final Path spillDirPath = new Path(tmpPath, id);
        FileSystem fileSystem = spillDirPath.getFileSystem(getSpillingConfig());
        if (!fileSystem.mkdirs(spillDirPath, PERMISSIONS)) {
          // TODO: withContextParameters()
          throw UserException.dataWriteError()
              .message(
                  "Failed to create directory for spilling. Please check that the spill location is accessible and confirm read, write & execute permissions. "
                      + "If the query was ran on a reflection please ensure that arrow caching on the reflection is disabled when iceberg and unlimited splits are enabled.")
              .addContext("Spill directory path", directory)
              .build(logger);
        }
      } catch (Exception e) {
        // TODO: withContextParameters()
        throw UserException.dataWriteError(e)
            .message("Failed to create spill directory for id " + id)
            .addContext("Spill directory path", directory)
            .build(logger);
      }
    }
  }

  @Override
  public void deleteSpillSubdirs(String id) {
    // Delete the spill directory for each disk. Intentionally deleting
    for (String directory : spillDirs) {
      try {
        final Path monitoredPath = monitoredSpillDirectoryMap.get(directory);
        if (monitoredPath == null) {
          // nothing to delete, as this spill dir was never used
          continue;
        }
        final Path spillDirPath = new Path(monitoredPath, id);
        FileSystem fileSystem = spillDirPath.getFileSystem(getSpillingConfig());
        fileSystem.delete(spillDirPath, true);
      } catch (Exception e) {
        // Failed to delete the spill directory. Ignored -- this might be a directory that became
        // healthy only
        // after makeSpillSubdirs() was called
      }
    }
  }

  // checks if all spill directories are empty, used for testing.
  @Override
  public boolean isEmpty() throws IOException {
    for (String directory : spillDirs) {
      final Path monitoredPath = monitoredSpillDirectoryMap.get(directory);
      FileSystem fileSystem = monitoredPath.getFileSystem(getSpillingConfig());
      if (fileSystem.listFiles(monitoredPath, true).hasNext()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public SpillDirectory getSpillSubdir(String id) throws UserException {
    ArrayList<String> currentSpillDirs = Lists.newArrayList(spillDirs);
    while (!currentSpillDirs.isEmpty()) {
      // pick a random spill directory
      final int index = ThreadLocalRandom.current().nextInt(currentSpillDirs.size());
      final String spillDir = currentSpillDirs.get(index);

      final Path spillDirPath = new Path(spillDir);
      final Path monitoredPath = monitoredSpillDirectoryMap.get(spillDir);
      if (isHealthy(spillDirPath) && monitoredPath != null) {
        try {
          // TODO: track number of spills created in 'spillDir'
          FileSystem fileSystem = spillDirPath.getFileSystem(getSpillingConfig());
          final Path spillSubdir = new Path(monitoredPath, id);
          return new SpillDirectory(spillSubdir, fileSystem);
        } catch (IOException e) {
          // Ignore this 'spillDir'. Still consider the others
          logger.warn(
              "Spill directory {} hit disk issues after successful health check. Error was: {} ",
              spillDir,
              e.getMessage());
        }
      }
      logger.info("Spill directory {} hit disk issues", spillDir);

      // Hm... 'spillDir' didn't work out. Let's consider the others
      currentSpillDirs.remove(index);
    }
    // TODO: withContextParameters()
    throw UserException.dataWriteError()
        .message("Failed to spill to disk. Please check space availability")
        .addContext("spill id", id)
        .addContext("all spill locations", spillDirs.toString())
        .build(logger);
  }

  private boolean isHealthy(Path spillDirPath) {
    if (healthCheckEnabled) {
      final File disk = new File(Path.getPathWithoutSchemeAndAuthority(spillDirPath).toString());
      final double totalSpace = (double) disk.getTotalSpace();
      minDiskSpace = options.minDiskSpace();
      minDiskSpacePercentage = options.minDiskSpacePercentage();
      logger.debug(
          "Check isHealthy for {} minDiskSpace: {} minDiskSpacePercentage: {}",
          spillDirPath.getName(),
          minDiskSpace,
          minDiskSpacePercentage);
      final long threshold =
          Math.max((long) ((totalSpace / 100.0) * minDiskSpacePercentage), minDiskSpace);
      final long available = disk.getFreeSpace();
      if (available < threshold) {
        logger.info(
            "Check isHealthy available space {} is less than threshold {} for spillDirectory {} "
                + "minDiskSpace {}, minDiskSpacePercentage {}",
            available,
            threshold,
            spillDirPath.getName(),
            minDiskSpace,
            minDiskSpacePercentage);
        return false;
      }
    }
    return true;
  }

  private static boolean isHealthCheckEnabled(String scheme) {
    return DREMIO_LOCAL_SCHEME.equals(scheme) || LOCAL_SCHEME.equals(scheme);
  }

  private static Set<ExecutorId> convertEndpointsToId(
      Provider<Iterable<NodeEndpoint>> nodesProvider) {
    if (nodesProvider == null) {
      // to retain current behaviour
      return null;
    }
    final Iterable<NodeEndpoint> availableEndpoints = nodesProvider.get();
    if (availableEndpoints != null) {
      return StreamSupport.stream(availableEndpoints.spliterator(), false)
          .map(
              nodeEndpoint ->
                  new ExecutorId(nodeEndpoint.getAddress(), nodeEndpoint.getFabricPort()))
          .collect(Collectors.toSet());
    } else {
      return null;
    }
  }

  private static ExecutorId convertEndpointToId(Provider<NodeEndpoint> identityProvider) {
    if (identityProvider == null) {
      // to retain current behaviour
      return null;
    }
    final NodeEndpoint current = identityProvider.get();
    return new ExecutorId(current.getAddress(), current.getFabricPort());
  }

  class SpillHealthCheckTask implements Runnable {
    private long lastSpillSweep = 0;

    @Override
    public void run() {
      ArrayList<String> newHealthySpillDirs = Lists.newArrayList();
      for (String spillDir : spillDirs) {
        final Path spillDirPath = new Path(spillDir);
        if (isHealthy(spillDirPath)) {
          boolean healthy = true;
          if (!monitoredSpillDirectoryMap.containsKey(spillDir)) {
            try {
              monitoredSpillDirectoryMap.put(
                  spillDir, folderManager.createTmpDirectory(spillDirPath));
            } catch (IOException e) {
              // if we cannot create temp folder now, try again later
              healthy = false;
              logger.warn(
                  "Spill directory hit disk issues immediately after successful health check. Error was: {} ",
                  e.getMessage());
            }
          }
          if (healthy) {
            newHealthySpillDirs.add(spillDir);
          }
        }
      }
      healthySpillDirs = newHealthySpillDirs;

      long timeNow = System.currentTimeMillis();
      if (lastSpillSweep + spillSweepInterval < timeNow) {
        long targetTime = timeNow > spillSweepThreshold ? (timeNow - spillSweepThreshold) : 0;
        for (String spillDir : newHealthySpillDirs) {
          sweep(spillDir, targetTime);
        }
        lastSpillSweep = timeNow;
      }
    }

    // Remove any sub-directories of 'spillDir' that are older than 'targetTime'
    private void sweep(String spillDir, long targetTime) {
      try {
        final Path spillDirPath = monitoredSpillDirectoryMap.get(spillDir);
        if (spillDirPath == null) {
          // nothing to sweep
          return;
        }
        FileSystem fileSystem = spillDirPath.getFileSystem(getSpillingConfig());
        RemoteIterator<LocatedFileStatus> files = fileSystem.listLocatedStatus(spillDirPath);
        while (files.hasNext()) {
          LocatedFileStatus st = files.next();
          if (st.getModificationTime() <= targetTime) {
            fileSystem.delete(st.getPath(), true);
          }
        }
      } catch (IOException e) {
        // exception silently ignored. Directory will be revisited at the next sweep
      }
    }
  }
}
