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

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.exec.store.LocalSyncableFileSystem;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.collect.Lists;

/**
 * Implementation of the {@link SpillService} API
 */
public class SpillServiceImpl implements SpillService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpillServiceImpl.class);
  private static final String DREMIO_LOCAL_IMPL_STRING = "fs.dremio-local.impl";
  private static final String DREMIO_LOCAL_SCHEME = "dremio-local";
  private static final String LOCAL_SCHEME = "file";
  private static final Configuration SPILLING_CONFIG;

  static {
    SPILLING_CONFIG = new Configuration();
    SPILLING_CONFIG.set(DREMIO_LOCAL_IMPL_STRING, LocalSyncableFileSystem.class.getName());
    SPILLING_CONFIG.set("fs.file.impl", LocalSyncableFileSystem.class.getName());
    SPILLING_CONFIG.set("fs.file.impl.disable.cache", "true");
    // If the location URI doesn't contain any schema, fall back to local.
    SPILLING_CONFIG.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
  }
  private static final FsPermission PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private final ArrayList<String> spillDirs;
  private final SpillServiceOptions options;
  private final Provider<SchedulerService> schedulerService;
  private long minDiskSpace;
  private double minDiskSpacePercentage;
  private long healthCheckInterval;
  private boolean healthCheckEnabled;
  private long spillSweepInterval;
  private long spillSweepThreshold;

  private final Map<String, StreamInfo> spillStreams;
  // NB: healthySpillDirs set by a background task, and used by users fo SpillServiceImpl
  private volatile ArrayList<String> healthySpillDirs;
  private Cancellable healthCheckTask;

  /**
   * Create the spill service
   * @param config Configuration for the spill service, containing items such as the spill path(s),
   *               number of I/O completion threads, etc.
   */
  public SpillServiceImpl(DremioConfig config, SpillServiceOptions options, final Provider<SchedulerService> schedulerService) {
    this.spillDirs = new ArrayList<>(config.getStringList(DremioConfig.SPILLING_PATH_STRING));
    this.spillStreams = new HashMap<>();
    this.options = options;
    this.schedulerService = schedulerService;
    // Option values set at start
    minDiskSpace = 0;
    minDiskSpacePercentage = 0;
    healthCheckInterval = 0;
    spillSweepInterval = 0;
    spillSweepThreshold = 0;
    healthCheckEnabled = false;
    for (String spillDir : this.spillDirs) {
      try {
        final Path spillDirPath = new Path(spillDir);
        final FileSystem fileSystem = spillDirPath.getFileSystem(SPILLING_CONFIG);
        healthCheckEnabled = healthCheckEnabled || isHealthCheckEnabled(fileSystem.getUri().getScheme());
      } catch (Exception e) {}
    }

    // healthySpillDirs set at start()
    this.healthySpillDirs = Lists.newArrayList();
  }

  @Override
  public void start() throws Exception {
    // TODO: Implement the following:
    // TODO: 1. global pool of compression buffers
    // TODO: 2. pool of I/O completion threads (Note: for local FS only)
    // TODO: 3. create the spill filesystem adapter
    minDiskSpace = options.minDiskSpace();
    minDiskSpacePercentage = options.minDiskSpacePercentage();
    healthCheckInterval = options.healthCheckInterval();
    healthCheckEnabled = healthCheckEnabled && options.enableHealthCheck();
    spillSweepInterval = options.spillSweepInterval();
    spillSweepThreshold = options.spillSweepThreshold();

    // Create spill directories, in case it doesn't already exist
    assert healthySpillDirs.isEmpty();
    if (healthCheckEnabled) {
      for (String spillDir : this.spillDirs) {
        try {
          final Path spillDirPath = new Path(spillDir);
          final FileSystem fileSystem = spillDirPath.getFileSystem(SPILLING_CONFIG);
          if (fileSystem.exists(spillDirPath) || fileSystem.mkdirs(spillDirPath, PERMISSIONS)) {
            healthySpillDirs.add(spillDir);
          }
        } catch (Exception e) {
        }
      }
    }

    if (healthCheckEnabled) {
      healthCheckTask = schedulerService.get()
        .schedule(Schedule.Builder
            .everyMillis(healthCheckInterval)
            .startingAt(Instant.now())
            .build(),
          new SpillHealthCheckTask()
        );
    }
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public void makeSpillSubdirs(String id) throws UserException {
    //TODO: use only the healthy spill directories, once health checks implemented (shortly!). Reviewer: if you see this code, ask Vanco to fix it!
    ArrayList<String> healthySpillDirs = this.healthySpillDirs;

    // Create spill directories for each disk.
    for (String directory : healthySpillDirs) {
      try {
        final Path spillDirPath = new Path(new Path(directory), id);
        FileSystem fileSystem = spillDirPath.getFileSystem(SPILLING_CONFIG);
        if (!fileSystem.mkdirs(spillDirPath, PERMISSIONS)) {
          //TODO: withContextParameters()
          throw UserException.dataWriteError()
            .message("Failed to create directory for spilling. Please check that the spill location is accessible and confirm read, write & execute permissions.")
            .addContext("Spill directory path", directory)
            .build(logger);
        }
      } catch (Exception e) {
        //TODO: withContextParameters()
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
        final Path spillDirPath = new Path(new Path(directory), id);
        FileSystem fileSystem = spillDirPath.getFileSystem(SPILLING_CONFIG);
        fileSystem.delete(spillDirPath, true);
      } catch (Exception e) {
        // Failed to delete the spill directory. Ignored -- this might be a directory that became healthy only
        // after makeSpillSubdirs() was called
      }
    }
  }

  @Override
  public SpillDirectory getSpillSubdir(String id) throws UserException {
    ArrayList<String> currentSpillDirs = Lists.newArrayList(spillDirs);
    while (!currentSpillDirs.isEmpty()) {
      // pick a random spill directory
      final int index = ThreadLocalRandom.current().nextInt(currentSpillDirs.size());
      final String spillDir = currentSpillDirs.get(index);

      final Path spillDirPath = new Path(spillDir);
      if (isHealthy(spillDirPath)) {
        try {
          //TODO: track number of spills created in 'spillDir'
          FileSystem fileSystem = spillDirPath.getFileSystem(SPILLING_CONFIG);
          final Path spillSubdir = new Path(spillDirPath, id);
          return new SpillDirectory(spillSubdir, fileSystem);
        } catch (IOException e) {
          // Ignore this 'spillDir'. Still consider the others
        }
      }
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
      logger.debug("Check isHealthy for {} minDiskSpace: {} minDiskSpacePercentage: {}",
        spillDirPath.getName(), minDiskSpace, minDiskSpacePercentage);
      final long threshold = Math.max((long) ((totalSpace / 100.0) * minDiskSpacePercentage), minDiskSpace);
      final long available = disk.getFreeSpace();
      if (available < threshold) {
        return false;
      }
    }
    return true;
  }

  private static boolean isHealthCheckEnabled(String scheme) {
    return DREMIO_LOCAL_SCHEME.equals(scheme) || LOCAL_SCHEME.equals(scheme);
  }

  /**
   * Information maintained for every stream in the spill service
   */
  private static class StreamInfo {
    static enum StreamState {
      WRITE_IN_PROGRESS,
      WRITE_COMPLETED,
      READ_IN_PROGRESS,
      READ_COMPLETED
    }
    private final String streamName;
    private final String dataFileName;
    private final String indexFileName;
    private StreamState streamState;

    StreamInfo(String streamName, String dataFileName, String indexFileName, StreamState streamState) {
      this.streamName = streamName;
      this.dataFileName = dataFileName;
      this.indexFileName = indexFileName;
      this.streamState = streamState;
    }

    String getName() {
      return streamName;
    }
    String getDataFileName() {
      return dataFileName;
    }
    String getIndexFileName() {
      return indexFileName;
    }
    StreamState getStreamState() {
      return streamState;
    }
  }

  class SpillHealthCheckTask implements Runnable {
    private long lastSpillSweep = 0;

    @Override
    public void run() {
      ArrayList<String> newHealthySpillDirs = Lists.newArrayList();
      // Local copy of spillDirs, to protect against changes
      ArrayList<String> currentSpillDirs = spillDirs;
      for (String spillDir : currentSpillDirs) {
        final Path spillDirPath = new Path(spillDir);
        if (isHealthy(spillDirPath)) {
          newHealthySpillDirs.add(spillDir);
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
        final Path spillDirPath = new Path(spillDir);
        FileSystem fileSystem = spillDirPath.getFileSystem(SPILLING_CONFIG);
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
