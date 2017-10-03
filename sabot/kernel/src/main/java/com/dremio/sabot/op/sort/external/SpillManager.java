/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.sort.external;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.google.common.collect.Lists;

/**
 * Distribute spills across given list of directories.
 * Monitor disk space left and stop using disks which are running low on free space.
 * Monitoring is disabled for spill directories on non local filesystems.
 */
public class SpillManager implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DiskRunManager.class);

  static String DREMIO_LOCAL_IMPL_STRING = "fs.dremio-local.impl";
  static String DREMIO_LOCAL_SCHEME = "dremio-local";

  private final String id;
  private final List<SpillDirectory> allSpillDirectories;
  private final List<SpillDirectory> healthySpillDirectories;
  private final long minDiskSpace;
  private final double minDiskSpacePercentage;
  private final long healthCheckInterval;
  private final long healthCheckSpills;
  private final Configuration hadoopConf;

  public SpillManager(SabotConfig sabotConfig, OptionManager optionManager, String id, Configuration hadoopConf)  {
    final List<String> directories = new ArrayList<>(sabotConfig.getStringList(ExecConstants.SPILL_DIRS));
    if (directories.isEmpty()) {
      throw UserException.dataWriteError().message("No spill locations specified.").build(logger);
    }

    this.id  = id;
    this.hadoopConf = hadoopConf;
    // load options
    if (optionManager != null) {
      this.minDiskSpacePercentage = optionManager.getOption(ExecConstants.SPILL_DISK_SPACE_LIMIT_PERCENTAGE);
      this.minDiskSpace = optionManager.getOption(ExecConstants.SPILL_DISK_SPACE_LIMIT_BYTES);
      this.healthCheckInterval = optionManager.getOption(ExecConstants.SPILL_DISK_SPACE_CHECK_INTERVAL);
      this.healthCheckSpills = optionManager.getOption(ExecConstants.SPILL_DISK_SPACE_CHECK_SPILLS);
    } else {
      this.minDiskSpacePercentage = ExecConstants.SPILL_DISK_SPACE_LIMIT_PERCENTAGE.getDefault().float_val;
      this.minDiskSpace = ExecConstants.SPILL_DISK_SPACE_LIMIT_BYTES.getDefault().num_val;
      this.healthCheckInterval = ExecConstants.SPILL_DISK_SPACE_CHECK_INTERVAL.getDefault().num_val;
      this.healthCheckSpills = ExecConstants.SPILL_DISK_SPACE_CHECK_SPILLS.getDefault().num_val;
    }

    this.allSpillDirectories = Lists.newArrayListWithCapacity(directories.size());
    this.healthySpillDirectories = Lists.newArrayListWithCapacity(directories.size());

    // Create spill directories for each disk.
    for (String directory : directories) {
      final Path spillDirPath = new Path(new Path(directory), id);
      try {
        final SpillDirectory spillDirectory = new SpillDirectory(spillDirPath);
        healthySpillDirectories.add(spillDirectory);
        allSpillDirectories.add(spillDirectory); // for cleanup
      } catch (IOException ioe) {
        throw UserException.dataWriteError(ioe).message("Failed to create sort spill directory " + spillDirPath)
          .build(logger);
      }
    }
  }

  public SpillFile getSpillFile(String fileName) throws RuntimeException {
    while (!healthySpillDirectories.isEmpty()) {
      // pick a random spill directory
      final int index = ThreadLocalRandom.current().nextInt(healthySpillDirectories.size());
      final SpillDirectory spillDirectory = healthySpillDirectories.get(index);

      if (spillDirectory.isHealthy()) {
        spillDirectory.assign();
        return new SpillFile(spillDirectory.getFileSystem(), new Path(spillDirectory.getSpillDirPath(), fileName));
      } else {
        healthySpillDirectories.remove(index);
      }
    }

    throw UserException.dataWriteError().
      message(String.format("Failed to allocate disk space for spill during sort for %s. All spill directories %s are full",
        id, allSpillDirectories)).build(logger);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(allSpillDirectories);
  }

  final public class SpillFile implements AutoCloseable {
    private final FileSystem fs;
    private final Path path;

    SpillFile(FileSystem fs, Path path) {
      this.fs = fs;
      this.path = path;
    }

    public FSDataOutputStream create() throws IOException {
      return fs.create(path);
    }

    public FSDataInputStream open() throws IOException {
      return fs.open(path);
    }

    private void delete() throws IOException {
      fs.delete(path, true);
    }

    @Override
    public void close() throws Exception {
      delete();
    }

    public FileStatus getFileStatus() throws IOException {
      return fs.getFileStatus(path);
    }

    public Path getPath() {
      return path;
    }
  }

  private final class SpillDirectory implements AutoCloseable {
    private long threshold;
    private long lastChecked;
    private File disk;
    private long spillsAssignedSinceLastCheck;
    private final Path spillDirPath;
    private final FileSystem fileSystem;
    private final boolean enableHealthCheck;

    private SpillDirectory(Path spillDir) throws IOException {
      try {
        final URI spillDirUri = spillDir.toUri();
        this.fileSystem = FileSystemWrapper.get(spillDirUri, hadoopConf);
        fileSystem.mkdirs(spillDir);
        enableHealthCheck = DREMIO_LOCAL_SCHEME.equals(spillDirUri.getScheme());
      } catch (IOException e) {
        throw UserException.dataWriteError(e).message("Failed to create spill directory " + spillDir).build(logger);
      }
      this.spillDirPath = spillDir;
      if (enableHealthCheck) {
        this.disk = new File(spillDir.toString());
        final double totalSpace = (double) disk.getTotalSpace();
        this.threshold = Math.max((long) ((totalSpace / 100.0) * minDiskSpacePercentage), minDiskSpace);
        this.lastChecked = System.currentTimeMillis();
      }
    }

    Path getSpillDirPath() {
      return spillDirPath;
    }

    public FileSystem getFileSystem() {
      return fileSystem;
    }

    private boolean isHealthy() {
      if (enableHealthCheck) {
        final long now = System.currentTimeMillis();
        if (spillsAssignedSinceLastCheck >=  healthCheckSpills || lastChecked + healthCheckInterval > now) {
          final long available = disk.getFreeSpace();
          if (available < threshold) {
            return false;
          }
        }
        lastChecked = now;
        spillsAssignedSinceLastCheck = 0;
      }
      return true;
    }

    public void assign() {
      ++spillsAssignedSinceLastCheck;
    }

    @Override
    public void close() throws Exception {
      fileSystem.delete(spillDirPath, true);
    }
  }
}