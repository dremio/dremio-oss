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
package com.dremio.sabot.op.sort.external;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.service.spill.SpillDirectory;
import com.dremio.service.spill.SpillService;

/**
 * Distribute spills across given list of directories.
 * Monitor disk space left and stop using disks which are running low on free space.
 * Monitoring is disabled for spill directories on non local filesystems.
 */
public class SpillManager implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpillManager.class);

  static final String DREMIO_LOCAL_IMPL_STRING = "fs.dremio-local.impl";
  private static final String DREMIO_LOCAL_SCHEME = "dremio-local";
  private static final String LOCAL_SCHEMA = "file";

  private final String id;
  private final long minDiskSpace;
  private final double minDiskSpacePercentage;
  private final long healthCheckInterval;
  private final SpillService spillService;
  private final String caller;
  private final Configuration hadoopConf;

  public SpillManager(SabotConfig sabotConfig, OptionManager optionManager, String id, Configuration hadoopConf,
      SpillService spillService, String caller)  {
    final List<String> directories = new ArrayList<>(sabotConfig.getStringList(ExecConstants.SPILL_DIRS));
    if (directories.isEmpty()) {
      throw UserException.dataWriteError().message("No spill locations specified.").build(logger);
    }

    this.id  = id;
    this.caller = caller;
    this.hadoopConf = hadoopConf;
    this.spillService = spillService;
    // load options
    if (optionManager != null) {
      this.minDiskSpacePercentage = optionManager.getOption(ExecConstants.SPILL_DISK_SPACE_LIMIT_PERCENTAGE);
      this.minDiskSpace = optionManager.getOption(ExecConstants.SPILL_DISK_SPACE_LIMIT_BYTES);
      this.healthCheckInterval = optionManager.getOption(ExecConstants.SPILL_DISK_SPACE_CHECK_INTERVAL);
    } else {
      this.minDiskSpacePercentage = ExecConstants.SPILL_DISK_SPACE_LIMIT_PERCENTAGE.getDefault().getFloatVal();
      this.minDiskSpace = ExecConstants.SPILL_DISK_SPACE_LIMIT_BYTES.getDefault().getNumVal();
      this.healthCheckInterval = ExecConstants.SPILL_DISK_SPACE_CHECK_INTERVAL.getDefault().getNumVal();
    }

    try {
      spillService.makeSpillSubdirs(id);
    } catch (UserException e) {
      throw UserException.dataWriteError(e)
        .addContext("Caller", caller)
        .build(logger);
    }
  }

  public SpillFile getSpillFile(String fileName) throws RuntimeException {
    try {
      final SpillDirectory spillDirectory = spillService.getSpillSubdir(id);
      return new SpillFile(spillDirectory.getFileSystem(), new Path(spillDirectory.getSpillDirPath(), fileName));
    } catch (UserException e) {
      throw UserException.dataWriteError(e)
        .addContext("for %s spill id %s", caller, id)
        .addContext("Caller", caller)
        .build(logger);
    }
  }

  @Override
  public void close() throws Exception {
    spillService.deleteSpillSubdirs(id);
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

    public FSDataOutputStream append() throws IOException {
      return fs.append(path);
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

  private static boolean enabledHealthCheck(String scheme) {
    return DREMIO_LOCAL_SCHEME.equals(scheme) || LOCAL_SCHEMA.equals(scheme);
  }

  private UserException.Builder withContextParameters(UserException.Builder uexBuilder) {
    uexBuilder.addContext("minDiskSpace", minDiskSpace);
    uexBuilder.addContext("minDiskSpacePercentage", minDiskSpacePercentage);
    uexBuilder.addContext("healthCheckInterval", healthCheckInterval);

    return uexBuilder;
  }

}
