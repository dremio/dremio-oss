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

package com.dremio.exec.store.deltalake;

import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * Given a version and readLatest will generate a {@link DeltaMetadataFetchJob} which attempts to read that
 * version commit or json file. If readLatest is true then the versions generated will move forward.
 * version, version + 1 ..
 * Otherwise, the version will move backward from version till 0.
 * version, version - 1, .. 0
 */
public class DeltaMetadataFetchJobProducer {

  private final FileSystem fs;
  private final SabotContext context;
  private final Path metadataDir;
  private final DeltaVersion startVersion;
  private long version;
  private int subparts;


  DeltaMetadataFetchJobProducer(SabotContext context, FileSystem fs, Path metadataDir, DeltaVersion version) {
    this.fs = fs;
    this.context = context;
    this.metadataDir = metadataDir;
    this.startVersion = version;
    this.version = version.getVersion();
    this.subparts = version.getSubparts();
  }

  public boolean hasNext() {
    /*
    If reading from checkpoint then next version to read is version + 1.
    Producer doesn't know whether the version + 1 exists or not.
    That check is performed in DeltaMetadataFetchJob
    */

    if (startVersion.isCheckpoint()) {
      return true;
    }

    /*
    If reading from commit then versions are moving backwards (to checkpoint or 0).
    The smallest version to read is 0.
    */
    return version >= 0;
  }

  public DeltaMetadataFetchJob next() {
    if (!hasNext()) {
      throw new IllegalStateException("Cannot produce new Job. Iterator completed");
    }
    DeltaVersion currentVersion = DeltaVersion.of(version, subparts, getTryCheckpointReadFlag());
    moveToNextVersion();
    return new DeltaMetadataFetchJob(context, metadataDir, fs, currentVersion);
  }

  public long currentVersion() {
    return version;
  }

  private void moveToNextVersion() {
    if (startVersion.isCheckpoint()) {
      ++version;
    } else {
      --version;
    }
    subparts = 1;
  }

  private boolean getTryCheckpointReadFlag() {
    if (startVersion.isCheckpoint()) {
      //While moving forward first file is always a checkpoint
      //While moving forward all files other than the first are commit json/
      return version == startVersion.getVersion();
    } else {
      //Moving backward so don't know whether the file is checkpoint or parquet.
      return true;
    }
  }
}
