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
 * Otherwise the version will move backward from version till 0.
 * version, version - 1, .. 0
 */
public class DeltaMetadataFetchJobProducer {

  public FileSystem fs;
  public SabotContext context;
  public Path metaDir;
  public Long version;
  public Long subparts;
  public boolean readLatest;

  private long startTimeStamp;
  private long startVersion;

  DeltaMetadataFetchJobProducer(SabotContext context, FileSystem fs, Path metaDir, Long version, long subparts, boolean readLatest) {
    this.fs = fs;
    this.context = context;
    this.metaDir = metaDir;
    this.version = version;
    this.subparts = subparts;
    this.readLatest = readLatest;
    startTimeStamp = System.currentTimeMillis();
    this.startVersion = version;
  }

  public boolean hasNext() {
    /*
    If readLatest then next version to read is version + 1.
    Producer doesn't know weather the version + 1 exists or not.
    That check is performed in DeltaMetadataFetchJob
    */

    if(readLatest) {
      return true;
    }

    /*
    If readLatest is false versions are moving backwards.
    The smallest version to read is 0.
    */
    if(version >= 0) {
      return true;
    }

    return false;
  }

  public DeltaMetadataFetchJob next() {
    if(!hasNext()) {
      throw new IllegalStateException("Cannot produce new Job. Iterator completed");
    }
    boolean readCheckpoint = getTryCheckpointReadFlag();
    Long currentVersion = version;
    Long currentSubparts = subparts;
    moveToNextVersion();
    return new DeltaMetadataFetchJob(context, metaDir, fs, startTimeStamp, readCheckpoint, currentVersion, currentSubparts);
  }


  public long currentVersion() {
    return version;
  }

  private void moveToNextVersion() {
    if (readLatest) {
      ++version;
    } else {
      --version;
    }
    subparts = 1L;
  }

  private boolean getTryCheckpointReadFlag() {
    if(version == startVersion && readLatest) {
      //While moving forward first file is always a checkpoint
      return true;
    }
    else if(version > startVersion && readLatest) {
      //While moving forward all files other than the first are commit json/
      return false;
    }
    else {
      //Moving backward so don't know weather the file is checkpoint or parquet.
      return true;
    }
  }


}
