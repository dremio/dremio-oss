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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.io.IOException;

import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.DataFile;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordReader;
import com.dremio.io.file.Path;

/**
 * Datafile processor implementation which generates paths from data files
 */
public class PathGeneratingDatafileProcessor implements DatafileProcessor {
  private VarCharVector datafilePaths;
  private boolean doneWithCurrentDatafile;

  @Override
  public void setup(VectorAccessible incoming, VectorAccessible outgoing) {
    datafilePaths = (VarCharVector) getVectorFromSchemaPath(outgoing, RecordReader.DATAFILE_PATH);
  }

  @Override
  public int processDatafile(DataFile currentDataFile, int startOutIndex, int maxOutputCount) throws IOException {
    if (!shouldProcessCurrentDatafile(maxOutputCount)) {
      return 0;
    }

    String modifiedPath = Path.getContainerSpecificRelativePath(Path.of(currentDataFile.path().toString()));
    byte[] path = modifiedPath.getBytes();
    datafilePaths.setSafe(startOutIndex, path);
    doneWithCurrentDatafile = true;
    return 1;
  }

  @Override
  public void closeDatafile() {
    doneWithCurrentDatafile = false;
  }

  @Override
  public void close() throws Exception {
    // Nothing to close
  }

  private boolean shouldProcessCurrentDatafile(int maxOutputCount) {
    return !doneWithCurrentDatafile && maxOutputCount > 0;
  }
}
