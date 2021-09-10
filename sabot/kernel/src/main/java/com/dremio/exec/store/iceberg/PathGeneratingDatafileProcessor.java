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

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.PathGeneratingDataFileProcessor;

/**
 * Datafile processor implementation which generates paths from data files
 */
public class PathGeneratingDatafileProcessor implements DatafileProcessor {
  private VarCharVector datafilePaths;
  private VarBinaryVector icebergPartitionDataVector;
  private PartitionSpec icebergPartitionSpec;
  private boolean doneWithCurrentDatafile;

  @Override
  public void setup(VectorAccessible incoming, VectorAccessible outgoing) {
    datafilePaths = (VarCharVector) getVectorFromSchemaPath(outgoing, PathGeneratingDataFileProcessor.OUTPUT_SCHEMA.DATAFILE_PATH);
    icebergPartitionDataVector = (VarBinaryVector) getVectorFromSchemaPath(outgoing, PathGeneratingDataFileProcessor.OUTPUT_SCHEMA.PARTITION_DATA_PATH);
  }

  @Override
  public void initialise(PartitionSpec partitionSpec) {
    icebergPartitionSpec = partitionSpec;
  }

    @Override
  public int processDatafile(DataFile currentDataFile, int startOutIndex, int maxOutputCount) throws IOException {
    if (!shouldProcessCurrentDatafile(maxOutputCount)) {
      return 0;
    }
    String modifiedPath = currentDataFile.path().toString();
    byte[] path = modifiedPath.getBytes();
    datafilePaths.setSafe(startOutIndex, path);
    icebergPartitionDataVector.setSafe(startOutIndex,
      IcebergSerDe.serializeToByteArray(IcebergPartitionData.fromStructLike(icebergPartitionSpec, currentDataFile.partition())));
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
