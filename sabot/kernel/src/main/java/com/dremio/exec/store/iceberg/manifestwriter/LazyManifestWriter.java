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

package com.dremio.exec.store.iceberg.manifestwriter;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;

public class LazyManifestWriter {
  private ManifestWriter<DataFile> manifestWriter = null;

  private final FileIO fileIO;
  private final String manifestLocation;
  private final PartitionSpec partitionSpec;

  public LazyManifestWriter(FileIO fileIO, String manifestLocation, PartitionSpec partitionSpec) {
    this.fileIO = fileIO;
    this.manifestLocation = manifestLocation;
    this.partitionSpec = partitionSpec;
  }

  public synchronized ManifestWriter<DataFile> getInstance() {
    if (manifestWriter == null) {
      final OutputFile manifestFile = fileIO.newOutputFile(manifestLocation);
      this.manifestWriter = ManifestFiles.write(partitionSpec, manifestFile);
    }
    return manifestWriter;
  }

  public boolean isInitialized() {
    return manifestWriter != null;
  }
}
