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

import com.dremio.exec.record.VectorAccessible;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DremioManifestReaderUtils.ManifestEntryWrapper;
import org.apache.iceberg.PartitionSpec;

/** Interface for specifying behaviors for processing Iceberg data files */
public interface ManifestEntryProcessor extends AutoCloseable {

  void setup(VectorAccessible incoming, VectorAccessible outgoing);

  default void initialise(PartitionSpec partitionSpec, int row) {}

  default void initialise(
      PartitionSpec partitionSpec,
      int row,
      Configuration conf,
      String fsScheme,
      String pathSchemeVariate) {
    initialise(partitionSpec, row);
  }

  int processManifestEntry(
      ManifestEntryWrapper<? extends ContentFile<?>> manifestEntry,
      int startOutIndex,
      int currentOutputCount)
      throws IOException;

  void closeManifestEntry();
}
