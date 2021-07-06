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

import java.io.IOException;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;

import com.dremio.exec.record.VectorAccessible;

/**
 * Interface for specifying behaviors for processing Iceberg data files
 */
public interface DatafileProcessor extends AutoCloseable {

  void setup(VectorAccessible incoming, VectorAccessible outgoing);

  default void initialise(PartitionSpec partitionSpec) {
  }

  int processDatafile(DataFile dataFile, int startOutIndex, int currentOutputCount) throws IOException;

  void closeDatafile();
}
