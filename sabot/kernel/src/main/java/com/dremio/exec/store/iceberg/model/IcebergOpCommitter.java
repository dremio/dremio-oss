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
package com.dremio.exec.store.iceberg.model;

import java.util.Map;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;

import com.dremio.exec.record.BatchSchema;
import com.google.protobuf.ByteString;

/**
 * Implementations of this interface commit an iceberg transaction
 */
public interface IcebergOpCommitter {
  /**
   * Commits the Iceberg operation
   * @return new Snapshot that gets created as part of commit operation
   */
  Snapshot commit();

  /**
   * Stores the DataFile instance to delete during commit operation
   * @param icebergDeleteDatafile DataFile instance to delete from table
   * @throws UnsupportedOperationException
   */
  void consumeDeleteDataFile(DataFile icebergDeleteDatafile) throws UnsupportedOperationException;

  /**
   * Stores the manifest file instance to include during commit operation
   * @param manifestFile ManifestFile instance to include in table
   */
  void consumeManifestFile(ManifestFile manifestFile);

  /**
   * Stores the new schema to use during commit operation
   * @param newSchema new schema of the table
   */
  void updateSchema(BatchSchema newSchema);

  /**
   * Gets the current root pointer of the table
   * @return current metadata location of the table
   */
  String getRootPointer();

  /**
   * Gets the current spec map of the table
   * @return current spec map of the table
   */
  Map<Integer, PartitionSpec> getCurrentSpecMap();

  default void updateReadSignature(ByteString newReadSignature) {}
}
