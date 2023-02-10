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
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.sabot.op.writer.WriterCommitterOutputHandler;
import com.google.protobuf.ByteString;

/**
 * Implementations of this interface commit an iceberg transaction
 */
public interface IcebergOpCommitter {
  String CONCURRENT_DML_OPERATION_ERROR = "Concurrent DML operation has updated the table, please retry.";

  /**
   * Commits the Iceberg operation
   * @Param Access to output handler, for cases when committer wants to write custom output
   * @return new Snapshot that gets created as part of commit operation
   */
  default Snapshot commit(WriterCommitterOutputHandler outputHandler) {
    return commit();
  }

  /**
   * Commits the Iceberg operation
   * @Param outgoing Access to outgoing container in case committer wants to write custom output
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
   * Stores data file path to delete during commit operation
   * @param icebergDeleteDatafilePath The path to data file to delete from table
   * @throws UnsupportedOperationException
   */
  void consumeDeleteDataFilePath(String icebergDeleteDatafilePath) throws UnsupportedOperationException;

  /**
   * Stores the manifest file instance to include during commit operation
   * @param manifestFile ManifestFile instance to include in table
   */
  void consumeManifestFile(ManifestFile manifestFile);

  /**
   * Stores the data file instance to include during the rewrite operation.
   * Consuming data files is not supported where consuming manifests are possible.
   *
   * @param addDataFile
   * @throws UnsupportedOperationException
   */
  default void consumeAddDataFile(DataFile addDataFile) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

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

  /**
   * Gets the current Schema of the table
   * @return current schema of the table
   */
  Schema getCurrentSchema();

  /**
   * Checks is iceberg table is updated or not
   * @return  true when the committer detectects iceberg table root pointer
   */
  boolean isIcebergTableUpdated();

  default void updateReadSignature(ByteString newReadSignature) {}

  /**
   * Cleanup in case of exceptions during commit
   */
  default void cleanup(DremioFileIO dremioFileIO) {}
}
