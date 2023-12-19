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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.Snapshot;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.expressions.IsNotNull;
import io.delta.standalone.types.StructType;

public class DeltaTableIntegrationTestUtils {

  private String tablePath;
  private Configuration conf;
  private DeltaLog log;

  public DeltaTableIntegrationTestUtils(Configuration conf, String tablePath) {
    this.conf = conf;
    this.tablePath = tablePath;
    this.log = DeltaLog.forTable(conf, tablePath);
  }

  // Read functionalities
  /**
  * Returns the snapshot
  * */
  public Snapshot getSnapshot() {
    return log.snapshot();
  }

  /**
   * Returns the snapshot for version
   * @param version version for the snapshot
   */
  public Snapshot getSnapshotForVersionAsOf(long version) {
    return log.getSnapshotForVersionAsOf(version);
  }

  /**
   * Returns the snapshot for a timestamp
   * @param timestamp timestamp for the snapshot
   */
  public Snapshot getSnapshotForTimestampAsOf(long timestamp) {
    return log.getSnapshotForTimestampAsOf(timestamp);
  }

  /**
   * Returns the current version
   * */
  public long getVersion() {
    return log.update().getVersion();
  }

  // Write functionalities
  public void doAddFilesCommit() throws IOException {

  OptimisticTransaction txn = log.startTransaction();
    StructType currentSchema = log.update().getMetadata().getSchema();
    String firstColumnName = currentSchema.getFieldNames()[0];
    DeltaScan scan = txn.markFilesAsRead(new IsNotNull(currentSchema.column(firstColumnName)));

    // add the first file to do a new commit
    CloseableIterator<AddFile> iter = scan.getFiles();
    List<AddFile> addFiles = new ArrayList<>();
    if (iter == null || !iter.hasNext()) {
      throw new IOException(String.format("Cannot get File List for Delta Table: %s", tablePath));
    } else {
      AddFile addFile = iter.next();
      addFiles.add(addFile);
    }
    iter.close();

    txn.commit(addFiles, new Operation(Operation.Name.UPDATE), "Zippy/1.0.0");
  }

  public List<CommitInfo> getHistory() {
    List<CommitInfo> history = new ArrayList<>();
    Iterator<VersionLog> verItr = log.getChanges(0, false);
    while (verItr.hasNext()) {
      history.add(log.getCommitInfoAt(verItr.next().getVersion()));
    }
    return history;
  }
}
