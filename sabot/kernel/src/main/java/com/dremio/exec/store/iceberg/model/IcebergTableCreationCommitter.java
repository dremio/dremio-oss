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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;

import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Class used to commit CTAS operation
 */
public class IcebergTableCreationCommitter implements IcebergOpCommitter {
  private final List<ManifestFile> manifestFileList = new ArrayList<>();
  private final IcebergCommand icebergCommand;
  private final OperatorStats operatorStats;

  public IcebergTableCreationCommitter(String tableName, BatchSchema batchSchema, List<String> partitionColumnNames,
                                       IcebergCommand icebergCommand, Map<String, String> tableParameters, OperatorStats operatorStats) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    Preconditions.checkState(batchSchema != null, "Schema must be present");
    Preconditions.checkState(tableName != null, "Table name must be present");
    this.icebergCommand = icebergCommand;
    this.icebergCommand.beginCreateTableTransaction(tableName, batchSchema, partitionColumnNames, tableParameters);
    this.operatorStats = operatorStats;
  }

  public IcebergTableCreationCommitter(String tableName, BatchSchema batchSchema, List<String> partitionColumnNames, IcebergCommand icebergCommand, OperatorStats operatorStats) {
    this(tableName, batchSchema, partitionColumnNames, icebergCommand, Collections.emptyMap(), operatorStats);
  }

  @Override
  public Snapshot commit() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    icebergCommand.beginInsert();
    icebergCommand.consumeManifestFiles(manifestFileList);
    icebergCommand.finishInsert();
    Snapshot snapshot = icebergCommand.endCreateTableTransaction();
    long totalCommitTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    /* OperatorStats are null when create empty table is executed via Coordinator*/
    if(operatorStats != null) {
      operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_COMMIT_TIME, totalCommitTime);
    }
    return snapshot;
  }

  @Override
  public void consumeManifestFile(ManifestFile icebergManifestFile) {
    manifestFileList.add(icebergManifestFile);
  }

  @Override
  public void consumeDeleteDataFile(DataFile icebergDeleteDatafile) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Delete data file operation not allowed in Create table Transaction");
  }

  public void updateSchema(BatchSchema newSchema) {
    throw new UnsupportedOperationException("Updating schema is not supported for Creation table Transaction");
  }

  @Override
  public String getRootPointer() {
    return icebergCommand.getRootPointer();
  }

  @Override
  public Map<Integer, PartitionSpec> getCurrentSpecMap() {
    return icebergCommand.getPartitionSpecMap();
  }
}
