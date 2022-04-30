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
package com.dremio.exec.store.iceberg.nessie;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.plugins.NessieClient;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

public class IcebergNessieVersionedTableOperations extends BaseMetastoreTableOperations {

  private static final Logger logger = LoggerFactory.getLogger(IcebergNessieTableOperations.class);

  private final OperatorStats operatorStats;
  private final FileIO fileIO;
  private final NessieClient nessieClient;
  private final List<String> tableKey;
  private final String fullTableName;
  private final ResolvedVersionContext version;

  public IcebergNessieVersionedTableOperations(OperatorStats operatorStats,
                                               FileIO fileIO,
                                               NessieClient nessieClient,
                                               IcebergNessieVersionedTableIdentifier nessieVersionedTableIdentifier) {
    this.operatorStats = operatorStats;
    this.fileIO = fileIO;
    this.fullTableName = nessieVersionedTableIdentifier.getTableIdentifier().toString();
    this.nessieClient = Preconditions.checkNotNull(nessieClient);

    Preconditions.checkNotNull(nessieVersionedTableIdentifier);
    this.tableKey = nessieVersionedTableIdentifier.getTableKey();
    this.version = nessieVersionedTableIdentifier.getVersion();
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return fullTableName;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = nessieClient.getMetadataLocation(tableKey, version);
    refreshFromMetadataLocation(metadataLocation, 2);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    Stopwatch stopwatchWriteNewMetadata = Stopwatch.createStarted();
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    long totalMetadataWriteTime = stopwatchWriteNewMetadata.elapsed(TimeUnit.MILLISECONDS);
    if(operatorStats != null) {
      operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_METADATA_WRITE_TIME, totalMetadataWriteTime);
    }
    boolean threw = true;
    try {
      Stopwatch stopwatchCatalogUpdate = Stopwatch.createStarted();
      nessieClient.commitOperation(tableKey, newMetadataLocation, metadata, version);
      threw = false;
      long totalCatalogUpdateTime = stopwatchCatalogUpdate.elapsed(TimeUnit.MILLISECONDS);
      if (operatorStats != null) {
        operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_CATALOG_UPDATE_TIME, totalCatalogUpdateTime);
      }
    } finally {
      if (threw) {
        logger.debug("Deleting metadata file {} of table {}", tableKey, newMetadataLocation);
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  public void deleteKey() {
    nessieClient.deleteCatalogEntry(tableKey, version);
  }
}
