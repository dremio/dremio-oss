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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

public class IcebergNessieVersionedTableOperations extends BaseMetastoreTableOperations {

  private static final Logger logger = LoggerFactory.getLogger(IcebergNessieTableOperations.class);

  private final FileIO fileIO;
  private final NessieApiV1 nessieClient;
  private final OperatorStats operatorStats;
  private final ContentsKey contentsKey;
  private final Reference versionRef;

  public IcebergNessieVersionedTableOperations(OperatorStats operatorStats, FileIO fileIO,
                                               NessieApiV1 nessieClient, ContentsKey contentsKey,
                                               Reference versionRef) {
    Preconditions.checkNotNull(nessieClient);
    Preconditions.checkNotNull(versionRef);
    Preconditions.checkNotNull(contentsKey);
    this.fileIO = fileIO;
    this.nessieClient = nessieClient;
    this.contentsKey = contentsKey;
    this.versionRef = versionRef;
    this.operatorStats = operatorStats;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    try {
      Map<ContentsKey, Contents> map = nessieClient.getContents().key(contentsKey).refName(versionRef.getName()).get();
      Contents contents = map.get(contentsKey);
      if (contents instanceof IcebergTable) {
        IcebergTable icebergTable = (IcebergTable) contents;
        metadataLocation = icebergTable.getMetadataLocation();
        logger.debug("Metadata location of table: {}, is {}", contentsKey, metadataLocation);
      }
    } catch (NessieNotFoundException e) {
      if (e.getErrorCode() == ErrorCode.REFERENCE_NOT_FOUND || e.getErrorCode() != ErrorCode.CONTENTS_NOT_FOUND) {
        throw UserException.dataReadError(e).buildSilently();
      }
      logger.debug("Metadata location was not found for table: {}", contentsKey);
    }
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
      nessieClient.commitMultipleOperations()
        .branchName(versionRef.getName())
        .hash(versionRef.getHash())
        .operation(Operation.Put.of(contentsKey, IcebergTable.of(newMetadataLocation, "x")))
        .commitMeta(CommitMeta.fromMessage("Put key: " + contentsKey))
        .commit();

      threw = false;
      long totalCatalogUpdateTime = stopwatchCatalogUpdate.elapsed(TimeUnit.MILLISECONDS);
      if(operatorStats != null) {
        operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_CATALOG_UPDATE_TIME, totalCatalogUpdateTime);
      }
    } catch (NessieConflictException e) {
      throw new CommitFailedException(e, "Failed to commit operation");
    } catch (NessieNotFoundException e) {
      throw UserException.dataReadError(e).buildSilently();
    } finally {
      if (threw) {
        logger.debug("Deleting metadata file {} of table {}", contentsKey, newMetadataLocation);
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  public void deleteKey() throws NessieConflictException, NessieNotFoundException {
    nessieClient.commitMultipleOperations()
      .branchName(versionRef.getName())
      .hash(versionRef.getHash())
      .operation(Operation.Delete.of(contentsKey))
      .commitMeta(CommitMeta.fromMessage("Deleting key: " + contentsKey))
      .commit();
  }
}
