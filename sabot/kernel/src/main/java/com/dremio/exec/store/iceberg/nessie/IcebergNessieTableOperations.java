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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.model.Reference;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.plugins.NessieClient;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.google.common.base.Stopwatch;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Iceberg nessie table operations
 */
class IcebergNessieTableOperations extends BaseMetastoreTableOperations {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergNessieTableOperations.class);
  private final FileIO fileIO;
  private final NessieClient nessieClient;
  private final IcebergNessieTableIdentifier nessieTableIdentifier;
  private Reference reference;
  private final OperatorStats operatorStats;

  public IcebergNessieTableOperations(OperatorStats operatorStats, NessieClient nessieClient, FileIO fileIO, IcebergNessieTableIdentifier nessieTableIdentifier) {
    this.fileIO = fileIO;
    this.nessieClient = nessieClient;
    this.nessieTableIdentifier = nessieTableIdentifier;
    this.reference = null;
    this.operatorStats = operatorStats;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return nessieTableIdentifier.toString();
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = nessieClient.getMetadataLocation(
      getNessieKey(nessieTableIdentifier.getTableIdentifier()),
      getDefaultBranch());
    refreshFromMetadataLocation(metadataLocation, 2);
  }

  private List<String> getNessieKey(TableIdentifier tableIdentifier) {
    return Arrays.asList(
      tableIdentifier.namespace().toString(),
      tableIdentifier.name());
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    Stopwatch stopwatchWriteNewMetadata = Stopwatch.createStarted();
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    long totalMetadataWriteTime = stopwatchWriteNewMetadata.elapsed(TimeUnit.MILLISECONDS);
    if (operatorStats != null) {
      operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_METADATA_WRITE_TIME, totalMetadataWriteTime);
    }
    boolean threw = true;
    try {
      Stopwatch stopwatchCatalogUpdate = Stopwatch.createStarted();
      nessieClient.commitOperation(getNessieKey(nessieTableIdentifier.getTableIdentifier()),
        newMetadataLocation,
        metadata,
        getDefaultBranch());
      threw = false;
      long totalCatalogUpdateTime = stopwatchCatalogUpdate.elapsed(TimeUnit.MILLISECONDS);
      if (operatorStats != null) {
        operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_CATALOG_UPDATE_TIME, totalCatalogUpdateTime);
      }
    } catch (StatusRuntimeException sre) {
      if (sre.getStatus().getCode() == Status.Code.ABORTED) {
        logger.debug(String.format("Commit failed: Reference hash is out of date. " +
            "Update the reference %s and try again for table %s",
          reference.getHash(), nessieTableIdentifier));
        throw new CommitFailedException(sre, "Commit failed: Reference hash is out of date. " +
          "Update the reference %s and try again", reference.getHash());
      } else {
        throw UserException.dataReadError(sre).build(logger);
      }
    } finally {
      if (threw) {
        logger.debug(String.format("Deleting metadata file %s of table %s", nessieTableIdentifier, newMetadataLocation));
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  public void deleteKey(){
   nessieClient.deleteCatalogEntry(
     getNessieKey(nessieTableIdentifier.getTableIdentifier()),
     getDefaultBranch());
  }

  private ResolvedVersionContext getDefaultBranch() {
    try {
      return nessieClient.getDefaultBranch();
    } catch (NoDefaultBranchException e) {
      throw UserException.sourceInBadState(e)
        .message("No default branch set.")
        .buildSilently();
    }
  }

}
