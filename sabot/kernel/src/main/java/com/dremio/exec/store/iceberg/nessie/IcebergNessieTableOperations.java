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

import static com.dremio.plugins.NessieUtils.createNamespaceInDefaultBranchIfNotExists;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.VersionedPlugin.EntityType;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.options.OptionManager;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClientImpl;
import com.dremio.plugins.NessieContent;
import com.dremio.plugins.NessieTableAdapter;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.google.common.base.Stopwatch;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Provider;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;

/** Iceberg nessie table operations */
public class IcebergNessieTableOperations extends BaseMetastoreTableOperations
    implements IcebergCommitOriginAwareTableOperations {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(IcebergNessieTableOperations.class);
  private final FileIO fileIO;
  private final Provider<NessieApiV2> nessieApi;
  private final IcebergNessieTableIdentifier nessieTableIdentifier;
  private final List<String> nessieTableKey;
  private @Nullable IcebergCommitOrigin commitOrigin;
  private ResolvedVersionContext reference;
  private final OperatorStats operatorStats;
  private final OptionManager optionManager;
  private String baseContentId;

  public IcebergNessieTableOperations(
      OperatorStats operatorStats,
      Provider<NessieApiV2> nessieApi,
      FileIO fileIO,
      IcebergNessieTableIdentifier nessieTableIdentifier,
      @Nullable IcebergCommitOrigin commitOrigin,
      OptionManager optionManager) {
    this.fileIO = fileIO;
    this.nessieApi = nessieApi;
    this.nessieTableIdentifier = nessieTableIdentifier;
    this.nessieTableKey = parseNessieKey(nessieTableIdentifier.getTableIdentifier());
    this.commitOrigin = commitOrigin;
    this.reference = null;
    this.operatorStats = operatorStats;
    this.optionManager = optionManager;
  }

  private NessieClient nessieClient() {
    // Avoid holding a NessieApi instance in this class as it may cause the Nessie client to be held
    // too long and
    // be used when the underlying connection is no longer valid. Instead, re-create the client on
    // demand.
    // Provider implementations are expected to cache expensive objects where appropriate.
    return new NessieClientImpl(nessieApi.get(), optionManager);
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return nessieTableIdentifier.toString();
  }

  @WithSpan
  @Override
  protected void doRefresh() {
    if (baseContentId == null) { // check and create namespace on first refresh only
      // Note: parseNessieKey(...) below ensures that nessieTableKey always has two components,
      // the first being the namespace (single level).
      createNamespaceInDefaultBranchIfNotExists(nessieApi, nessieTableKey.get(0));
    }

    NessieClient nessieClient = nessieClient();
    reference = getDefaultBranch(nessieClient);

    baseContentId = null;
    String metadataLocation = null;
    Optional<NessieContent> maybeNessieContent =
        nessieClient.getContent(nessieTableKey, reference, null);
    if (maybeNessieContent.isPresent()) {
      NessieContent nessieContent = maybeNessieContent.get();
      baseContentId = nessieContent.getContentId();
      metadataLocation =
          nessieContent
              .getMetadataLocation()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "No metadataLocation for iceberg table: "
                              + nessieTableKey
                              + " ref: "
                              + reference));
    }

    refreshFromMetadataLocation(metadataLocation, 2);
  }

  /**
   * refresh table from specified metadata version within table's nessie commit log.
   *
   * @param metadataLocation reference location of the metadata.
   */
  public void doRefreshFromPreviousCommit(String metadataLocation) {
    NessieClient nessieClient = nessieClient();
    reference = getDefaultBranch(nessieClient);

    baseContentId =
        nessieClient
            .getContent(nessieTableKey, reference, null)
            .map(NessieContent::getContentId)
            .orElse(null);
    if (baseContentId == null) {
      metadataLocation = null;
    }

    refreshFromMetadataLocation(metadataLocation, 2);
  }

  private static List<String> parseNessieKey(TableIdentifier tableIdentifier) {
    return Arrays.asList(tableIdentifier.namespace().toString(), tableIdentifier.name());
  }

  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    Stopwatch stopwatchWriteNewMetadata = Stopwatch.createStarted();
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    long totalMetadataWriteTime = stopwatchWriteNewMetadata.elapsed(TimeUnit.MILLISECONDS);
    if (operatorStats != null) {
      operatorStats.addLongStat(
          WriterCommitterOperator.Metric.ICEBERG_METADATA_WRITE_TIME, totalMetadataWriteTime);
    }
    boolean threw = true;
    try {
      Stopwatch stopwatchCatalogUpdate = Stopwatch.createStarted();
      nessieClient()
          .commitTable(
              nessieTableKey,
              newMetadataLocation,
              new NessieTableAdapter(
                  metadata.currentSnapshot().snapshotId(),
                  metadata.currentSchemaId(),
                  metadata.defaultSpecId(),
                  metadata.sortOrder().orderId()),
              reference,
              baseContentId,
              commitOrigin,
              null,
              null);
      threw = false;
      long totalCatalogUpdateTime = stopwatchCatalogUpdate.elapsed(TimeUnit.MILLISECONDS);
      if (operatorStats != null) {
        operatorStats.addLongStat(
            WriterCommitterOperator.Metric.ICEBERG_CATALOG_UPDATE_TIME, totalCatalogUpdateTime);
      }
    } catch (ReferenceConflictException e) {
      if (e.getCause() instanceof NessieConflictException) {
        logger.debug(
            String.format(
                "Retrying commit for table %s because of conflict exception", nessieTableKey));
        throw new CommitFailedException(e.getCause());
      } else {
        throw new IllegalStateException(e);
      }
    } finally {
      if (threw) {
        logger.debug(
            String.format(
                "Deleting metadata file %s of table %s",
                nessieTableIdentifier, newMetadataLocation));
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  public void deleteKey() {
    NessieClient nessieClient = nessieClient();
    ResolvedVersionContext version = getDefaultBranch(nessieClient);
    nessieClient.deleteCatalogEntry(nessieTableKey, EntityType.ICEBERG_TABLE, version, null);
  }

  private ResolvedVersionContext getDefaultBranch(NessieClient nessieClient) {
    try {
      return nessieClient.getDefaultBranch();
    } catch (NoDefaultBranchException e) {
      throw UserException.sourceInBadState(e).message("No default branch set.").buildSilently();
    }
  }

  @Override
  public void tryNarrowCommitOrigin(
      @Nullable IcebergCommitOrigin oldOrigin, IcebergCommitOrigin newOrigin) {
    if (this.commitOrigin == oldOrigin) {
      this.commitOrigin = newOrigin;
    }
  }
}
