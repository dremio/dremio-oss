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

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table function to invoke the rewrite manifests optimization function. The table function is
 * pluggable to any input/output streams. The table information is picked from the context. The
 * input data is not used, but forwarded as is to the output streams. To avoid repeated rewrite
 * manifest attempts in every minor fragment, this TableFunction should only be used with a
 * Singleton Trait.
 */
public class OptimizeManifestsTableFunction extends AbstractTableFunction {
  @VisibleForTesting static final Function<DataFile, Object> NO_CLUSTERING_RULE = df -> 1;

  private static final Logger LOGGER =
      LoggerFactory.getLogger(OptimizeManifestsTableFunction.class);
  private final OpProps opProps;
  private final FragmentExecutionContext fec;
  private int outputRecords;

  public OptimizeManifestsTableFunction(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    this.opProps = props;
    this.fec = fec;
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) throws Exception {
    super.setup(incoming);

    return incoming; // outgoing = incoming
  }

  @Override
  public void startBatch(int records) throws Exception {
    this.outputRecords = records;
  }

  @Override
  public void noMoreToConsume() throws Exception {
    Stopwatch timer = Stopwatch.createStarted();
    try {
      rewriteManifests();
    } catch (Exception e) {
      LOGGER.error("Error while rewriting table manifests.", e);
      throw UserRemoteException.dataWriteError(e)
          .message("Error while rewriting table manifests.")
          .buildSilently();
    } finally {
      LOGGER.info("Time taken on rewrite manifests {}", timer.elapsed());
    }
  }

  private void rewriteManifests() throws ExecutionSetupException, IOException {
    // Set up IcebergModel
    OptimizeManifestsTableFunctionContext ctx =
        (OptimizeManifestsTableFunctionContext) functionConfig.getFunctionContext();
    SupportsIcebergMutablePlugin icebergMutablePlugin = fec.getStoragePlugin(ctx.getPluginId());
    IcebergTableProps tableProps = ctx.getIcebergTableProps();
    try (FileSystem fs =
        icebergMutablePlugin.createFS(
            tableProps.getTableLocation(), opProps.getUserName(), context)) {
      FileIO fileIO = icebergMutablePlugin.createIcebergFileIO(fs, null, null, null, null);
      IcebergModel icebergModel =
          icebergMutablePlugin.getIcebergModel(
              tableProps, opProps.getUserName(), context, fileIO, null);
      icebergModel.refreshVersionContext();
      IcebergTableIdentifier icebergTableIdentifier =
          icebergModel.getTableIdentifier(tableProps.getTableLocation());

      LOGGER.info("Attempting rewrite manifests");
      Table table = icebergModel.getIcebergTable(icebergTableIdentifier);

      if (table.currentSnapshot() == null) {
        LOGGER.info("Aborting rewrite manifests as the table has no snapshots");
        setCommitStatus(0);
        return;
      }

      RewriteManifests rewriteManifests =
          table
              .rewriteManifests()
              .rewriteIf(m -> isNotInOptimalSizeRange(m, icebergModel, icebergTableIdentifier))
              .clusterBy(NO_CLUSTERING_RULE);
      Snapshot newSnapshot = rewriteManifests.apply();

      if (hasNoManifestChanges(newSnapshot)) {
        cleanOrphans(table.io(), newSnapshot);
        LOGGER.info("Optimization of manifest files skipped");
        setCommitStatus(0);
        return;
      }

      try {
        rewriteManifests.commit();
      } catch (RuntimeException e) {
        try {
          cleanOrphans(table.io(), newSnapshot);
        } catch (Exception inner) {
          e.addSuppressed(inner);
        }
        throw e;
      }
      LOGGER.info(
          "Optimization of manifest files is successful with snapshot id {}",
          newSnapshot.snapshotId());
      setCommitStatus(1);
    }
  }

  @VisibleForTesting
  static void cleanOrphans(FileIO io, Snapshot snapshot) {
    snapshot.allManifests(io).stream()
        .filter(m -> m.snapshotId() == snapshot.snapshotId())
        .map(ManifestFile::path)
        .forEach(path -> tryDeleteOrphanFile(io, path));
    tryDeleteOrphanFile(io, snapshot.manifestListLocation());
  }

  private static void tryDeleteOrphanFile(FileIO io, String path) {
    try {
      io.deleteFile(path);
    } catch (RuntimeException e) {
      LOGGER.warn("Error while trying to clean up orphans", e);
      // Not throwing further as it's not a failure condition to leave the stale orphans.
    }
  }

  @VisibleForTesting
  static boolean hasNoManifestChanges(Snapshot snapshot) {
    // The iceberg implementation doesn't take care of skipping the NOOP cases. Hence, putting in
    // this custom
    // computation. NOOP if no manifests are created/replaced or the residual manifest was picked
    // and
    // was rewritten with the same content.
    Map<String, String> summary =
        Optional.ofNullable(snapshot).map(Snapshot::summary).orElseGet(ImmutableMap::of);
    String manifestsCreated = summary.get("manifests-created");
    String manifestsReplaced = summary.get("manifests-replaced");
    String totalDeleteManifestsStr = summary.get("total-delete-files");
    String totalDataManifestsStr = summary.get("total-data-files");

    int minDeleteManifests =
        totalDeleteManifestsStr != null && Integer.parseInt(totalDeleteManifestsStr) > 0 ? 1 : 0;
    int minDataFileManifests =
        totalDataManifestsStr != null && Integer.parseInt(totalDataManifestsStr) > 0 ? 1 : 0;

    int minManifests = minDeleteManifests + minDataFileManifests;

    return manifestsCreated == null
        || manifestsReplaced == null
        || (Integer.parseInt(manifestsCreated) == 0 && Integer.parseInt(manifestsReplaced) == 0)
        || (Integer.parseInt(manifestsCreated) == minManifests
            && Integer.parseInt(manifestsReplaced) == minManifests); // only residual manifests
  }

  private void setCommitStatus(long statusValue) {
    OperatorStats operatorStats = context.getStats();
    if (operatorStats != null) {
      operatorStats.addLongStat(TableFunctionOperator.Metric.SNAPSHOT_COMMIT_STATUS, statusValue);
    }
  }

  private boolean isNotInOptimalSizeRange(
      ManifestFile manifestFile,
      IcebergModel icebergModel,
      IcebergTableIdentifier icebergTableIdentifier) {
    long manifestTargetSizeBytes =
        icebergModel.propertyAsLong(
            icebergTableIdentifier, MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    long minManifestFileSize = (long) (manifestTargetSizeBytes * 0.75);
    long maxManifestFileSize = (long) (manifestTargetSizeBytes * 1.8);

    return manifestFile.length() < minManifestFileSize
        || manifestFile.length() > maxManifestFileSize;
  }

  @Override
  public void startRow(int row) throws Exception {
    // Do nothing
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    // Just indicate number of inputs as number of outputs. No action to be taken.
    int rowsProjected = this.outputRecords;
    this.outputRecords = 0; // reset to zero
    return rowsProjected;
  }

  @Override
  public void closeRow() throws Exception {
    // Do nothing
  }
}
