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

import static com.dremio.exec.store.IcebergExpiryMetric.ICEBERG_COMMIT_TIME;
import static com.dremio.exec.store.IcebergExpiryMetric.NUM_EXPIRED_SNAPSHOTS;
import static com.dremio.exec.store.IcebergExpiryMetric.NUM_TABLE_EXPIRY;
import static com.dremio.exec.store.IcebergExpiryMetric.NUM_TOTAL_SNAPSHOTS;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.store.IcebergExpiryMetric;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for expiring the snapshots for a given table, and identifying live/expired snapshots
 * based on the snapshot mode. Extending classes should implement the logic for setting up {@link
 * IcebergExpiryAction} and make noMoreActions to true when there's nothing more to produce.
 */
public abstract class IcebergExpirySnapshotsReader implements RecordReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergExpirySnapshotsReader.class);
  private static final byte[] METADATA =
      IcebergFileType.METADATA_JSON.name().getBytes(StandardCharsets.UTF_8);

  private Iterator<SnapshotEntry> snapshotsIterator = Collections.emptyIterator();
  protected Iterator<String> metadataPathsToRetain = Collections.emptyIterator();

  private VarCharVector metadataFilePathOutVector;
  private VarCharVector manifestListOutVector;
  private BigIntVector snapshotIdOutVector;
  private VarCharVector filePathVector;
  private VarCharVector fileTypeVector;

  protected final OpProps props;
  protected final SupportsIcebergMutablePlugin icebergMutablePlugin;
  protected final OperatorContext context;
  protected final SnapshotsScanOptions snapshotsScanOptions;
  protected volatile FileSystem fs;
  protected volatile FileIO io;

  protected OutputMutator output;

  protected boolean noMoreActions;
  protected IcebergExpiryAction currentExpiryAction;

  public IcebergExpirySnapshotsReader(
      OperatorContext context,
      SupportsIcebergMutablePlugin icebergMutablePlugin,
      OpProps props,
      SnapshotsScanOptions snapshotsScanOptions) {
    this.context = context;
    this.icebergMutablePlugin = icebergMutablePlugin;
    this.props = props;
    this.snapshotsScanOptions = snapshotsScanOptions;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;

    metadataFilePathOutVector = (VarCharVector) output.getVector(SystemSchemas.METADATA_FILE_PATH);
    manifestListOutVector = (VarCharVector) output.getVector(SystemSchemas.MANIFEST_LIST_PATH);
    snapshotIdOutVector = (BigIntVector) output.getVector(SystemSchemas.SNAPSHOT_ID);
    filePathVector = (VarCharVector) output.getVector(SystemSchemas.FILE_PATH);
    fileTypeVector = (VarCharVector) output.getVector(SystemSchemas.FILE_TYPE);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  @Override
  public int next() {
    int recordCount;
    int startIndex = 0;
    int totalRecordCount = 0;
    while ((recordCount = nextBatch(startIndex, context.getTargetBatchSize())) != 0) {
      totalRecordCount += recordCount;
      startIndex = totalRecordCount;
    }
    return totalRecordCount;
  }

  public int nextBatch(int startOutIndex, int maxOutIndex) {
    while (noPendingRecords()) {
      processNextTable();
    }

    int outIndex = startOutIndex;

    while (snapshotsIterator.hasNext() && outIndex < maxOutIndex) {
      SnapshotEntry snapshot = snapshotsIterator.next();
      metadataFilePathOutVector.setSafe(
          outIndex, snapshot.getMetadataJsonPath().getBytes(StandardCharsets.UTF_8));
      snapshotIdOutVector.setSafe(outIndex, snapshot.getSnapshotId());
      manifestListOutVector.setSafe(
          outIndex, snapshot.getManifestListPath().getBytes(StandardCharsets.UTF_8));
      outIndex++;
    }

    while (metadataPathsToRetain.hasNext() && outIndex <= maxOutIndex) {
      filePathVector.setSafe(
          outIndex, metadataPathsToRetain.next().getBytes(StandardCharsets.UTF_8));
      fileTypeVector.setSafe(outIndex, METADATA);
      outIndex++;
    }

    int valueCount = outIndex - startOutIndex;
    int lastOutIndex = outIndex;
    output.getVectors().forEach(v -> v.setValueCount(lastOutIndex));
    return valueCount;
  }

  protected void processNextTable() {
    currentExpiryAction = null;

    if (noMoreActions) {
      return;
    }

    setupNextExpiryAction();

    if (currentExpiryAction == null) {
      return;
    }

    metadataPathsToRetain = currentExpiryAction.getMetadataPathsToRetain().iterator();

    if (currentExpiryAction.getTotalSnapshotsCount() == 0) {
      return;
    }

    List<SnapshotEntry> snapshots;
    SnapshotsScanOptions.Mode mode = snapshotsScanOptions.getMode();
    switch (mode) {
      case EXPIRED_SNAPSHOTS:
        snapshots = currentExpiryAction.getExpiredSnapshots();
        addLongStat(NUM_EXPIRED_SNAPSHOTS, snapshots.size());
        break;
      case LIVE_SNAPSHOTS:
        // Commit expire operation
        snapshots = currentExpiryAction.getRetainedSnapshots();
        long expiredSnapshotsCnt = currentExpiryAction.getTotalSnapshotsCount() - snapshots.size();
        if (expiredSnapshotsCnt > 0) {
          addLongStat(NUM_TABLE_EXPIRY, 1L);
          addLongStat(NUM_EXPIRED_SNAPSHOTS, currentExpiryAction.getExpiredSnapshots().size());
        }
        break;
      case ALL_SNAPSHOTS:
        snapshots = currentExpiryAction.getAllSnapshotEntries();
        addLongStat(NUM_EXPIRED_SNAPSHOTS, 0L);
        break;
      default:
        throw new IllegalStateException(String.format("Unknown Snapshots scan mode: %s", mode));
    }
    addLongStat(NUM_TOTAL_SNAPSHOTS, currentExpiryAction.getTotalSnapshotsCount());
    addLongStat(ICEBERG_COMMIT_TIME, currentExpiryAction.getTimeElapsedForExpiry());

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Mode {}, Table {}, Snapshot ids: [{}]",
          mode.name(),
          currentExpiryAction.getTableName(),
          snapshots.stream()
              .map(se -> String.valueOf(se.getSnapshotId()))
              .collect(Collectors.joining(",")));
    }
    snapshotsIterator = snapshots.iterator();
  }

  protected abstract void setupNextExpiryAction();

  protected boolean noPendingRecords() {
    return !noMoreActions && !snapshotsIterator.hasNext() && !metadataPathsToRetain.hasNext();
  }

  @Override
  public void close() {
    context.getStats().setReadIOStats();
  }

  protected void addLongStat(IcebergExpiryMetric metric, long val) {
    context.getStats().addLongStat(metric, val);
  }

  protected synchronized void setupFsIfNecessary(String path) {
    if (this.fs != null) {
      return; // Already initialized
    }

    try {
      this.fs = icebergMutablePlugin.createFS(path, props.getUserName(), context);
      this.io = icebergMutablePlugin.createIcebergFileIO(this.fs, context, null, null, null);
    } catch (Exception e) {
      LOGGER.error("Failed to initialize the file system with error message {}.", e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
