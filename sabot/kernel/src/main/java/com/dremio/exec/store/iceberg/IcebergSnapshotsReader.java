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

import static com.dremio.exec.store.iceberg.model.IcebergOpCommitter.CONCURRENT_DML_OPERATION_ERROR;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Stopwatch;

/**
 * Output a list of snapshot ids accordingly.
 */
public class IcebergSnapshotsReader implements RecordReader {
  private static final Logger logger = LoggerFactory.getLogger(IcebergSnapshotsReader.class);

  private final OperatorContext context;
  private final String metadataLocation;
  private final SupportsIcebergMutablePlugin icebergMutablePlugin;
  private final OpProps props;
  private final IcebergTableProps icebergTableProps;
  private final SnapshotsScanOptions snapshotsScanOptions;

  private OutputMutator output;
  private boolean emptyTable;
  private TableOperations ops;
  private Iterator<Map.Entry<Long, String>> snapshotIdsIterator;
  private VarCharVector metadataFilePathOutVector;
  private VarCharVector manifestListOutVector;
  private BigIntVector snapshotIdOutVector;

  public IcebergSnapshotsReader(OperatorContext context,
                                String metadataLocation,
                                SupportsIcebergMutablePlugin icebergMutablePlugin,
                                OpProps props,
                                IcebergTableProps icebergTableProps,
                                SnapshotsScanOptions snapshotsScanOptions) {
    this.context = context;
    this.metadataLocation = metadataLocation;
    this.icebergMutablePlugin = icebergMutablePlugin;
    this.props = props;
    this.icebergTableProps = icebergTableProps;
    this.snapshotsScanOptions = snapshotsScanOptions;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    FileSystem fs;
    try {
      // Do not use Async options, as it can result in different URI schemes - s3a -> dremioS3 -> s3
      fs = icebergMutablePlugin.createFS(this.metadataLocation, props.getUserName(), context);
    } catch (IOException e) {
      logger.info("Failed creating filesystem", e);
      throw new ExecutionSetupException("Failed creating filesystem", e);
    }

    IcebergModel icebergModel = icebergMutablePlugin.getIcebergModel(icebergTableProps, props.getUserName(), null, fs);
    IcebergTableIdentifier tableIdentifier = icebergModel.getTableIdentifier(icebergMutablePlugin.getTableLocation(icebergTableProps));
    BaseTable icebergTable = (BaseTable) icebergModel.getIcebergTableLoader(tableIdentifier).getIcebergTable();
    ops = icebergTable.operations();
    final TableMetadata tableMetadata = ops.current();
    if (tableMetadata.snapshots() == null || tableMetadata.snapshots().isEmpty()) {
      // Currently, this reader does not generate any output for empty Iceberg table. However, it might be better to
      // throw exception tell users that the Iceberg table is empty.
      emptyTable = true;
      return;
    }

    if (isIcebergTableUpdated(tableMetadata.metadataFileLocation())) {
      throw UserException.concurrentModificationError().message(CONCURRENT_DML_OPERATION_ERROR).buildSilently();
    }

    Stopwatch stopwatchIceberg = Stopwatch.createStarted();
    Map<Long, String> snapshotManifests;
    switch (snapshotsScanOptions.getMode()) {
      case EXPIRED_SNAPSHOTS:
        snapshotManifests = icebergTable.expireSnapshots()
          .expireOlderThan(snapshotsScanOptions.getOlderThanInMillis())
          .retainLast(snapshotsScanOptions.getRetainLast())
          .apply().stream().collect(Collectors.toMap(Snapshot::snapshotId, Snapshot::manifestListLocation));
        break;
      case LIVE_SNAPSHOTS:
        // Commit expire operation
        snapshotManifests = icebergModel.expireSnapshots(
          tableIdentifier, snapshotsScanOptions.getOlderThanInMillis(), snapshotsScanOptions.getRetainLast());
        break;
      default:
        throw new IllegalStateException(String.format("Unknown Snapshots scan mode: %s", snapshotsScanOptions.getMode()));
    }
    logger.info("Snapshot ids: {}", snapshotManifests);
    snapshotIdsIterator = snapshotManifests.entrySet().iterator();
    long totalExpireTime = stopwatchIceberg.elapsed(TimeUnit.MILLISECONDS);
    context.getStats().addLongStat(ScanOperator.Metric.ICEBERG_COMMIT_TIME, totalExpireTime);

    metadataFilePathOutVector = (VarCharVector) output.getVector(SystemSchemas.METADATA_FILE_PATH);
    manifestListOutVector = (VarCharVector) output.getVector(SystemSchemas.MANIFEST_LIST_PATH);
    snapshotIdOutVector = (BigIntVector) output.getVector(SystemSchemas.SNAPSHOT_ID);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  @Override
  public int next() {
    if (emptyTable || !snapshotIdsIterator.hasNext()) {
      return 0;
    }

    int outIndex = 0;
    while (snapshotIdsIterator.hasNext() && outIndex < context.getTargetBatchSize()) {
      Map.Entry<Long, String> snapshotManifest = snapshotIdsIterator.next();
      metadataFilePathOutVector.setSafe(outIndex, metadataLocation.getBytes(StandardCharsets.UTF_8));
      snapshotIdOutVector.setSafe(outIndex, snapshotManifest.getKey());
      manifestListOutVector.setSafe(outIndex, snapshotManifest.getValue().getBytes(StandardCharsets.UTF_8));
      outIndex++;
    }

    int valueCount = outIndex;
    output.getVectors().forEach(v -> v.setValueCount(valueCount));
    return valueCount;
  }

  @Override
  public void close() throws Exception {
    context.getStats().setReadIOStats();
  }

  private boolean isIcebergTableUpdated(String rootPointer) {
    return !Path.getContainerSpecificRelativePath(Path.of(rootPointer))
      .equals(Path.getContainerSpecificRelativePath(Path.of(metadataLocation)));
  }
}
