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

import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;
import static org.apache.iceberg.hadoop.Util.VERSION_HINT_FILENAME;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Performs iceberg expiry operations. */
public class IcebergExpiryAction {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergExpiryAction.class);
  private static final String METADATA_FOLDER_NAME = "metadata";

  protected final VacuumOptions vacuumOptions;
  protected TableMetadata tableMetadata;
  protected final FileIO fileIO;
  protected final SupportsIcebergMutablePlugin icebergMutablePlugin;
  private final OpProps props;
  private final OperatorContext context;
  protected final String tableName;
  protected final String dbName;
  protected final ResolvedVersionContext versionContext;
  private final long timeElapsedForExpiry;

  private final List<Snapshot> allSnapshots;
  private List<SnapshotEntry> expiredSnapshots;
  protected List<SnapshotEntry> liveSnapshots;
  private List<String> metadataPathsToRetain;
  private final String schemeVariate;
  private final Configuration conf;
  private final String fsScheme;

  public IcebergExpiryAction(
      SupportsIcebergMutablePlugin icebergMutablePlugin,
      OpProps props,
      OperatorContext context,
      VacuumOptions vacuumOptions,
      final TableMetadata tableMetadata,
      String tableName,
      String dbName,
      ResolvedVersionContext versionContext,
      FileIO fileIO,
      boolean commitExpiry,
      String schemeVariate,
      String fsScheme) {
    this.icebergMutablePlugin = icebergMutablePlugin;
    this.props = props;
    this.context = context;
    this.tableName = tableName;
    this.dbName = dbName;
    this.versionContext = versionContext;
    this.tableMetadata = tableMetadata;
    this.fileIO = fileIO;
    this.vacuumOptions = vacuumOptions;
    this.schemeVariate = schemeVariate;
    this.conf = icebergMutablePlugin.getFsConfCopy();
    this.fsScheme = fsScheme;

    Stopwatch stopwatchIceberg = Stopwatch.createStarted();
    try {
      IcebergModel icebergModel =
          icebergMutablePlugin.getIcebergModel(
              getTableProps(), props.getUserName(), this.context, fileIO);
      IcebergTableIdentifier tableId = getTableIdentifier(icebergModel);
      this.allSnapshots = ImmutableList.copyOf(tableMetadata.snapshots());
      if (!commitExpiry) {
        if (vacuumOptions.isRemoveOrphans()) {
          this.liveSnapshots =
              allSnapshots.stream()
                  .map(s -> buildSnapshotEntry(tableMetadata.metadataFileLocation(), s))
                  .collect(Collectors.toList());
          Set<Long> liveSnapshotIds =
              liveSnapshots.stream().map(SnapshotEntry::getSnapshotId).collect(Collectors.toSet());
          this.expiredSnapshots =
              allSnapshots.stream()
                  .filter(s -> !liveSnapshotIds.contains(s.snapshotId()))
                  .map(s -> buildSnapshotEntry(tableMetadata.metadataFileLocation(), s))
                  .collect(Collectors.toList());
          this.metadataPathsToRetain = computeMetadataPathsToRetain(liveSnapshotIds);
        } else {
          // Collect expiry snapshots from TableMetadata.
          final long olderThanInMillis =
              getOlderThanInMillisWithTableProperties(
                  tableMetadata.properties(), vacuumOptions.getOlderThanInMillis());
          final int retainLast =
              getRetainLastWithTableProperties(
                  tableMetadata.properties(), vacuumOptions.getRetainLast());
          IcebergExpirySnapshotsCollector collector =
              new IcebergExpirySnapshotsCollector(tableMetadata);
          Pair<List<SnapshotEntry>, Set<Long>> candidates =
              collector.collect(olderThanInMillis, retainLast);
          this.expiredSnapshots = candidates.first();
          Set<Long> idsToRetain = candidates.second();
          this.liveSnapshots =
              allSnapshots.stream()
                  .filter(s -> idsToRetain.contains(s.snapshotId()))
                  .map(s -> buildSnapshotEntry(tableMetadata.metadataFileLocation(), s))
                  .collect(Collectors.toList());
          this.metadataPathsToRetain = Collections.emptyList();
        }
      } else {
        // Perform expiry on table and calculate live and expired snapshots
        Preconditions.checkState(
            vacuumOptions.isExpireSnapshots(), "Should be ExpireSnapshots status");
        if (allSnapshots.size() <= vacuumOptions.getRetainLast()) {
          // nothing to expire
          LOGGER.info(
              "Skipping {} because snapshot count <= {}", tableId, vacuumOptions.getRetainLast());
          this.liveSnapshots =
              allSnapshots.stream()
                  .map(s -> buildSnapshotEntry(tableMetadata.metadataFileLocation(), s))
                  .collect(Collectors.toList());
        } else {
          this.liveSnapshots =
              icebergModel.expireSnapshots(
                  tableId, vacuumOptions.getOlderThanInMillis(), vacuumOptions.getRetainLast());
          List<String> newMetadataLocation =
              liveSnapshots.stream()
                  .map(SnapshotEntry::getMetadataJsonPath)
                  .distinct()
                  .collect(Collectors.toList());
          Preconditions.checkState(newMetadataLocation.size() == 1);
          this.tableMetadata = TableMetadataParser.read(fileIO, newMetadataLocation.get(0));
        }

        Set<Long> liveSnapshotIds =
            liveSnapshots.stream().map(SnapshotEntry::getSnapshotId).collect(Collectors.toSet());
        this.expiredSnapshots =
            allSnapshots.stream()
                .filter(s -> !liveSnapshotIds.contains(s.snapshotId()))
                .map(s -> buildSnapshotEntry(tableMetadata.metadataFileLocation(), s))
                .collect(Collectors.toList());
        this.metadataPathsToRetain = computeMetadataPathsToRetain(liveSnapshotIds);
      }
    } finally {
      this.timeElapsedForExpiry = stopwatchIceberg.elapsed(TimeUnit.MILLISECONDS);
    }
  }

  protected List<String> computeMetadataPathsToRetain(Set<Long> liveSnapshotIds) {
    // Metadata files are only needed to be appended for Remove Orphan Files.
    if (!vacuumOptions.isRemoveOrphans()) {
      return Collections.emptyList();
    }

    // Old metadata is kept for history by default. The retention is determined by the table
    // properties
    // write.metadata.delete-after-commit.enabled and write.metadata.previous-versions-max during
    // writes.
    // Add version hint file location
    String versionHintFilePath =
        tableMetadata.location()
            + Path.SEPARATOR
            + METADATA_FOLDER_NAME
            + Path.SEPARATOR
            + VERSION_HINT_FILENAME;
    List<String> metadataFiles =
        Stream.concat(
                Stream.of(tableMetadata.metadataFileLocation(), versionHintFilePath),
                tableMetadata.previousFiles().stream().map(TableMetadata.MetadataLogEntry::file))
            .collect(Collectors.toList());
    return metadataFiles.stream().map(p -> getIcebergPath(p)).collect(Collectors.toList());
  }

  public List<SnapshotEntry> getRetainedSnapshots() {
    return liveSnapshots;
  }

  public List<Snapshot> getAllSnapshots() {
    return allSnapshots;
  }

  public List<SnapshotEntry> getAllSnapshotEntries() {
    return allSnapshots.stream()
        .map(s -> buildSnapshotEntry(tableMetadata.metadataFileLocation(), s))
        .collect(Collectors.toList());
  }

  public int getTotalSnapshotsCount() {
    return (allSnapshots == null) ? 0 : allSnapshots.size();
  }

  public List<SnapshotEntry> getExpiredSnapshots() {
    return expiredSnapshots;
  }

  public List<String> getMetadataPathsToRetain() {
    return metadataPathsToRetain;
  }

  public String getTableName() {
    return String.join(".", dbName, tableName);
  }

  public long getTimeElapsedForExpiry() {
    return timeElapsedForExpiry;
  }

  public static ExpireSnapshots getIcebergExpireSnapshots(
      Table table, long olderThanInMillis, int retainLast) {
    olderThanInMillis =
        getOlderThanInMillisWithTableProperties(table.properties(), olderThanInMillis);
    retainLast = getRetainLastWithTableProperties(table.properties(), retainLast);
    return table
        .expireSnapshots()
        .expireOlderThan(olderThanInMillis)
        .retainLast(retainLast)
        .cleanExpiredFiles(false);
  }

  private static long getOlderThanInMillisWithTableProperties(
      Map<String, String> tableProperties, long olderThanInMillis) {
    long tablePropOlderThanInMillis =
        System.currentTimeMillis()
            - PropertyUtil.propertyAsLong(tableProperties, MAX_SNAPSHOT_AGE_MS, 0);
    return Math.min(olderThanInMillis, tablePropOlderThanInMillis);
  }

  private static int getRetainLastWithTableProperties(
      Map<String, String> tableProperties, int retainLast) {
    return Math.max(
        retainLast, PropertyUtil.propertyAsInt(tableProperties, MIN_SNAPSHOTS_TO_KEEP, 1));
  }

  protected IcebergTableProps getTableProps() {
    Preconditions.checkNotNull(tableMetadata, "TableMetadata has not been initialized");
    return IcebergTableProps.createInstance(
        IcebergCommandType.VACUUM, tableName, dbName, tableMetadata, versionContext);
  }

  private IcebergTableIdentifier getTableIdentifier(IcebergModel icebergModel) {
    String location = Path.getContainerSpecificRelativePath(Path.of(tableMetadata.location()));
    if (icebergMutablePlugin instanceof IcebergPathSanitizer) {
      location = ((IcebergPathSanitizer) icebergMutablePlugin).sanitizePath(location);
    }
    return icebergModel.getTableIdentifier(location);
  }

  protected String getIcebergPath(String path) {
    return IcebergUtils.getIcebergPathAndValidateScheme(path, conf, fsScheme, schemeVariate);
  }

  private SnapshotEntry buildSnapshotEntry(String metadataFileLocation, Snapshot s) {
    return new SnapshotEntry(
        getIcebergPath(metadataFileLocation),
        s.snapshotId(),
        getIcebergPath(s.manifestListLocation()),
        s.timestampMillis());
  }
}
