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
package com.dremio.plugins.dataplane.exec;

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergExpiryAction;
import com.dremio.exec.store.iceberg.SnapshotEntry;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.PropertyUtil;

/** Nessie specific variants in the expiry actions. */
public class NessieIcebergExpiryAction extends IcebergExpiryAction {

  private static final List<String> TABLE_OVERRIDE_PROPERTY_KEYS =
      ImmutableList.of(GC_ENABLED, MAX_SNAPSHOT_AGE_MS, MIN_SNAPSHOTS_TO_KEEP);

  public NessieIcebergExpiryAction(
      SupportsIcebergMutablePlugin icebergMutablePlugin,
      OpProps props,
      OperatorContext context,
      VacuumOptions vacuumOptions,
      TableMetadata tableMetadata,
      String tableName,
      String dbName,
      ResolvedVersionContext versionContext,
      FileIO fileIO,
      boolean commitExpiry,
      String schemeVariate,
      String fsScheme) {
    super(
        icebergMutablePlugin,
        props,
        context,
        vacuumOptions,
        tableMetadata,
        tableName,
        dbName,
        versionContext,
        fileIO,
        commitExpiry,
        schemeVariate,
        fsScheme);
  }

  @Override
  protected List<String> computeMetadataPathsToRetain(Set<Long> liveSnapshotIds) {
    // Identify older metadata paths to allow commit level time-travel for the given history in
    // retention override cases.
    // Traversing commit history for a given table isn't an efficient operation in Nessie. Hence,
    // using the Iceberg metadata.
    // Identification of these paths will be based on Nessie commit metadata instead of Iceberg
    // metadata in the future.
    long oldestSnapshotTimestamp =
        getAllSnapshots().stream()
            .filter(s -> liveSnapshotIds.contains(s.snapshotId()))
            .mapToLong(Snapshot::timestampMillis)
            .min()
            .orElse(System.currentTimeMillis());
    Predicate<TableMetadata.MetadataLogEntry> shouldRetain =
        PropertyUtil.propertyAsBoolean(tableMetadata.properties(), GC_ENABLED, true)
            ? me -> me.timestampMillis() >= oldestSnapshotTimestamp
            : Predicates.alwaysTrue();

    return Stream.concat(
            Stream.of(tableMetadata.metadataFileLocation()),
            tableMetadata.previousFiles().stream()
                .filter(shouldRetain)
                .map(TableMetadata.MetadataLogEntry::file))
        .map(super::getIcebergPath)
        .collect(Collectors.toList());
  }

  @Override
  public List<SnapshotEntry> getRetainedSnapshots() {
    // Only retain table level overrides since regular live snapshots are already handled by
    // NessieCommitsRecordReader
    // Identify snapshots beyond cut-off policy but included in the liveSnapshots list.
    if (!TABLE_OVERRIDE_PROPERTY_KEYS.stream().anyMatch(tableMetadata.properties()::containsKey)) {
      return Collections.emptyList();
    }

    Set<Long> allRetainedSnapshotIds =
        liveSnapshots.stream().map(SnapshotEntry::getSnapshotId).collect(Collectors.toSet());
    return getAllSnapshots().stream()
        .sorted(Comparator.comparing(Snapshot::timestampMillis).reversed())
        .skip(1) // Skip the latest snapshot as it'll always be retained
        .filter(s -> s.timestampMillis() <= vacuumOptions.getOlderThanInMillis())
        .filter(s -> allRetainedSnapshotIds.contains(s.snapshotId()))
        .map(s -> new SnapshotEntry(tableMetadata.metadataFileLocation(), s))
        .collect(Collectors.toList());
  }

  @Override
  protected IcebergTableProps getTableProps() {
    IcebergTableProps tableProps =
        IcebergTableProps.createInstance(
            IcebergCommandType.VACUUM, tableName, dbName, tableMetadata, versionContext);
    // Set the table name correctly.It should concatenate foldersName and
    // TableName.like(folder1.folder2.table)
    // here dbName is a list of folders.
    if (!StringUtils.isEmpty(dbName)) {
      tableProps.setTableName(dbName + "." + tableName);
    }
    return tableProps;
  }
}
