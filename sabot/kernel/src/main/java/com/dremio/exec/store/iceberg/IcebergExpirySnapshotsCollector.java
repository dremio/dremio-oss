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

import static org.apache.iceberg.TableProperties.MAX_REF_AGE_MS;
import static org.apache.iceberg.TableProperties.MAX_REF_AGE_MS_DEFAULT;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Collect the snapshots for expiry from Iceberg TableMetadata.
 */
public class IcebergExpirySnapshotsCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergExpirySnapshotsCollector.class);
  private final TableMetadata tableMetadata;

  private List<SnapshotEntry> expiredSnapshots = Lists.newArrayList();
  private final Set<Long> idsToRetain = Sets.newHashSet();

  public IcebergExpirySnapshotsCollector (TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public Pair<List<SnapshotEntry>, Set<Long>> collect(long olderThanInMillis, int retainLast) {
    // Identify refs that should be retained
    Map<String, SnapshotRef> retainedRefs = computeRetainedRefs(tableMetadata.refs());
    Map<Long, List<String>> retainedIdToRefs = Maps.newHashMap();
    for (Map.Entry<String, SnapshotRef> retainedRefEntry : retainedRefs.entrySet()) {
      long snapshotId = retainedRefEntry.getValue().snapshotId();
      retainedIdToRefs.putIfAbsent(snapshotId, Lists.newArrayList());
      retainedIdToRefs.get(snapshotId).add(retainedRefEntry.getKey());
      idsToRetain.add(snapshotId);
    }

    idsToRetain.addAll(computeAllBranchSnapshotsToRetain(retainedRefs.values(), olderThanInMillis, retainLast));
    idsToRetain.addAll(unreferencedSnapshotsToRetain(retainedRefs.values(), olderThanInMillis));

    expiredSnapshots =  tableMetadata.snapshots().stream()
      .filter(s -> !idsToRetain.contains(s.snapshotId()))
      .map(s -> new SnapshotEntry(tableMetadata.metadataFileLocation(), s))
      .collect(Collectors.toList());

    return Pair.of(expiredSnapshots, idsToRetain);
  }

  private Map<String, SnapshotRef> computeRetainedRefs(Map<String, SnapshotRef> refs) {
    Map<String, SnapshotRef> retainedRefs = Maps.newHashMap();
    for (Map.Entry<String, SnapshotRef> refEntry : refs.entrySet()) {
      String name = refEntry.getKey();
      SnapshotRef ref = refEntry.getValue();
      if (name.equals(SnapshotRef.MAIN_BRANCH)) {
        retainedRefs.put(name, ref);
        continue;
      }

      Snapshot snapshot = tableMetadata.snapshot(ref.snapshotId());
      long defaultMaxRefAgeMs =
        PropertyUtil.propertyAsLong(tableMetadata.properties(), MAX_REF_AGE_MS, MAX_REF_AGE_MS_DEFAULT);

      long maxRefAgeMs = ref.maxRefAgeMs() != null ? ref.maxRefAgeMs() : defaultMaxRefAgeMs;
      if (snapshot != null) {
        long refAgeMs = System.currentTimeMillis() - snapshot.timestampMillis();
        if (refAgeMs <= maxRefAgeMs) {
          retainedRefs.put(name, ref);
        }
      } else {
        LOGGER.warn("Removing invalid ref {}: snapshot {} does not exist", name, ref.snapshotId());
      }
    }

    return retainedRefs;
  }

  private Set<Long> computeAllBranchSnapshotsToRetain(
    Collection<SnapshotRef> refs, long expireSnapshotsOlderThan, int minSnapshotsToKeep) {
    Set<Long> branchSnapshotsToRetain = Sets.newHashSet();
    for (SnapshotRef ref : refs) {
      if (ref.isBranch()) {
        branchSnapshotsToRetain.addAll(
          computeBranchSnapshotsToRetain(
            ref.snapshotId(), expireSnapshotsOlderThan, minSnapshotsToKeep));
      }
    }

    return branchSnapshotsToRetain;
  }

  private Set<Long> computeBranchSnapshotsToRetain(
    long snapshot, long expireSnapshotsOlderThan, int minSnapshotsToKeep) {
    Set<Long> idsToRetain = Sets.newHashSet();
    for (Snapshot ancestor : SnapshotUtil.ancestorsOf(snapshot, tableMetadata::snapshot)) {
      if (idsToRetain.size() < minSnapshotsToKeep
        || ancestor.timestampMillis() >= expireSnapshotsOlderThan) {
        idsToRetain.add(ancestor.snapshotId());
      } else {
        return idsToRetain;
      }
    }

    return idsToRetain;
  }

  private Set<Long> unreferencedSnapshotsToRetain(Collection<SnapshotRef> refs, long expireSnapshotsOlderThan) {
    Set<Long> referencedSnapshots = Sets.newHashSet();
    for (SnapshotRef ref : refs) {
      if (ref.isBranch()) {
        for (Snapshot snapshot : SnapshotUtil.ancestorsOf(ref.snapshotId(), tableMetadata::snapshot)) {
          referencedSnapshots.add(snapshot.snapshotId());
        }
      } else {
        referencedSnapshots.add(ref.snapshotId());
      }
    }

    Set<Long> snapshotsToRetain = Sets.newHashSet();
    for (Snapshot snapshot : tableMetadata.snapshots()) {
      if (!referencedSnapshots.contains(snapshot.snapshotId())
        && // unreferenced
        snapshot.timestampMillis() >= expireSnapshotsOlderThan) { // not old enough to expire
        snapshotsToRetain.add(snapshot.snapshotId());
      }
    }

    return snapshotsToRetain;
  }
}
