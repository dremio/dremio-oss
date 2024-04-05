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

import com.dremio.connector.metadata.DatasetMetadataVerifyResult;
import com.dremio.connector.metadata.DatasetVerifyAppendOnlyResult;
import com.dremio.connector.metadata.DatasetVerifyDataModifiedResult;
import com.dremio.connector.metadata.DatasetVerifyDataModifiedResult.ResultCode;
import com.dremio.connector.metadata.ImmutableDatasetVerifyAppendOnlyResult;
import com.dremio.connector.metadata.ImmutableDatasetVerifyDataModifiedResult;
import com.dremio.connector.metadata.options.MetadataVerifyRequest;
import com.dremio.connector.metadata.options.VerifyAppendOnlyRequest;
import com.dremio.connector.metadata.options.VerifyDataModifiedRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;

/** Processor that processes {@link MetadataVerifyRequest} on iceberg table. */
public final class IcebergMetadataVerifyProcessors {

  @Nonnull
  public static Optional<DatasetMetadataVerifyResult> verify(
      MetadataVerifyRequest metadataVerifyRequest, Table table) {
    if (metadataVerifyRequest instanceof VerifyAppendOnlyRequest) {
      VerifyAppendOnlyRequest request = (VerifyAppendOnlyRequest) metadataVerifyRequest;
      return Optional.of(
          isAppendOnlyBetweenSnapshots(
              table,
              Long.valueOf(request.getBeginSnapshotId()),
              Long.valueOf(request.getEndSnapshotId())));
    } else if (metadataVerifyRequest instanceof VerifyDataModifiedRequest) {
      VerifyDataModifiedRequest request = (VerifyDataModifiedRequest) metadataVerifyRequest;
      return Optional.of(
          isDatasetModifiedBetweenSnapshots(
              table,
              Long.valueOf(request.getBeginSnapshotId()),
              Long.valueOf(request.getEndSnapshotId())));
    }

    return Optional.empty();
  }

  /**
   * Verifies if changes on given iceberg table were append-only. Return result code and optional
   * snapshot ranges. - OVERWRITE operation with no delete data file changes is considered same as
   * APPEND. - If changes were APPEND operations only, result code: APPEND_ONLY,
   * range:[(beginSnapshotId, endSnapshotId)]) - If changes were APPEND and REPLACE operations,
   * result code: APPEND_ONLY, original range (beginSnapshotId, endSnapshotId) is split into
   * multiple sub ranges, e.g. Base table changes: S0 -> Append -> S1 -> Append -> S2 -> Compact ->
   * S3 -> Compact -> S4 -> Append -> S5 original range (S0, S5) result ranges: [(S4, S5), (S0, S2)]
   * - If beginSnapshotId == endSnapshotId it's still considered as append-only (result code:
   * APPEND_ONLY, range: [(beginSnapshotId, beginSnapshotId)]) - If changes were not append-only,
   * result code: INVALID_BEGIN_SNAPSHOT/INVALID_END_SNAPSHOT/NOT_ANCESTOR/NOT_APPEND_ONLY, range is
   * empty.
   *
   * @param table iceberg table
   * @param beginSnapshotId
   * @param endSnapshotId
   * @return MetadataVerifyResult.ResultEntry
   */
  public static DatasetMetadataVerifyResult isAppendOnlyBetweenSnapshots(
      Table table, Long beginSnapshotId, Long endSnapshotId) {
    ImmutableDatasetVerifyAppendOnlyResult.Builder resultBuilder =
        DatasetVerifyAppendOnlyResult.builder();
    List<Pair<String, String>> snapshotRanges = new ArrayList<>();

    if (table.snapshot(beginSnapshotId) == null) {
      resultBuilder.resultCode(DatasetVerifyAppendOnlyResult.ResultCode.INVALID_BEGIN_SNAPSHOT);
      return resultBuilder.build();
    }
    if (table.snapshot(endSnapshotId) == null) {
      resultBuilder.resultCode(DatasetVerifyAppendOnlyResult.ResultCode.INVALID_END_SNAPSHOT);
      return resultBuilder.build();
    }
    if (!SnapshotUtil.isAncestorOf(table, endSnapshotId, beginSnapshotId)) {
      resultBuilder.resultCode(DatasetVerifyAppendOnlyResult.ResultCode.NOT_ANCESTOR);
      return resultBuilder.build();
    }

    boolean isAppendOnly = true;
    long endRange = 0;
    try {
      // Note:
      // - Snapshots from SnapshotUtil.ancestorsBetween(endSnapshotId, beginSnapshotId,
      // table::snapshot) are iterated
      //   backwards in history
      // - Snapshot for beginSnapshotId is not in SnapshotUtil.ancestorsBetween(endSnapshotId,
      // beginSnapshotId, table::snapshot)
      // - I.e. [endSnapshot -> ... -> beginSnapshot)
      for (Snapshot snapshot :
          SnapshotUtil.ancestorsBetween(endSnapshotId, beginSnapshotId, table::snapshot)) {
        Map<String, String> summary =
            Optional.ofNullable(snapshot).map(Snapshot::summary).orElseGet(ImmutableMap::of);
        if ((snapshot.operation().equals(DataOperations.APPEND)
            || (snapshot.operation().equals(DataOperations.OVERWRITE)
                && ((Long.parseLong(summary.getOrDefault(SnapshotSummary.DELETED_FILES_PROP, "0"))
                        == 0)
                    && (Long.parseLong(
                            summary.getOrDefault(SnapshotSummary.ADDED_DELETE_FILES_PROP, "0"))
                        == 0)
                    && (Long.parseLong(
                            summary.getOrDefault(SnapshotSummary.REMOVED_DELETE_FILES_PROP, "0"))
                        == 0))))) {
          if (endRange == 0) {
            endRange = snapshot.snapshotId();
          }
          // skip adjacent APPEND operations
        } else if (snapshot.operation().equals(DataOperations.REPLACE)) {
          if (endRange != 0) {
            snapshotRanges.add(
                ImmutablePair.of(Long.toString(snapshot.snapshotId()), Long.toString(endRange)));
            endRange = 0; // reset
          }
          // skip adjacent REPLACE operations
        } else {
          isAppendOnly = false;
          break;
        }
      }
    } catch (NoSuchElementException e) {
      isAppendOnly = false;
    }

    if (!isAppendOnly) {
      resultBuilder.resultCode(DatasetVerifyAppendOnlyResult.ResultCode.NOT_APPEND_ONLY);
      return resultBuilder.build();
    }

    // beginSnapshotId is not in SnapshotUtil.ancestorsBetween(endSnapshotId, beginSnapshotId,
    // table::snapshot)
    if (endRange != 0) {
      snapshotRanges.add(ImmutablePair.of(beginSnapshotId.toString(), Long.toString(endRange)));
    }

    // If beginSnapshotId == endSnapshotId or all operations between are REPLACE operations
    if (snapshotRanges.size() == 0) {
      snapshotRanges.add(ImmutablePair.of(beginSnapshotId.toString(), beginSnapshotId.toString()));
    }

    resultBuilder.snapshotRanges(ImmutableList.copyOf(snapshotRanges));
    resultBuilder.resultCode(DatasetVerifyAppendOnlyResult.ResultCode.APPEND_ONLY);
    return resultBuilder.build();
  }

  /**
   * Verifies if changes on given iceberg table were data modifying. Returns result code and
   * optional snapshot ranges. DATA_MODIFIED - snapshot range has at least one non-REPLACE type
   * DataOperation. NOT_DATA_MODIFIED - snapshot range ONLY has REPLACE type operations.
   * NOT_ANCESTOR - snapshots are not ancestors. INVALID_BEGIN_SNAPSHOT - invalid beginning snapshot
   * for base table INVALID_END_SNAPSHOT - invalid end snapshot for base table
   *
   * @param table iceberg table
   * @param beginSnapshotId
   * @param endSnapshotId
   * @return MetadataVerifyResult.ResultEntry
   */
  public static DatasetMetadataVerifyResult isDatasetModifiedBetweenSnapshots(
      Table table, Long beginSnapshotId, Long endSnapshotId) {
    ImmutableDatasetVerifyDataModifiedResult.Builder resultBuilder =
        DatasetVerifyDataModifiedResult.builder();

    if (table.snapshot(beginSnapshotId) == null) {
      resultBuilder.resultCode(DatasetVerifyDataModifiedResult.ResultCode.INVALID_BEGIN_SNAPSHOT);
      return resultBuilder.build();
    }
    if (table.snapshot(endSnapshotId) == null) {
      resultBuilder.resultCode(DatasetVerifyDataModifiedResult.ResultCode.INVALID_END_SNAPSHOT);
      return resultBuilder.build();
    }
    if (!SnapshotUtil.isAncestorOf(table, endSnapshotId, beginSnapshotId)) {
      resultBuilder.resultCode(DatasetVerifyDataModifiedResult.ResultCode.NOT_ANCESTOR);
      return resultBuilder.build();
    }

    boolean isDataModifying = false;

    for (Snapshot snapshot :
        SnapshotUtil.ancestorsBetween(endSnapshotId, beginSnapshotId, table::snapshot)) {
      if (!snapshot.operation().equals(DataOperations.REPLACE)) {
        isDataModifying = true;
        break;
      }
    }
    if (!isDataModifying) {
      resultBuilder.resultCode(ResultCode.NOT_DATA_MODIFIED);
    } else {
      resultBuilder.resultCode(ResultCode.DATA_MODIFIED);
    }
    return resultBuilder.build();
  }
}
