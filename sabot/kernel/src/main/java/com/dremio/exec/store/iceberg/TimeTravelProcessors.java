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

import java.util.List;
import java.util.function.Predicate;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.options.TimeTravelOption.SnapshotIdRequest;
import com.dremio.connector.metadata.options.TimeTravelOption.TimeTravelRequest;
import com.dremio.connector.metadata.options.TimeTravelOption.TimestampRequest;
import com.google.common.base.Preconditions;

/**
 * Utilities related to time travel processors.
 */
public final class TimeTravelProcessors {

  private static List<HistoryEntry> getLogEntries(Table table) {
    Preconditions.checkArgument(table instanceof HasTableOperations);
    return ((HasTableOperations) table).operations().current().snapshotLog();
  }

  private static TableSnapshotProvider getTimestampProcessor(
      List<String> tablePath,
      TimestampRequest timestampRequest
  ) {
    final long millis = timestampRequest.getTimestampMillis();

    final Predicate<HistoryEntry> entryPredicate;
    switch (timestampRequest.getTimeTravelSpecifier()) {
    case AT:
      entryPredicate = historyEntry -> historyEntry.timestampMillis() <= millis;
      break;

    case BEFORE:
      entryPredicate = historyEntry -> historyEntry.timestampMillis() < millis;
      break;

    default:
      throw new IllegalArgumentException("Unsupported specifier: " + timestampRequest.getTimeTravelSpecifier());
    }

    return table -> {
      Long lastSnapshotId = null;
      for (HistoryEntry logEntry : getLogEntries(table)) {
        if (entryPredicate.test(logEntry)) {
          lastSnapshotId = logEntry.snapshotId();
        }
      }

      final Snapshot snapshot = lastSnapshotId != null ? table.snapshot(lastSnapshotId) : null;
      if (snapshot == null) {
        throw UserException.validationError()
            .message("For table '%s', the provided time travel timestamp value '%d' is out of range",
                tablePath, millis)
            .buildSilently();
      }

      return snapshot;
    };
  }

  private static TableSnapshotProvider getSnapshotIdProcessor(
      List<String> tablePath,
      SnapshotIdRequest snapshotIdRequest
  ) {
    final long snapshotId = Long.parseLong(snapshotIdRequest.getSnapshotId());

    switch (snapshotIdRequest.getTimeTravelSpecifier()) {
    case AT:
      return table -> getTableSnapshotAtId(tablePath, table, snapshotId);

    case BEFORE:
      return table -> {
        final Snapshot snapshot = getTableSnapshotAtId(tablePath, table, snapshotId);
        if (snapshot.parentId() == null) {
          throw UserException.validationError()
              .message("For table '%s', the provided snapshot ID '%d' does not have a parent snapshot",
                  tablePath, snapshotId)
              .buildSilently();
        }
        return getTableSnapshotAtId(tablePath, table, snapshot.parentId());
      };

    default:
      throw new IllegalArgumentException("Unsupported specifier: " + snapshotIdRequest.getTimeTravelSpecifier());
    }
  }

  private static Snapshot getTableSnapshotAtId(
      List<String> tablePath,
      Table table,
      long snapshotId
  ) {
    final Snapshot snapshot = table.snapshot(snapshotId);
    if (snapshot == null) {
      throw UserException.validationError()
          .message("For table '%s', the provided snapshot ID '%d' is invalid", tablePath, snapshotId)
          .buildSilently();
    }
    return snapshot;
  }

  /**
   * Provide an appropriate {@link TableSnapshotProvider} given a {@link TimeTravelRequest}. If time travel request is
   * null, return current snapshot provider.
   *
   * @param tablePath         table path
   * @param timeTravelRequest time travel request
   * @return table snapshot provider
   */
  public static TableSnapshotProvider getTableSnapshotProvider(
      List<String> tablePath,
      TimeTravelRequest timeTravelRequest
  ) {
    if (timeTravelRequest instanceof TimestampRequest) {
      return getTimestampProcessor(tablePath, (TimestampRequest) timeTravelRequest);
    } else if (timeTravelRequest instanceof SnapshotIdRequest) {
      return getSnapshotIdProcessor(tablePath, (SnapshotIdRequest) timeTravelRequest);
    }

    return Table::currentSnapshot;
  }

  private TimeTravelProcessors() {
  }
}
