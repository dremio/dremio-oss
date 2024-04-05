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

package com.dremio.exec.store.deltalake;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.io.file.PathFilters;
import com.dremio.service.namespace.file.proto.FileType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expects path of _delta_log as metadataDir. Resolves: null - version of the latest checkpoint or 0
 * SnapshotId - version of the provided id (does existance check) Timestamp - version before or at
 * the timestamp. If the given timestamp is outside the range of [oldestCommit, latestCommit] then
 * `resolveOldest` and `resolveLatest` flags control whether an exception is thrown. By default,
 * resolveOldest is false and resolveLatest is true. Throws UserException if provided SnapshotId is
 * not a number, or out of range of valid snapsots provided Timestamp is older than the oldest valid
 * version
 */
public final class DeltaVersionResolver {

  private static final Logger logger = LoggerFactory.getLogger(DeltaVersionResolver.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final FileSystem fs;
  private final Path metadataDir;
  private boolean resolveOldest = false;
  private boolean resolveLatest = true;

  public DeltaVersionResolver(FileSystem fs, Path metadataDir) {
    this.fs = fs;
    this.metadataDir = metadataDir;
  }

  public boolean canResolveOldest() {
    return this.resolveOldest;
  }

  public void setResolveOldest(boolean resolveOldest) {
    this.resolveOldest = resolveOldest;
  }

  public boolean canResolveLatest() {
    return resolveLatest;
  }

  public void setResolveLatest(boolean resolveLatest) {
    this.resolveLatest = resolveLatest;
  }

  public DeltaVersion resolve(TimeTravelOption.TimeTravelRequest travelRequest) {
    if (travelRequest == null) {
      return getLastCheckpoint();
    }

    if (travelRequest instanceof TimeTravelOption.SnapshotIdRequest) {
      return DeltaVersion.of(
          resolveVersionAtSnapshotId((TimeTravelOption.SnapshotIdRequest) travelRequest));
    } else if (travelRequest instanceof TimeTravelOption.TimestampRequest) {
      return DeltaVersion.of(
          resolveVersionAtTimestamp((TimeTravelOption.TimestampRequest) travelRequest));
    } else {
      throw new IllegalStateException("Unexpected value for TimeTravelRequest ");
    }
  }

  public DeltaVersion getLastCheckpoint() {
    Path lastCheckpointPath = metadataDir.resolve(Path.of(DeltaConstants.DELTA_LAST_CHECKPOINT));
    try (final FSInputStream lastCheckPointFs = fs.open(lastCheckpointPath);
        final BufferedReader bufferedReader =
            new BufferedReader(new InputStreamReader(lastCheckPointFs))) {
      final JsonNode lastCheckpoint = OBJECT_MAPPER.readTree(bufferedReader.readLine());
      final long version = lastCheckpoint.get("version").asLong(0L);
      final int subparts = lastCheckpoint.has("parts") ? lastCheckpoint.get("parts").asInt(1) : 1;
      return DeltaVersion.ofCheckpoint(version, subparts);
    } catch (FileNotFoundException e) {
      logger.debug("_last_checkpoint file not found in {}. Job Parameters {}", metadataDir, this);
    } catch (Exception e) {
      throw UserException.dataReadError(e)
          .message(
              String.format(
                  "Failed to read _last_checkpoint file %s. Error %s",
                  lastCheckpointPath, e.getMessage()))
          .build(logger);
    }

    return DeltaVersion.ofCheckpoint(0L, 1);
  }

  private long resolveVersionAtSnapshotId(TimeTravelOption.SnapshotIdRequest snapshotIdRequest) {
    try {
      long version = Long.parseLong(snapshotIdRequest.getSnapshotId());
      checkVersion(version);
      return version;
    } catch (Exception e) {
      throw UserException.validationError(e)
          .message(
              "The provided snapshot ID '%s' is invalid for delta dataset %s.",
              snapshotIdRequest.getSnapshotId(), metadataDir.getParent())
          .build(logger);
    }
  }

  private void checkVersion(long version) throws IOException {
    List<Path> versionLogPath =
        DeltaFilePathResolver.resolve(metadataDir, version, 1, FileType.JSON);
    if (!fs.exists(versionLogPath.get(0))) {
      throw new FileNotFoundException(
          String.format("Log file %s not found.", versionLogPath.get(0)));
    }
  }

  private long resolveVersionAtTimestamp(TimeTravelOption.TimestampRequest timestampRequest) {
    long timestampMillis = timestampRequest.getTimestampMillis();

    try {
      List<FileAttributes> commitList = listCommitFiles();
      if (commitList.isEmpty()) {
        throw UserException.invalidMetadataError()
            .message("Metadata log is empty for delta dataset %s.", metadataDir.getParent())
            .build(logger);
      }

      long oldestTimestamp = DeltaLogCommitJsonReader.parseTimestamp(fs, commitList.get(0));
      if (timestampMillis <= oldestTimestamp) {
        if (canResolveOldest() || timestampMillis == oldestTimestamp) {
          return DeltaFilePathResolver.getVersionFromPath(commitList.get(0).getPath());
        }
        throw UserException.validationError()
            .message(
                "The provided Time Travel timestamp value '%s' is out of range for delta dataset %s.",
                String.valueOf(timestampMillis), metadataDir.getParent())
            .build(logger);
      }

      long latestTimestamp =
          DeltaLogCommitJsonReader.parseTimestamp(fs, commitList.get(commitList.size() - 1));
      if (timestampMillis >= latestTimestamp) {
        if (canResolveLatest() || timestampMillis == latestTimestamp) {
          return DeltaFilePathResolver.getVersionFromPath(
              commitList.get(commitList.size() - 1).getPath());
        }
        throw UserException.validationError()
            .message(
                "The provided Time Travel timestamp value '%s' is out of range for delta dataset %s.",
                String.valueOf(timestampMillis), metadataDir.getParent())
            .build(logger);
      }

      FileAttributes timestampCommit = findCommitBeforeOrAtTimestamp(commitList, timestampMillis);
      return DeltaFilePathResolver.getVersionFromPath(timestampCommit.getPath());
    } catch (Exception e) {
      throw UserException.invalidMetadataError(e)
          .message(
              "Failed to read version for Time Travel timestamp value '%s' for delta dataset %s.",
              String.valueOf(timestampMillis), metadataDir.getParent())
          .build(logger);
    }
  }

  private List<FileAttributes> listCommitFiles() throws IOException {
    try (DirectoryStream<FileAttributes> stream =
        fs.glob(metadataDir.resolve("*.json"), PathFilters.ALL_FILES)) {
      return StreamSupport.stream(stream.spliterator(), false)
          .sorted(Comparator.comparing(FileAttributes::getPath))
          .collect(Collectors.toList());
    }
  }

  private FileAttributes findCommitBeforeOrAtTimestamp(
      List<FileAttributes> commitList, long targetTimestamp) throws IOException {
    // preconditions (checked on caller side):
    // commitList - non-empty sorted list
    // oldestTimestamp < targetTimestamp < latestTimestamp

    int low = 0;
    int high = commitList.size() - 1;
    FileAttributes lastCommitBeforeTimestamp = commitList.get(0);

    while (low <= high) {
      int mid = (low + high) / 2;
      FileAttributes currentCommit = commitList.get(mid);
      long currentTimestamp = DeltaLogCommitJsonReader.parseTimestamp(fs, currentCommit);
      if (currentTimestamp == targetTimestamp) {
        return currentCommit;
      } else if (currentTimestamp < targetTimestamp) {
        lastCommitBeforeTimestamp = currentCommit;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return lastCommitBeforeTimestamp;
  }
}
