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

import com.dremio.common.util.Retryer;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.io.file.PathFilters;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.Lists;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provided with a metadataDir (_delta_log path), version and tryCheckpointRead flag will:
 *
 * <p>a) If (tryCheckpointRead) i) Check weather the version.checkpoint.parquet exits and was
 * written before startTime. If both conditions are satisfied then initializes a {@link
 * DeltaLogReader} to read the checkpoint file and returns a DeltaLogSnapshot.
 *
 * <p>b) Checks weather version.json exists and was written before startTime. If both conditions are
 * satisfied then initializes a {@link DeltaLogReader} to read the commit file and returns a
 * DeltaLogSnapshot.
 *
 * <p>Each job is intended to run in a separate thread. Submitted to a pool in {@link
 * DeltaMetadataFetchJobManager}.
 */
public class DeltaMetadataFetchJob implements Supplier<DeltaLogSnapshot> {
  private static final Logger logger = LoggerFactory.getLogger(DeltaMetadataFetchJob.class);

  private final Path metadataDir;
  private final Path rootFolder;
  private final FileSystem fs;
  private final SabotContext context;
  private final DeltaVersion version;

  private static Retryer retryer =
      Retryer.newBuilder()
          .retryIfExceptionOfType(RuntimeException.class)
          .setWaitStrategy(Retryer.WaitStrategy.EXPONENTIAL, 250, 1500)
          .setMaxRetries(5)
          .retryIfExceptionOfType(IOException.class)
          .build();

  public DeltaMetadataFetchJob(
      SabotContext context, Path metadataDir, FileSystem fs, DeltaVersion version) {
    this.metadataDir = metadataDir;
    this.rootFolder = metadataDir.getParent();
    this.fs = fs;
    this.context = context;
    this.version = version;
  }

  public boolean isTryCheckpointRead() {
    return version.isCheckpoint();
  }

  @Override
  public DeltaLogSnapshot get() {
    try {
      if (isTryCheckpointRead()) {
        DeltaLogSnapshot checkpoint = tryGetCheckpoint();
        if (checkpoint != null) {
          return checkpoint;
        }
      }

      DeltaLogSnapshot commit = tryGetCommit();
      if (commit != null) {
        return commit;
      }

      throw new CompletionException(
          new InvalidFileException(
              "File for version "
                  + version
                  + " either doesn't exist or is not suitable for reading"));
    } catch (IOException e) {
      logger.error(
          "Unknown error occurred in DeltaMetadataFetchJob. Error {}, Job Parameters {}. Stacktrace {}",
          e.getMessage(),
          this.toString(),
          e.getStackTrace());
      throw new CompletionException(e);
    }
  }

  private DeltaLogSnapshot tryGetCommit() throws IOException {
    List<Path> pathList =
        DeltaFilePathResolver.resolve(metadataDir, version.getVersion(), 1, FileType.JSON);
    List<FileAttributes> fileAttrList = convertPathList(pathList);
    if (fileAttrList.isEmpty()) {
      return null;
    }

    return readAndSetVersion(fileAttrList, FileType.JSON);
  }

  private DeltaLogSnapshot tryGetCheckpoint() throws IOException {
    List<Path> pathList =
        DeltaFilePathResolver.resolve(
            metadataDir, version.getVersion(), version.getSubparts(), FileType.PARQUET);
    List<FileAttributes> fileAttrList = convertPathList(pathList);
    if (!checkCheckpointFiles(fileAttrList)) {
      Path pattern =
          DeltaFilePathResolver.resolveCheckpointPattern(metadataDir, version.getVersion());
      fileAttrList = listPathPattern(pattern);
      if (!checkCheckpointFiles(fileAttrList)) {
        return null;
      }
    }

    return readAndSetVersion(fileAttrList, FileType.PARQUET);
  }

  private boolean checkCheckpointFiles(List<FileAttributes> fileAttrs) {
    if (fileAttrs.isEmpty()) {
      return false;
    }
    int partCount =
        DeltaFilePathResolver.getPartCountFromPath(fileAttrs.get(0).getPath(), FileType.PARQUET);
    if (partCount > 1 && partCount != fileAttrs.size()) {
      logger.warn(
          "Multipart checkpoint {} is missing parts, expected {}, got {} files.",
          fileAttrs.get(0).getPath(),
          partCount,
          fileAttrs.size());
    }
    return partCount == fileAttrs.size();
  }

  private DeltaLogSnapshot readAndSetVersion(List<FileAttributes> fileAttrsList, FileType type)
      throws IOException {
    DeltaLogReader reader = DeltaLogReader.getInstance(type);
    DeltaLogSnapshot snapshot =
        reader.parseMetadata(rootFolder, context, fs, fileAttrsList, version.getVersion());
    snapshot.setVersionId(version.getVersion());
    return snapshot;
  }

  @Override
  public String toString() {
    return "DeltaMetadataFetchJob{"
        + "metadataDir="
        + metadataDir
        + ", fs="
        + fs
        + ", tryCheckpointRead="
        + isTryCheckpointRead()
        + ", version="
        + version.getVersion()
        + ", subparts="
        + version.getSubparts()
        + '}';
  }

  private List<FileAttributes> convertPathList(List<Path> pathList) throws IOException {
    List<FileAttributes> resolvedFileAttributes = Lists.newArrayList();
    for (Path path : pathList) {
      try {
        resolvedFileAttributes.add(fs.getFileAttributes(path));
      } catch (FileNotFoundException f) {
        // Avoid retrying for fileNotFound. Will always happen with the last job.
        logger.debug(
            "{} file not found in directory {}. Job Parameters {}",
            path,
            metadataDir.toString(),
            this.toString());
        return Lists.newArrayList();
      }
    }
    return resolvedFileAttributes;
  }

  private List<FileAttributes> listPathPattern(Path pattern) throws IOException {
    Callable<List<FileAttributes>> retryBlock =
        () -> {
          try {
            return Lists.newArrayList(fs.glob(pattern, PathFilters.ALL_FILES));
          } catch (FileNotFoundException f) {
            // Avoid retrying for fileNotFound. Will always happen with the last job.
            logger.debug(
                "{} file not found in directory {}. Job Parameters {}",
                pattern.getName(),
                metadataDir.toString(),
                this.toString());
            return Lists.newArrayList();
          }
        };

    try {
      return retryer.call(retryBlock);
    } catch (Retryer.OperationFailedAfterRetriesException e) {
      throw e.getWrappedCause(IOException.class, ex -> new IOException(ex));
    }
  }

  static class InvalidFileException extends Exception {
    public InvalidFileException(String message) {
      super(message);
    }
  }
}
