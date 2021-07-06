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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.util.Retryer;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.file.proto.FileType;

/**
 * Provided with a metaDir(_delta_log path), version and tryCheckpointRead flag will:
 *
 * a) If (tryCheckpointRead)
 *    i) Check weather the version.checkpoint.parquet exits and was written before startTime.
 *    If both conditions are satisfied then initializes a {@link DeltaLogReader} to read the checkpoint file and returns a DeltaLogSnapshot.
 *
 * b) Checks weather version.json exists and was written before startTime.
 *    If both conditions are satisfied then initializes a {@link DeltaLogReader} to read the commit file and returns a DeltaLogSnapshot.
 *
 * Each job is intended to run in a separate thread. Submitted to a pool in {@link DeltaMetadataFetchJobManager}.
 */

public class DeltaMetadataFetchJob implements Supplier {
  private static final Logger logger = LoggerFactory.getLogger(DeltaLakeSchemaConverter.class);

  public Path metaDir;
  private final Path rootFolder;
  public FileSystem fs;
  public SabotContext context;
  public long startTime;
  public boolean tryCheckpointRead;
  public long version;

  private static Retryer retryer = new Retryer.Builder()
    .retryIfExceptionOfType(RuntimeException.class)
    .setWaitStrategy(Retryer.WaitStrategy.EXPONENTIAL, 250, 1500)
    .setMaxRetries(5).retryIfExceptionOfType(IOException.class).build();

  private DeltaFilePathResolver resolver = new DeltaFilePathResolver();


  public DeltaMetadataFetchJob(SabotContext context, Path metaDir, FileSystem fs, long startTime, boolean tryCheckpointRead, long version) {
    this.metaDir = metaDir;
    this.rootFolder = metaDir.getParent();
    this.fs = fs;
    this.context = context;
    this.startTime = startTime;
    this.tryCheckpointRead = tryCheckpointRead;
    this.version = version;
  }

  @Override
  public DeltaLogSnapshot get() {
    try {
      Path checkPointParquetPath = resolver.resolve(metaDir, version, FileType.PARQUET);

      if(tryCheckpointRead ) {
        final Optional<FileAttributes> fileAttrs = getFileAttrs(checkPointParquetPath);
        if (checkFileExistsAndValid(fileAttrs)) {
          logger.debug("Reading checkpoint file for Delta table at {}. Checkpoint version {}.", metaDir.toString(), version);
          return readAndSetVersion(fileAttrs.get(), FileType.PARQUET);
        }
      }

      Path commitJsonPath = resolver.resolve(metaDir, version, FileType.JSON);
      final Optional<FileAttributes> fileAttrs = getFileAttrs(commitJsonPath);
      if(checkFileExistsAndValid(fileAttrs)) {
        logger.debug("Reading commit file for Delta table at {}. Commit version {}.", metaDir.toString(), version);
        return readAndSetVersion(fileAttrs.get(), FileType.JSON);
      }

      throw new CompletionException(new
        InvalidFileException("File for version " + version
        + " either doesn't exist or is not suitable for reading"));

    } catch (IOException e) {
      logger.error("Unknown error occurred in DeltaMetadataFetchJob. Error {}, Job Parameters {}. Stacktrace {}", e.getMessage(), this.toString(), e.getStackTrace());
      throw new CompletionException(e);
    }
  }

  public DeltaLogSnapshot readAndSetVersion(FileAttributes fileAttrs, FileType type) throws IOException {
    DeltaLogReader reader = DeltaLogReader.getInstance(type);
    DeltaLogSnapshot snapshot =  reader.parseMetadata(rootFolder, context, fs, fileAttrs, version);
    snapshot.setVersionId(version);
    snapshot.setFileAttrs(fileAttrs);
    return snapshot;
  }

  @Override
  public String toString() {
    return "DeltaMetadataFetchJob{" +
      "metaDir=" + metaDir +
      ", fs=" + fs +
      ", startTime=" + startTime +
      ", tryCheckpointRead=" + tryCheckpointRead +
      ", version=" + version +
      '}';
  }

  private boolean checkFileExistsAndValid(Optional<FileAttributes> attrs) {
    if (!attrs.isPresent()) {
      return false;
    } else if (attrs.get().lastModifiedTime().toMillis() > startTime) {
      logger.warn("File {} is modified after startTime {}. Hence is not eligible for reading. Job Parameters {}",
              attrs.get().getPath().toString(), startTime, this.toString());
      return false;
    } else {
      return true;
    }
  }

  private Optional<FileAttributes> getFileAttrs(Path p) throws IOException {
    FileAttributes attr;

    Callable retryBlock = () -> {
      try {
        return fs.getFileAttributes(p);
      } catch (FileNotFoundException f) {
        //Avoid retrying for fileNotFound. Will always happen with the last job.
        logger.debug("{} file not found in directory {}. Job Parameters {}", p.getName(), metaDir.toString(), this.toString());
        return null;
      }
    };

    try {
      attr = (FileAttributes) retryer.call(retryBlock);
    }
    catch (Retryer.OperationFailedAfterRetriesException e) {
      throw e.getWrappedCause(IOException.class, ex -> new IOException(ex));
    }

    return Optional.ofNullable(attr);
  }

  static class InvalidFileException extends Exception {
    public InvalidFileException(String message) {
      super(message);
    }
  }
}
