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
package com.dremio.exec.store.metadatarefresh.dirlisting;

import static com.dremio.exec.store.iceberg.model.IcebergConstants.FILE_VERSION;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Retryer;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.DirList;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.io.file.PathFilters;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.commons.lang3.StringUtils;

/**
 * RecordReader which given a path for a root dir will produce a list of files in the directory with
 * their size, mtime and their partitionInfo.
 *
 * <p>Input Parameters
 *
 * <p>isRecursive - controls whether the listing is recursive or only top level
 *
 * <p>lastReadSignatureMtime - ignore the files which are modified after the provided
 * lastReadSignatureMtime
 *
 * <p>partitionValues -
 *
 * <p>1) partitionValues == null (for filesystem datasets) Reader figure out partition values for
 * each file. For filesystem datasets partition values depend on the level of nesting of the file
 * relative to provided the rootDir.
 *
 * <p>2) If partitionValues is provided(for hive datasets) all files have the same partitionInfo.
 *
 * <p>Output Vector -
 *
 * <p>pathVector - VarCharVector of file paths sizeVector - BigIntVector of file sizes mtimeVector -
 * BigIntVector of file modification times
 *
 * <p>partitionInfoList - ListVector<Struct<key, value>>. For filesystem datasets keys are dir0,
 * dir1 ... depending on the level of nesting of the file and the values are directory names.
 */
public class DirListingRecordReader implements RecordReader {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DirListingRecordReader.class);

  /** The initial batch size for the case of incremental batch size */
  private static final int INITIAL_BATCH_SIZE = 32;

  private final FileSystem fs;
  protected final long startTime;
  protected final Path rootPath;
  protected final Path operatingPath;
  private final boolean isRecursive;
  private final boolean discoverPartitions;
  private OutputMutator outgoing;
  protected Iterator<FileAttributes> dirIterator;
  private IcebergPartitionData currPartitionInfo;
  private int batchesProcessed = 0;
  private final int maxBatchSize;
  private final int footerReaderWidth;
  private int batchSize;
  private boolean incrementalBatchSize;
  private boolean isFile;
  private boolean hasVersion;
  private boolean hasFiles;
  private List<String> files;
  private String globPattern;
  private final boolean excludeFutureModTimes;

  // Output Vectors
  private BigIntVector mtimeVector;
  private BigIntVector sizeVector;
  private VarCharVector pathVector;
  private VarBinaryVector partitionInfoVector;
  private final Retryer retryer =
      Retryer.newBuilder()
          .retryIfExceptionOfType(IOException.class)
          .retryIfExceptionOfType(RuntimeException.class)
          .setWaitStrategy(Retryer.WaitStrategy.EXPONENTIAL, 250, 2500)
          .setMaxRetries(10)
          .build();

  private PartitionParser partitionParser;

  public DirListingRecordReader(
      OperatorContext context,
      FileSystem fs,
      DirListInputSplitProto.DirListInputSplit dirListInputSplit,
      boolean isRecursive,
      BatchSchema tableSchema,
      List<PartitionProtobuf.PartitionValue> partitionValues,
      boolean discoverPartitions,
      boolean inferPartitions) {
    this.fs = fs;
    this.startTime = context.getFunctionContext().getContextInformation().getQueryStartTime();
    this.rootPath = Path.of(dirListInputSplit.getRootPath());
    this.operatingPath = Path.of(dirListInputSplit.getOperatingPath());
    this.isFile = dirListInputSplit.getIsFile();
    this.hasVersion = dirListInputSplit.hasHasVersion() ? dirListInputSplit.getHasVersion() : true;
    this.hasFiles = dirListInputSplit.hasHasFiles() ? dirListInputSplit.getHasFiles() : false;
    if (this.hasFiles) {
      this.files = dirListInputSplit.getFilesList();
    }
    this.globPattern = dirListInputSplit.getGlobPattern();
    this.isRecursive = isRecursive;
    this.discoverPartitions = discoverPartitions;
    this.maxBatchSize = context.getTargetBatchSize();
    this.batchSize = context.getTargetBatchSize();
    this.footerReaderWidth =
        context.getMinorFragmentEndpoints() == null
            ? 1
            : context.getMinorFragmentEndpoints().size();
    if (!discoverPartitions) {
      currPartitionInfo =
          IcebergSerDe.partitionValueToIcebergPartition(partitionValues, tableSchema);
    }
    partitionParser = PartitionParser.getInstance(rootPath, inferPartitions);
    this.excludeFutureModTimes =
        context.getOptions().getOption(ExecConstants.DIR_LISTING_EXCLUDE_FUTURE_MOD_TIMES);
    logger.debug(String.format("Initialized DirListRecordReader with configs %s", this));
  }

  /**
   * Initializes the incremental size of the output batches. To initialize this feature it shall be
   * invoked just after the constructor. (This is actually a hack to avoid passing an unrelated
   * parameter through the file system interfaces.)
   */
  public void initIncrementalBatchSize() {
    initIncrementalBatchSize(INITIAL_BATCH_SIZE);
  }

  @Override
  public void setup(OutputMutator output) {
    this.outgoing = output;
    pathVector = (VarCharVector) outgoing.getVector(DirList.OUTPUT_SCHEMA.FILE_PATH);
    mtimeVector = (BigIntVector) outgoing.getVector(DirList.OUTPUT_SCHEMA.MODIFICATION_TIME);
    sizeVector = (BigIntVector) outgoing.getVector(DirList.OUTPUT_SCHEMA.FILE_SIZE);
    partitionInfoVector =
        (VarBinaryVector) outgoing.getVector(DirList.OUTPUT_SCHEMA.PARTITION_INFO);
    try {
      initDirIterator(isFile);
    } catch (IOException e) {
      throw new IllegalStateException("Error listing directory " + operatingPath.toString(), e);
    }
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  @Override
  public int next() {
    return nextBatch(0, batchSize);
  }

  public int nextBatch(int startOutIndex, int maxOutIndex) {
    // Default value will never be used.
    int generatedRecords = startOutIndex;
    try {
      generatedRecords = iterateDirectory(startOutIndex, maxOutIndex);
    } catch (RuntimeException | IOException e) {
      boolean hasExceptionHandled = true;
      String errorMessage = "Failed to list files of directory " + operatingPath.toString();
      if (isRateLimitingException(e)) {
        try {
          generatedRecords = retryer.call(() -> iterateDirectory(startOutIndex, maxOutIndex));
        } catch (Retryer.OperationFailedAfterRetriesException retriesException) {
          hasExceptionHandled = false;
          errorMessage =
              "With retry attempt failed to list files of directory " + operatingPath.toString();
        }
      } else {
        hasExceptionHandled = false;
      }
      if (!hasExceptionHandled) {
        throw UserException.dataReadError(e)
            .message(errorMessage)
            .addContext("Directory path", operatingPath.toString())
            .build(logger);
      }
    }
    logger.debug("Processed batch {} of size {}", batchesProcessed, batchSize);
    batchesProcessed++;
    setNextBatchSize();
    int finalGeneratedRecords = generatedRecords;
    outgoing.getVectors().forEach(valueVector -> valueVector.setValueCount(finalGeneratedRecords));

    logger.debug("Directory listing record reader finished. Output Records = {}", generatedRecords);
    return finalGeneratedRecords - startOutIndex;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public String toString() {
    return "DirListingRecordReader{"
        + "  rootPath="
        + rootPath
        + "  operatingPath="
        + operatingPath
        + ", startTime="
        + startTime
        + ", isRecursive="
        + isRecursive
        + ", discoverPartitions="
        + discoverPartitions
        + '}';
  }

  protected int iterateDirectory() throws IOException {
    return iterateDirectory(0, batchSize);
  }

  private int iterateDirectory(int outIndex, int maxIndex) throws IOException {
    logger.debug(String.format("Performing directory listing on path %s", rootPath));
    while (dirIterator != null && outIndex < maxIndex && dirIterator.hasNext()) {
      FileAttributes attributes = dirIterator.next();
      if (!attributes.isDirectory()
          && isValidPath(attributes.getPath())
          && (attributes.lastModifiedTime().toMillis() <= startTime || !excludeFutureModTimes)) {
        logger.debug(String.format("Add path %s to the output", attributes.getPath()));
        addToOutput(attributes, outIndex++);
      } else {
        if (excludeFutureModTimes && attributes.lastModifiedTime().toMillis() > startTime) {
          logger.info(
              "Excluding path {} - mod time {} is greater than query start time {}",
              attributes.getPath(),
              attributes.lastModifiedTime().toMillis(),
              startTime);
        } else if (!isValidPath(attributes.getPath())) {
          logger.info("Excluding path {} - not a valid path", attributes.getPath());
        } else {
          logger.debug("Excluding path {} - is a directory", attributes.getPath());
        }
      }
    }
    return outIndex;
  }

  @VisibleForTesting
  public void initIncrementalBatchSize(int batchSize) {
    this.batchSize = batchSize;
    this.incrementalBatchSize = true;
  }

  protected void initDirIterator(boolean isFile) throws IOException {
    try {
      if (isFile) {
        dirIterator = Collections.singletonList(fs.getFileAttributes(operatingPath)).iterator();
      } else if (hasFiles) {
        dirIterator = getFilesIterator();
      } else {
        dirIterator = getPathIterator();
      }
    } catch (IOException e) {
      if (isRateLimitingException(e)) {
        try {
          dirIterator = retryer.call(this::getPathIterator);
        } catch (Retryer.OperationFailedAfterRetriesException retriesException) {
          String retryErrorMessage = "Retry attempted ";
          if (e.getMessage() != null) {
            retryErrorMessage += " with error message: " + e.getMessage();
          } else {
            retryErrorMessage += " with empty error message";
          }
          throw new IOException(retryErrorMessage, e.getCause());
        }
      } else {
        throw e;
      }
    }
  }

  private Iterator<FileAttributes> getFilesIterator() throws IOException {
    return Iterators.transform(
        files.iterator(),
        path -> {
          String completePath = rootPath + "/" + path;
          try {
            return fs.getFileAttributes(Path.of(completePath));
          } catch (IOException e) {
            String errorMessage = "Failed to get details for file: " + completePath;
            throw UserException.dataReadError(e)
                .message(errorMessage)
                .addContext("File path: ", operatingPath.toString())
                .build(logger);
          }
        });
  }

  private Iterator<FileAttributes> getPathIterator() throws IOException {
    if (StringUtils.isNotEmpty(globPattern)) {
      return fs.glob(operatingPath.resolve(globPattern), PathFilters.ALL_FILES).iterator();
    }
    return fs.listFiles(operatingPath, isRecursive).iterator();
  }

  private boolean isRateLimitingException(Exception e) {
    boolean shouldRateLimit = true;
    if (e instanceof FileNotFoundException || e instanceof AccessDeniedException) {
      shouldRateLimit = false;
    }
    // In case of wrapped Runtime exception
    if (e.getCause() != null
        && (e.getCause() instanceof FileNotFoundException
            || e.getCause() instanceof AccessDeniedException)) {
      shouldRateLimit = false;
    }
    if (e.getMessage() != null
        && (e.getMessage().contains("ConditionNotMet")
            || e.getMessage().contains("PathNotFound"))) {
      shouldRateLimit = false;
    }
    // In case of wrapped Runtime exception
    if (e.getCause() != null
        && e.getCause().getMessage() != null
        && (e.getCause().getMessage().contains("ConditionNotMet")
            || e.getCause().getMessage().contains("PathNotFound"))) {
      shouldRateLimit = false;
    }

    if (e instanceof UserException) {
      shouldRateLimit = false;
    }

    if (e instanceof IllegalStateException) {
      shouldRateLimit = false;
    }

    logger.info(
        "Rate limit flag is {} for cause {} and message {} ",
        shouldRateLimit,
        e.getCause(),
        e.getMessage());
    return shouldRateLimit;
  }

  protected void addToOutput(FileAttributes fileStatus, int index) throws IOException {
    addPath(fileStatus, index);
    addMtimeAndSize(fileStatus, index);
    addPartitionInfo(fileStatus, index);
  }

  private void addPath(FileAttributes fileStatus, int index) {
    String path = fileStatus.getPath().toString();
    if (hasVersion) {
      path += String.format("?%s=%d", FILE_VERSION, fileStatus.lastModifiedTime().toMillis());
    }
    pathVector.setSafe(index, path.getBytes(StandardCharsets.UTF_8));
  }

  private void addMtimeAndSize(FileAttributes fileStatus, int index) {
    mtimeVector.setSafe(index, fileStatus.lastModifiedTime().toMillis());
    sizeVector.setSafe(index, fileStatus.size());
  }

  private void addPartitionInfo(FileAttributes fileStatus, int index) throws IOException {
    if (this.discoverPartitions) {
      currPartitionInfo = partitionParser.parsePartitionToPath(fileStatus.getPath());
    }

    // Serialize the IcebergPartitionData to byte array
    byte[] serilaziedIcebergPartitionData = IcebergSerDe.serializeToByteArray(currPartitionInfo);
    partitionInfoVector.setSafe(index, serilaziedIcebergPartitionData);
  }

  private void setNextBatchSize() {
    if (incrementalBatchSize) {
      if (batchSize < maxBatchSize && batchesProcessed % footerReaderWidth == 0) {
        batchSize *= 2;
      }
      batchSize = Math.min(maxBatchSize, batchSize);
    }
  }

  protected boolean isValidPath(Path path) {
    String relativePath = PathUtils.relativePath(path, rootPath);
    List<String> components = PathUtils.toPathComponents(relativePath);
    for (String pathComponent : components) {
      if (!PathFilters.NO_HIDDEN_FILES.test(Path.of(pathComponent))) {
        return false;
      }
    }
    return true;
  }
}
