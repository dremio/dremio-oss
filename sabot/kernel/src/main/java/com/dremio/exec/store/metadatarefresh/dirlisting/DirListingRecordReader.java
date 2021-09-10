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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.SchemaConverter;
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

/**
 * RecordReader which given a path for a root dir will produce a list of files in the directory
 * with their size, mtime and their partitionInfo.
 *
 * Input Parameters
 *
 * isRecursive - controls whether the listing is recursive or only top level
 *
 * lastReadSignatureMtime - ignore the files which are modified after the provided lastReadSignatureMtime
 *
 * partitionValues -
 *
 *  1) partitionValues == null (for filesystem datasets)
 *     Reader figure out partition values for each file. For filesystem datasets partition values depend on the level
 *     of nesting of the file relative to provided the rootDir.
 *
 *  2) If partitionValues is provided(for hive datasets) all files have the same partitionInfo.
 *
 * Output Vector -
 *
 * pathVector -  VarCharVector of file paths
 * sizeVector -  BigIntVector of file sizes
 * mtimeVector - BigIntVector of file modification times
 *
 * partitionInfoList - ListVector<Struct<key, value>>. For filesystem datasets
 * keys are dir0, dir1 ... depending on the level of nesting of the file and
 * the values are directory names.
 */
public class DirListingRecordReader implements RecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirListingRecordReader.class);

  private final FileSystem fs;
  protected final long lastReadSignatureMtime;
  protected final Path rootPath;
  private final Path operatingPath;
  private final boolean isRecursive;
  private final boolean discoverPartitions;
  private OutputMutator outgoing;
  protected Iterator<FileAttributes> dirIterator;
  private IcebergPartitionData currPartitionInfo;
  private int batchesProcessed = 0;
  private final int maxBatchSize;
  private final int footerReaderWidth;
  protected int batchSize = 32; // start with a small batch size
  private boolean isFile;

  //Output Vectors
  private BigIntVector mtimeVector;
  private BigIntVector sizeVector;
  private VarCharVector pathVector;
  private VarBinaryVector partitionInfoVector;

  public DirListingRecordReader(OperatorContext context,
                                FileSystem fs,
                                DirListInputSplitProto.DirListInputSplit dirListInputSplit,
                                boolean isRecursive,
                                BatchSchema tableSchema,
                                List<PartitionProtobuf.PartitionValue> partitionValues,
                                boolean discoverPartitions) {
    this.fs = fs;
    this.lastReadSignatureMtime = dirListInputSplit.getReadSignature();
    this.rootPath = Path.of(dirListInputSplit.getRootPath());
    this.operatingPath = Path.of(dirListInputSplit.getOperatingPath());
    this.isFile = dirListInputSplit.getIsFile();
    this.isRecursive = isRecursive;
    this.discoverPartitions = discoverPartitions;
    this.maxBatchSize = context.getTargetBatchSize();
    this.footerReaderWidth = context.getMinorFragmentEndpoints().size();
    if(!discoverPartitions) {
      currPartitionInfo = IcebergSerDe.partitionValueToIcebergPartition(partitionValues, tableSchema);
    }
    logger.debug(String.format("Initialized DirListRecordReader with configs %s", this));
  }

  @Override
  public void setup(OutputMutator output) {
    this.outgoing = output;
    pathVector = (VarCharVector) outgoing.getVector(DirList.OUTPUT_SCHEMA.FILE_PATH);
    mtimeVector = (BigIntVector) outgoing.getVector(DirList.OUTPUT_SCHEMA.MODIFICATION_TIME);
    sizeVector = (BigIntVector) outgoing.getVector(DirList.OUTPUT_SCHEMA.FILE_SIZE);
    partitionInfoVector = (VarBinaryVector) outgoing.getVector(DirList.OUTPUT_SCHEMA.PARTITION_INFO);
    initDirIterator(isFile);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  @Override
  public int next() {
    int generatedRecords;
    try {
      generatedRecords = iterateDirectory();
      logger.debug("Processed batch {} of size {}", batchesProcessed, batchSize);
      batchesProcessed++;
      setNextBatchSize();
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to list subdirectories of directory " + operatingPath.toString())
        .addContext("Directory path", operatingPath.toString())
        .build(logger);
    }

    int finalGeneratedRecords = generatedRecords;
    outgoing.getVectors().forEach(valueVector -> valueVector.setValueCount(finalGeneratedRecords));

    logger.debug("Directory listing record reader finished. Output Records = {}", generatedRecords);
    return generatedRecords;
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public String toString() {
    return "DirListingRecordReader{" +
      "  rootPath=" + rootPath +
      "  operatingPath=" + operatingPath +
      ", lastReadSignatureMtime=" + lastReadSignatureMtime +
      ", isRecursive=" + isRecursive +
      ", discoverPartitions=" + discoverPartitions +
      '}';
  }

  protected int iterateDirectory() throws IOException {
    int generatedRecords = 0;
    logger.debug(String.format("Performing directory listing on path %s", rootPath));
    while (dirIterator != null && generatedRecords < batchSize && dirIterator.hasNext()) {
      FileAttributes attributes = dirIterator.next();
      if (!attributes.isDirectory() && isValidPath(attributes.getPath()) && attributes.lastModifiedTime().toMillis() < lastReadSignatureMtime) {
        generatedRecords++;
        logger.debug(String.format("Add path %s to the output", attributes.getPath()));
        addToOutput(attributes, generatedRecords - 1);
      }
    }
    return generatedRecords;
  }

  @VisibleForTesting
  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  protected void initDirIterator(boolean isFile) {
    try {
      if(isFile) {
        dirIterator = Collections.singletonList(fs.getFileAttributes(operatingPath)).iterator();
      } else {
        dirIterator = fs.listFiles(operatingPath, isRecursive).iterator();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get the list files details.",e);
    }
  }

  protected void addToOutput(FileAttributes fileStatus, int index) throws IOException {
    addPath(fileStatus, index);
    addMtimeAndSize(fileStatus, index);
    addPartitionInfo(fileStatus, index);
  }

  private void addPath(FileAttributes fileStatus, int index) {
    final String pathWithMTime = String.format("%s?%s=%d", fileStatus.getPath(), FILE_VERSION, fileStatus.lastModifiedTime().toMillis());
    pathVector.setSafe(index, pathWithMTime.getBytes(StandardCharsets.UTF_8));
  }

  private void addMtimeAndSize(FileAttributes fileStatus, int index) {
    mtimeVector.set(index, fileStatus.lastModifiedTime().toMillis());
    sizeVector.set(index, fileStatus.size());
  }

  private void addPartitionInfo(FileAttributes fileStatus, int index) throws IOException {
    if(this.discoverPartitions) {
      currPartitionInfo = generatePartitionInfoForFileSystem(fileStatus.getPath());
    }

    //Serialize the IcebergPartitionData to byte array
    byte[] serilaziedIcebergPartitionData = IcebergSerDe.serializeToByteArray(currPartitionInfo);
    partitionInfoVector.setSafe(index, serilaziedIcebergPartitionData);
  }

  private IcebergPartitionData generatePartitionInfoForFileSystem(Path filePath) {
    String[] dirs = Path.withoutSchemeAndAuthority(rootPath).relativize(Path.withoutSchemeAndAuthority(filePath)).toString().split(Path.SEPARATOR);

    PartitionSpec.Builder partitionSpecBuilder = PartitionSpec
      .builderFor(buildIcebergSchema(dirs.length - 1));

    IntStream.range(0, dirs.length - 1).forEach(i -> partitionSpecBuilder.identity("dir" + i));

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpecBuilder.build().partitionType());
    IntStream.range(0, dirs.length - 1).forEach(i -> icebergPartitionData.setString(i, dirs[i]));

    return icebergPartitionData;
  }

  //Schema generated here is not related to dataset schema. This is generated just to populate partitionInfo
  private Schema buildIcebergSchema(int levels) {
    Field[] fields = new Field[levels];

    for(int i = 0; i < levels; i++) {
      fields[i] = CompleteType.VARCHAR.toField("dir" + i);
    }

    BatchSchema tableSchema =
      BatchSchema.of(fields);

    SchemaConverter schemaConverter = new SchemaConverter();
    return schemaConverter.toIcebergSchema(tableSchema);
  }

  private void setNextBatchSize() {
    if (batchSize < maxBatchSize && batchesProcessed % footerReaderWidth == 0) {
      batchSize *= 2;
    }
    batchSize = Math.min(maxBatchSize, batchSize);
  }

  protected boolean isValidPath(Path path) {
    String relativePath = PathUtils.relativePath(path, rootPath);
    List<String> components = PathUtils.toPathComponents(relativePath);
    for (String pathComponent: components) {
      if (!PathFilters.NO_HIDDEN_FILES.test(Path.of(pathComponent))) {
        return false;
      }
    }
    return true;
  }
}
