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

import static com.dremio.exec.ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD_PATH;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD_SIZE;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD_STATS_PARSED;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_METADATA;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_METADATA_PARTITION_COLS;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_METADATA_SCHEMA_STRING;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_PROTOCOL;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_REMOVE;
import static com.dremio.exec.store.deltalake.DeltaConstants.PROTOCOL_MIN_READER_VERSION;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STATS;
import static com.dremio.exec.store.deltalake.DeltaConstants.STATS_PARSED_NUM_RECORDS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Preconditions;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.exec.store.parquet.ParquetScanProjectedColumns;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.exec.store.parquet.SingleStreamProvider;
import com.dremio.exec.store.parquet2.ParquetRowiseReader;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

/**
 * DeltaLog checkpoint parquet reader, which extracts datasize estimate and schema from a checkpoint.
 */
public class DeltaLogCheckpointParquetReader implements DeltaLogReader {
  private static final Logger logger = LoggerFactory.getLogger(DeltaLogCheckpointParquetReader.class);
  private static final long SMALL_PARQUET_FILE_SIZE = 1_000_000L;
  private static final long NUM_ROWS_IN_DATA_FILE = 200_000L;
  private static final String BUFFER_ALLOCATOR_NAME = "deltalake-checkpoint-alloc";
  private static final List<SchemaPath> META_PROJECTED_COLS = ImmutableList.of(
    SchemaPath.getSimplePath(DELTA_FIELD_ADD),
    SchemaPath.getSimplePath(DELTA_FIELD_METADATA),
    SchemaPath.getSimplePath(DELTA_FIELD_PROTOCOL));
  private static final int BATCH_SIZE = 500;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  long netBytesAdded = 0, netRecordsAdded = 0, numFilesModified = 0;
  private MutableParquetMetadata parquetMetadata;
  private long maxFooterLen;
  private StructVector protocolVector;
  private VarCharVector schemaStringVector;
  private ListVector partitionColsVector;
  private StructVector addVector;
  private StructVector metaDataVector;
  private long netFilesAdded;
  private long estimatedNetBytesAdded;
  private long estimatedNetRecordsAdded;
  private String schemaString = null;
  private List<String> partitionCols;
  private boolean protocolVersionFound = false;
  private boolean schemaFound = false;

  private long rowSizeEstimateForSmallFile = 0L;
  private long rowSizeEstimateForLargeFile = 0L;

  // The factor by which the row count estimate in data files has to be multipled
  private double estimationFactor;
  private long numAddedFilesReadLimit;

  @Override
  public DeltaLogSnapshot parseMetadata(Path rootFolder, SabotContext context, FileSystem fs, List<FileAttributes> fileAttributesList, long version) throws IOException {
    maxFooterLen = context.getOptionManager().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR);
    estimationFactor = context.getOptionManager().getOption(ExecConstants.DELTALAKE_ROWCOUNT_ESTIMATION_FACTOR);
    numAddedFilesReadLimit = context.getOptionManager().getOption(ExecConstants.DELTALAKE_MAX_ADDED_FILE_ESTIMATION_LIMIT);
    try (BufferAllocator allocator = context.getAllocator().newChildAllocator(BUFFER_ALLOCATOR_NAME, 0, Long.MAX_VALUE);
         OperatorContextImpl operatorContext = new OperatorContextImpl(context.getConfig(), context.getDremioConfig(), allocator, context.getOptionManager(), BATCH_SIZE, context.getExpressionSplitCache());
         SampleMutator mutator = new SampleMutator(allocator)) {
      Collections.sort(fileAttributesList, new Comparator<FileAttributes>() {
        @Override
        public int compare(FileAttributes o1, FileAttributes o2) {
          return o1.getPath().toString().compareTo(o2.getPath().toString());
        }
      });
      FileAttributes fileAttributes = fileAttributesList.get(0);
      List<DatasetSplit> snapSplitsList = new ArrayList<>();
      long totalBlocks = 0L;
      for (FileAttributes fileAttributesCurrent : fileAttributesList) {
        logger.debug("Reading footer and generating splits for checkpoint parquet file {}", fileAttributesCurrent.getPath());
        MutableParquetMetadata currentMetadata = readCheckpointParquetFooter(fs, fileAttributesCurrent.getPath(), fileAttributesCurrent.size());

        totalBlocks += currentMetadata.getBlocks().size();
        populateAddedFiles(currentMetadata.getBlocks());
        snapSplitsList.addAll(generateSplits(fileAttributesCurrent, version, currentMetadata));

        if (checkParquetContainsMetadata(currentMetadata.getBlocks())) {
          fileAttributes = fileAttributesCurrent;
          this.parquetMetadata = currentMetadata;
        }
      }
      Preconditions.checkState(totalBlocks > 0, "Illegal Deltalake checkpoint parquet file(s) with no row groups");
      final CompressionCodecFactory codec = CodecFactory.createDirectCodecFactory(
        new Configuration(), new ParquetDirectByteBufferAllocator(operatorContext.getAllocator()), 0);

      // we know that these are deltalake files and do not have the date corruption bug in Drill
      final SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
        .readInt96AsTimeStamp(operatorContext.getOptions().getOption(PARQUET_READER_INT96_AS_TIMESTAMP).getBoolVal())
        .dateCorruptionStatus(ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_NO_CORRUPTION)
        .build();

      int numRowsRead = 0;
      int numFilesReadToEstimateRowCount = 0;
      long prevRecordCntEstimate = 0;
      boolean isRowCountEstimateConverged = false;
      try (InputStreamProvider streamProvider = new SingleStreamProvider(fs, fileAttributes.getPath(), fileAttributes.size(),
        maxFooterLen, false, parquetMetadata, operatorContext, false)) {

        for (int rowGroupIdx = 0; rowGroupIdx < parquetMetadata.getBlocks().size() && !protocolVersionFound && !schemaFound; ++rowGroupIdx) {
          long rowCount = parquetMetadata.getBlocks().get(rowGroupIdx).getRowCount();
          long noOfBatches = rowCount / BATCH_SIZE + 1;

          if (protocolVersionFound && schemaFound && (isRowCountEstimateConverged || numFilesReadToEstimateRowCount > numAddedFilesReadLimit)) {
            break;
          }

          try (RecordReader reader = new ParquetRowiseReader(operatorContext, parquetMetadata, rowGroupIdx,
            fileAttributes.getPath().toString(), ParquetScanProjectedColumns.fromSchemaPaths(META_PROJECTED_COLS),
            fs, schemaHelper, streamProvider, codec, true)) {
            reader.setup(mutator);
            mutator.allocate(BATCH_SIZE);
            // Read the parquet file to populate inner list types
            mutator.getContainer().buildSchema(BatchSchema.SelectionVectorMode.NONE);
            long batchesRead = 0;
            while (batchesRead < noOfBatches) {
              int recordCount = reader.next();
              numRowsRead += recordCount;
              batchesRead++;
              prepareValueVectors(mutator);
              numFilesReadToEstimateRowCount += estimateStats(fs, rootFolder, recordCount);
              protocolVersionFound = protocolVersionFound || assertMinReaderVersion();
              schemaFound = schemaFound || findSchemaAndPartitionCols();
              long newRecordCountEstimate = numFilesReadToEstimateRowCount == 0 ? 0 : Math.round((netRecordsAdded * netFilesAdded * 1.0) / numFilesReadToEstimateRowCount);
              isRowCountEstimateConverged = prevRecordCntEstimate == 0 ? false :
                      isRowCountEstimateConverged || isRecordEstimateConverged(prevRecordCntEstimate, newRecordCountEstimate);
              if (protocolVersionFound && schemaFound && (isRowCountEstimateConverged || numFilesReadToEstimateRowCount > numAddedFilesReadLimit)) {
                break;
              }
              prevRecordCntEstimate = newRecordCountEstimate;
              // reset vectors as they'll be reused in next batch
              resetVectors();
            }
          } catch (Exception e) {
            logger.error("IOException occurred while reading deltalake table", e);
            throw new IOException(e);
          }
        }
      }

      if (!protocolVersionFound || !schemaFound) {
        UserException.invalidMetadataError()
          .message("Metadata read Failed. Malformed checkpoint parquet {}", fileAttributes.getPath())
          .build(logger);
      }

        logger.debug("Total rows read: {}", numRowsRead);
        estimatedNetBytesAdded = Math.round((netBytesAdded * netFilesAdded * 1.0) / numFilesReadToEstimateRowCount);
        estimatedNetRecordsAdded = Math.round((netRecordsAdded * netFilesAdded * 1.0) / numFilesReadToEstimateRowCount);

      logger.debug("Checkpoint parquet file {}, netFilesAdded {}, estiamteBytesAdded {}, estimatedRecordsAdded {}",
        fileAttributes.getPath(), netFilesAdded, estimatedNetBytesAdded, estimatedNetRecordsAdded);
      final DeltaLogSnapshot snap = new DeltaLogSnapshot("UNKNOWN", netFilesAdded,
        estimatedNetBytesAdded, estimatedNetRecordsAdded, netFilesAdded, System.currentTimeMillis(), true);
      snap.setSchema(schemaString, partitionCols);
      snap.setSplits(snapSplitsList);
      return snap;
    } catch (Exception e) {
      logger.error("IOException occurred while reading deltalake table", e);
      throw new IOException(e);
    }
  }

  private void resetVectors() {
    addVector.reset();
    protocolVector.reset();
    metaDataVector.reset();
  }

  private MutableParquetMetadata readCheckpointParquetFooter(FileSystem fs, Path filePath, long fileSize) throws Exception {
    try (SingleStreamProvider singleStreamProvider = new SingleStreamProvider(fs, filePath, fileSize, maxFooterLen, false, null, null, false)) {
      final MutableParquetMetadata footer = singleStreamProvider.getFooter();
      return footer;
    }
  }

  private void prepareValueVectors(SampleMutator mutator) {
    protocolVector = (StructVector) mutator.getVector(DELTA_FIELD_PROTOCOL);
    metaDataVector = (StructVector) mutator.getVector(DELTA_FIELD_METADATA);
    schemaStringVector = (VarCharVector) metaDataVector.getChild(DELTA_FIELD_METADATA_SCHEMA_STRING);
    partitionColsVector = (ListVector) metaDataVector.getChild(DELTA_FIELD_METADATA_PARTITION_COLS);
    addVector = (StructVector) mutator.getVector(DELTA_FIELD_ADD);
  }

  private boolean assertMinReaderVersion() {
    for (int i = 0; i < BATCH_SIZE; ++i) {
      if (!protocolVector.isNull(i)) {
        int minReaderVersion = ((IntVector) protocolVector.getChild(PROTOCOL_MIN_READER_VERSION)).get(i);
        Preconditions.checkState(minReaderVersion <= 1,
          "Protocol version {} is incompatible for Dremio plugin", minReaderVersion);
        return true;
      }
    }

    return false;
  }

  private boolean findSchemaAndPartitionCols() {
    boolean schemaFound = false;
    for (int i = 0; i < BATCH_SIZE; ++i) {
      if (!schemaStringVector.isNull(i)) {
        schemaString = schemaStringVector.getObject(i).toString();
        schemaFound = true;
      }

      if (!partitionColsVector.isNull(i)) {
        partitionCols = (List<String>) ((JsonStringArrayList) partitionColsVector.getObject(i)).stream().map(x -> x.toString()).collect(Collectors.toList());
        logger.debug("Partition cols are {}", partitionCols);
      }
    }
    return schemaFound;
  }

  private void populateAddedFiles(List<BlockMetaData> blockMetaDataList) throws IOException {
    for (BlockMetaData blockMetaData : blockMetaDataList) {
      long numRows = blockMetaData.getRowCount();
      long numFilesAdded, numFilesRemoved;
      try {
        ColumnChunkMetaData addColChunk = blockMetaData.getColumns().stream().filter(
          colChunk -> colChunk.getPath().equals(ColumnPath.get(DELTA_FIELD_ADD, "path"))).findFirst().get();
        ColumnChunkMetaData removeColChunk = blockMetaData.getColumns().stream().filter(
          colChunk -> colChunk.getPath().equals(ColumnPath.get(DELTA_FIELD_REMOVE, "path"))).findFirst().get();
        numFilesAdded = numRows - addColChunk.getStatistics().getNumNulls();
        numFilesRemoved = numRows - removeColChunk.getStatistics().getNumNulls();
        netFilesAdded += numFilesAdded;
        numFilesModified += numFilesAdded + numFilesRemoved;
      } catch (NoSuchElementException e) {
        logger.error("Path for added or removed files does not exist in add/remove columns");
        throw new IOException("Error occurred while reading deltalake table", e);
      }
    }
  }

  private boolean checkParquetContainsMetadata(List<BlockMetaData> blockMetaDataList) throws IOException {
    long numMetadataEntries = 0;
    for (BlockMetaData blockMetaData : blockMetaDataList) {
      long numRows = blockMetaData.getRowCount();
      try {
        ColumnChunkMetaData metadataColChunk = blockMetaData.getColumns().stream().filter(
          colChunk -> colChunk.getPath().equals(ColumnPath.get(DELTA_FIELD_METADATA, DELTA_FIELD_METADATA_SCHEMA_STRING))).findFirst().get();
        numMetadataEntries = numRows - metadataColChunk.getStatistics().getNumNulls();
        if (numMetadataEntries > 0) {
          break;
        }
      } catch (NoSuchElementException e) {
        logger.error("Path for added or removed files does not exist in add/remove columns");
        throw new IOException("Error occurred while reading deltalake table", e);
      }
    }
    return numMetadataEntries > 0;
  }

  private int estimateStats(FileSystem fs, Path rootFolder, int recordCount) throws Exception {
    int numFilesRead = 0;
    BigIntVector sizeVector = (BigIntVector) addVector.getChild(DELTA_FIELD_ADD_SIZE);
    StructVector statsStruct = (StructVector) addVector.getChild(DELTA_FIELD_ADD_STATS_PARSED);
    VarCharVector statsJson = (VarCharVector) addVector.getChild(SCHEMA_STATS);

    BigIntVector numRecordsVector = null;
    if (statsStruct != null) {
      numRecordsVector = (BigIntVector) (statsStruct.getChild(STATS_PARSED_NUM_RECORDS));
    }
    VarCharVector pathVector = (VarCharVector) addVector.getChild(DELTA_FIELD_ADD_PATH);

    for (int i = 0; i < recordCount; ++i) {
      if (!addVector.isNull(i)) {
        numFilesRead++;
        netBytesAdded += sizeVector.get(i);
        boolean statsNotFound = true;
        if ((numRecordsVector != null) && !numRecordsVector.isNull(i)) {
          netRecordsAdded += numRecordsVector.get(i);
          statsNotFound = false;
        } else if (statsJson != null && !statsJson.isNull(i)) {
          JsonNode statsJsonNode = OBJECT_MAPPER.readTree(statsJson.get(i));
          JsonNode numRecordsNode = statsJsonNode.get(STATS_PARSED_NUM_RECORDS);
          statsNotFound = numRecordsNode == null;
          if (!statsNotFound) {
            netRecordsAdded += numRecordsNode.asInt();
          }
        }

        if (rootFolder != null && statsNotFound) {
          // numRecords is not available
          long fileSize = sizeVector.get(i);
          Path fullFilePath = rootFolder.resolve(new String(pathVector.get(i), StandardCharsets.UTF_8));
          fullFilePath = Path.of(URLDecoder.decode(fullFilePath.toString(), "UTF-8"));

          long numRecords;
          try {
            numRecords = estimateRecordsAdded(fs, fullFilePath, fileSize);
            logger.debug("Num records not available in {}, fileSize {}, estimated row count {}", pathVector.getObject(i), fileSize, numRecords);
          } catch (FileNotFoundException fnfe) {
            // Should never happen in production. If this happens in production, this means that the metadata is inconsistent
            // The query will anyway fail at execution time
            numRecords = NUM_ROWS_IN_DATA_FILE;
            logger.debug("Data file {} not found, estimated row count {}", pathVector.getObject(i), numRecords);
          }
          netRecordsAdded += numRecords;
        }
      }
    }

    return numFilesRead;
  }

  private long estimateRecordsAdded(FileSystem fs, Path filePath, long fileSize) throws Exception {
    if (fileSize < SMALL_PARQUET_FILE_SIZE) {
      if (rowSizeEstimateForSmallFile > 0L) {
        double estimate = (fileSize * 1.0d) / rowSizeEstimateForSmallFile;
        estimate *= estimationFactor;
        return Math.round(estimate);
      } else {
        logger.debug("Finding rowSizeEstimate for small files using {}", filePath);
        MutableParquetMetadata footer = readCheckpointParquetFooter(fs, filePath, fileSize);
        long totalRecordCount = footer.getBlocks().stream().mapToLong(x -> x.getRowCount()).sum();
        footer = null; // release the footer object early

        if (totalRecordCount > 0) {
          rowSizeEstimateForSmallFile = Math.round((fileSize * 1.0d) / totalRecordCount);
          logger.debug("Number of records={}, fileSize={}, estimatedRowSize={}", totalRecordCount, fileSize, rowSizeEstimateForSmallFile);
        }
        return totalRecordCount;
      }
    }

    if (rowSizeEstimateForLargeFile > 0L) {
      double estimate = (fileSize * 1.0d) / rowSizeEstimateForLargeFile;
      estimate *= estimationFactor;
      return Math.round(estimate);
    } else {
      logger.debug("Finding rowSizeEstimate for large files using {}", filePath);
      MutableParquetMetadata footer = readCheckpointParquetFooter(fs, filePath, fileSize);
      long totalRecordCount = footer.getBlocks().stream().mapToLong(x -> x.getRowCount()).sum();
      footer = null;

      if (totalRecordCount > 0) {
        rowSizeEstimateForLargeFile = Math.round((fileSize * 1.0d) / totalRecordCount);
        logger.debug("Number of records={}, fileSize={}, estimatedRowSize={}", totalRecordCount, fileSize, rowSizeEstimateForLargeFile);
      }
      return totalRecordCount;
    }
  }

  private List<DatasetSplit> generateSplits(FileAttributes fileAttributes, long version, MutableParquetMetadata currentMetadata) {
    FileProtobuf.FileSystemCachedEntity fileProto = FileProtobuf.FileSystemCachedEntity
      .newBuilder()
      .setPath(fileAttributes.getPath().toString())
      .setLength(fileAttributes.size())
      .setLastModificationTime(0) // using 0 as the mtime to signify that these splits are immutable
      .build();

    int rowGrpIdx = 0;

    final List<DatasetSplit> datasetSplits = new ArrayList<>(currentMetadata.getBlocks().size());
    for (BlockMetaData blockMetaData : currentMetadata.getBlocks()) {
      final DeltaLakeProtobuf.DeltaCommitLogSplitXAttr deltaExtended = DeltaLakeProtobuf.DeltaCommitLogSplitXAttr
        .newBuilder().setRowGroupIndex(rowGrpIdx).setVersion(version).build();
      rowGrpIdx++;

      final EasyProtobuf.EasyDatasetSplitXAttr splitExtended = EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
        .setPath(fileAttributes.getPath().toString())
        .setStart(blockMetaData.getStartingPos())
        .setLength(blockMetaData.getCompressedSize())
        .setUpdateKey(fileProto)
        .setExtendedProperty(deltaExtended.toByteString())
        .build();
      datasetSplits.add(DatasetSplit.of(Collections.EMPTY_LIST, blockMetaData.getCompressedSize(),
        blockMetaData.getRowCount(), splitExtended::writeTo));
    }
    return datasetSplits;
  }

  private static JsonNode findNode(JsonNode node, String... paths) {
    for (String path : paths) {
      if (node==null) {
        return node;
      }
      node = node.get(path);
    }
    return node;
  }

  // get an optional value
  private static <T> T get(JsonNode node, T defaultVal, Function<JsonNode, T> typeFunc, String... paths) {
    node = findNode(node, paths);
    return (node==null) ? defaultVal:typeFunc.apply(node);
  }

  private static boolean isRecordEstimateConverged(long prevRecordCntEstimate, long newRecordCntEstimate) {
    long diff = Math.abs(newRecordCntEstimate - prevRecordCntEstimate);
    int deltaPercentage = 30;
    return diff * 100 * 1.0 / prevRecordCntEstimate < deltaPercentage;
  }
}
