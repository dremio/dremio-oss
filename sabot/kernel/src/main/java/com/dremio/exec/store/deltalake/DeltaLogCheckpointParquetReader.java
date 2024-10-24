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

import static com.dremio.exec.ExecConstants.ENABLE_MAP_DATA_TYPE;
import static com.dremio.exec.ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD_PATH;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD_SIZE;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD_STATS_PARSED;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_METADATA;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_METADATA_CONFIGURATION;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_METADATA_PARTITION_COLS;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_METADATA_SCHEMA_STRING;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_PROTOCOL;
import static com.dremio.exec.store.deltalake.DeltaConstants.FORMAT_NOT_SUPPORTED_VERSION;
import static com.dremio.exec.store.deltalake.DeltaConstants.PROTOCOL_MIN_READER_VERSION;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STATS;
import static com.dremio.exec.store.deltalake.DeltaConstants.STATS_PARSED_NUM_RECORDS;
import static com.dremio.exec.store.deltalake.DeltaLogReaderUtils.parseStatsFromJson;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.exec.store.parquet.AllRowGroupsParquetReader;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.InputStreamProviderFactory;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.ParquetFilterCreator;
import com.dremio.exec.store.parquet.ParquetFilters;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.ParquetReaderOptions;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.exec.store.parquet.ParquetScanProjectedColumns;
import com.dremio.exec.store.parquet.ParquetTypeHelper;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.exec.store.parquet.UnifiedParquetReader;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DeltaLog checkpoint parquet reader, which extracts datasize estimate and schema from a
 * checkpoint.
 */
public class DeltaLogCheckpointParquetReader implements DeltaLogReader {
  private static final Logger logger =
      LoggerFactory.getLogger(DeltaLogCheckpointParquetReader.class);
  private static final long SMALL_PARQUET_FILE_SIZE = 1_000_000L;
  private static final long NUM_ROWS_IN_DATA_FILE = 200_000L;
  private static final String BUFFER_ALLOCATOR_NAME = "deltalake-checkpoint-alloc";
  private static final List<SchemaPath> META_PROJECTED_COLS =
      ImmutableList.of(
          SchemaPath.getSimplePath(DELTA_FIELD_ADD),
          SchemaPath.getSimplePath(DELTA_FIELD_METADATA),
          SchemaPath.getSimplePath(DELTA_FIELD_PROTOCOL));
  private static final int BATCH_SIZE = 500;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private long netBytesAdded = 0;
  private long netRecordsAdded = 0;
  private long maxFooterLen;
  private StructVector protocolVector;
  private VarCharVector schemaStringVector;
  private ListVector partitionColsVector;
  private MapVector configurationVector;
  private StructVector addVector;
  private StructVector metaDataVector;
  private long netFilesAdded;
  private String schemaString = null;
  private List<String> partitionCols = null;
  private Map<String, String> configuration = null;
  private boolean protocolVersionFound = false;
  private boolean schemaFound = false;

  private long rowSizeEstimateForSmallFile = 0L;
  private long rowSizeEstimateForLargeFile = 0L;
  private int numRowsRead = 0;
  private int numFilesReadToEstimateRowCount = 0;

  // The factor by which the row count estimate in data files has to be multiplied
  private double estimationFactor;
  private long numAddedFilesReadLimit;
  private int minReaderVersionSupported = 2;
  private long noStatsFileSize = 0L;

  @Override
  public DeltaLogSnapshot parseMetadata(
      Path rootFolder,
      SabotContext context,
      FileSystem fs,
      List<FileAttributes> fileAttributesList,
      long version)
      throws IOException {
    maxFooterLen =
        context.getOptionManager().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR);
    estimationFactor =
        context.getOptionManager().getOption(ExecConstants.DELTALAKE_ROWCOUNT_ESTIMATION_FACTOR);
    numAddedFilesReadLimit =
        context
            .getOptionManager()
            .getOption(ExecConstants.DELTALAKE_MAX_ADDED_FILE_ESTIMATION_LIMIT);
    final boolean isFullRowCountEnabled =
        DeltaLogReaderUtils.isFullRowCountEnabled(context.getOptionManager());
    final boolean isMapDataTypeEnabled =
        context.getOptionManager().getOption(ExecConstants.ENABLE_MAP_DATA_TYPE);

    try (BufferAllocator allocator =
            context.getAllocator().newChildAllocator(BUFFER_ALLOCATOR_NAME, 0, Long.MAX_VALUE);
        OperatorContextImpl operatorContext = createOperatorContext(context, allocator);
        SampleMutator mutator = new SampleMutator(allocator)) {
      fileAttributesList.sort(Comparator.comparing(o -> o.getPath().toString()));

      List<DatasetSplit> snapSplitsList = new ArrayList<>();

      for (FileAttributes fileAttributesCurrent : fileAttributesList) {
        // calculate row estimations for each checkpoint file if it is a multi-part checkpoint
        long totalBlocks = 0L;
        logger.debug(
            "Reading footer for checkpoint parquet file {}", fileAttributesCurrent.getPath());

        MutableParquetMetadata footer =
            readCheckpointParquetFooter(
                fs,
                operatorContext,
                fileAttributesCurrent.getPath(),
                fileAttributesCurrent.size(),
                fileAttributesCurrent.lastModifiedTime().toMillis());
        totalBlocks += footer.getBlocks().size();
        populateAddedFiles(footer.getBlocks());
        snapSplitsList.addAll(generateSplits(fileAttributesCurrent, version, footer));

        final SchemaDerivationHelper schemaHelper =
            SchemaDerivationHelper.builder()
                .readInt96AsTimeStamp(
                    operatorContext
                        .getOptions()
                        .getOption(PARQUET_READER_INT96_AS_TIMESTAMP)
                        .getBoolVal())
                .dateCorruptionStatus(
                    ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_NO_CORRUPTION)
                .mapDataTypeEnabled(operatorContext.getOptions().getOption(ENABLE_MAP_DATA_TYPE))
                .build();

        // create mutator and BatchSchema for UnifiedParquetReader initialization
        Field addField = createFieldFromFooter(DELTA_FIELD_ADD, footer, schemaHelper);
        Field metadataField = createFieldFromFooter(DELTA_FIELD_METADATA, footer, schemaHelper);
        Field protocolField = createFieldFromFooter(DELTA_FIELD_PROTOCOL, footer, schemaHelper);

        mutator.addField(metadataField, StructVector.class);
        mutator.addField(addField, StructVector.class);
        mutator.addField(protocolField, StructVector.class);
        mutator.getAndResetSchemaChanged();
        mutator.getContainer().buildSchema(BatchSchema.SelectionVectorMode.NONE);

        final BatchSchema tableSchema =
            new BatchSchema(ImmutableList.of(addField, metadataField, protocolField));

        try (AllRowGroupsParquetReader parquetReader =
            createParquetReader(
                operatorContext, fs, fileAttributesCurrent.getPath(), tableSchema)) {
          parquetReader.setup(mutator);
          mutator.allocate(BATCH_SIZE);

          int recordCount = parquetReader.next();
          if (isFullRowCountEnabled) {
            processWithFullRowCount(
                parquetReader, rootFolder, operatorContext, fs, recordCount, mutator);
          } else {
            processWithEstimatedRowCount(
                parquetReader, rootFolder, operatorContext, fs, recordCount, mutator);
          }
        }

        if (totalBlocks == 0) {
          throw new IOException("Illegal Deltalake checkpoint parquet file(s) with no row groups");
        }

        logger.debug(
            "Checkpoint parquet file {}, numRowsRead {}, numFilesReadToEstimateRowCount {}",
            fileAttributesCurrent.getPath(),
            numRowsRead,
            numFilesReadToEstimateRowCount);
      }

      // None of the checkpoint files have the metadata - protocol version and schema
      if (!protocolVersionFound || !schemaFound) {
        throw UserException.invalidMetadataError()
            .message(
                "Metadata read Failed. Malformed checkpoint parquet files(s) %s",
                fileAttributesList.stream()
                    .map(FileAttributes::getPath)
                    .collect(Collectors.toList()))
            .buildSilently();
      }

      if (noStatsFileSize > 0L) {
        // when noStatsFileSize>0, we need further estimate rowCount by using noStatsFileSize /
        // estimatedRecordSize
        // where estimatedRecordSize is based on schema
        if (schemaString != null && !schemaString.isEmpty()) {
          // don't care about column mapping here
          BatchSchema schema =
              DeltaLakeSchemaConverter.newBuilder()
                  .withMapEnabled(isMapDataTypeEnabled)
                  .build()
                  .fromSchemaString(schemaString);
          int estimatedRecordSize =
              schema.estimateRecordSize(
                  (int)
                      operatorContext
                          .getOptions()
                          .getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE),
                  (int)
                      operatorContext
                          .getOptions()
                          .getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE));
          netRecordsAdded += noStatsFileSize / estimatedRecordSize;
          logger.warn(
              "stats info for {} not supplied, estimated row count {} based on file size",
              fileAttributesList.stream().map(FileAttributes::getPath).collect(Collectors.toList()),
              noStatsFileSize / estimatedRecordSize);
        } else {
          // In theory, it should not happen
          netRecordsAdded += noStatsFileSize;
          logger.warn(
              "stats info for {} not supplied, estimated row count {} based on file size with NO schema",
              fileAttributesList.stream().map(FileAttributes::getPath).collect(Collectors.toList()),
              noStatsFileSize);
        }
      }

      logger.debug("Total rows read for combined multi-part checkpoint files: {}", numRowsRead);
      long estimatedNetBytesAdded =
          Math.round((netBytesAdded * netFilesAdded * 1.0) / numFilesReadToEstimateRowCount);
      long estimatedNetRecordsAdded =
          Math.round((netRecordsAdded * netFilesAdded * 1.0) / numFilesReadToEstimateRowCount);

      logger.debug(
          "Stat Estimations: netFilesAdded {}, estimatedNetBytesAdded {}, estimatedNetRecordsAdded {}",
          netFilesAdded,
          estimatedNetBytesAdded,
          estimatedNetRecordsAdded);
      final DeltaLogSnapshot snap =
          new DeltaLogSnapshot(
              "UNKNOWN",
              netFilesAdded,
              estimatedNetBytesAdded,
              estimatedNetRecordsAdded,
              netFilesAdded,
              System.currentTimeMillis(),
              true);
      snap.setSchema(schemaString, partitionCols, configuration);
      snap.setSplits(snapSplitsList);
      return snap;
    } catch (IOException e) {
      logger.error("IOException occurred while reading deltalake table", e);
      throw e;
    } catch (Exception e) {
      logger.error("Exception occurred while reading deltalake table", e);
      throw new IOException(e);
    }
  }

  private void processWithFullRowCount(
      AllRowGroupsParquetReader parquetReader,
      Path rootFolder,
      OperatorContextImpl operatorContext,
      FileSystem fs,
      int recordCount,
      SampleMutator mutator)
      throws Exception {
    while (recordCount > 0) {
      prepareValueVectors(mutator);
      numRowsRead += getStats(operatorContext, fs, rootFolder, recordCount, true);
      protocolVersionFound = protocolVersionFound || assertMinReaderVersion();
      schemaFound = schemaFound || findSchemaAndPartitionCols();
      numFilesReadToEstimateRowCount = numRowsRead;
      // reset vectors as they'll be reused in next batch
      resetVectors();
      recordCount = parquetReader.next();
    }
  }

  private void processWithEstimatedRowCount(
      AllRowGroupsParquetReader parquetReader,
      Path rootFolder,
      OperatorContextImpl operatorContext,
      FileSystem fs,
      int recordCount,
      SampleMutator mutator)
      throws Exception {
    boolean isRowCountEstimateConverged = false;
    long prevRecordCntEstimate = 0;
    while (recordCount > 0) {
      numRowsRead += recordCount;
      prepareValueVectors(mutator);
      numFilesReadToEstimateRowCount +=
          getStats(operatorContext, fs, rootFolder, recordCount, false);
      protocolVersionFound = protocolVersionFound || assertMinReaderVersion();
      schemaFound = schemaFound || findSchemaAndPartitionCols();
      long newRecordCountEstimate =
          numFilesReadToEstimateRowCount == 0
              ? 0
              : Math.round(
                  (netRecordsAdded * netFilesAdded * 1.0) / numFilesReadToEstimateRowCount);
      isRowCountEstimateConverged =
          prevRecordCntEstimate == 0
              ? false
              : isRowCountEstimateConverged
                  || isRecordEstimateConverged(prevRecordCntEstimate, newRecordCountEstimate);
      if (protocolVersionFound
          && schemaFound
          && (isRowCountEstimateConverged
              || numFilesReadToEstimateRowCount > numAddedFilesReadLimit)) {
        break;
      }
      prevRecordCntEstimate = newRecordCountEstimate;
      // reset vectors as they'll be reused in next batch
      resetVectors();
      recordCount = parquetReader.next();
    }
  }

  private OperatorContextImpl createOperatorContext(
      SabotContext context, BufferAllocator allocator) {
    final OperatorStats stats = new OperatorStats(new OpProfileDef(0, 0, 0), allocator);
    final QueryOptionManager queryOptionManager =
        new QueryOptionManager(context.getOptionValidatorListing());
    final long metadataFieldSize =
        context.getOptionManager().getOption(ExecConstants.DELTALAKE_METADATA_FIELD_SIZE_BYTES);
    queryOptionManager.setOption(
        OptionValue.createLong(
            OptionValue.OptionType.QUERY,
            ExecConstants.LIMIT_FIELD_SIZE_BYTES.getOptionName(),
            metadataFieldSize));
    final OptionManager optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionManager(context.getOptionManager())
            .withOptionManager(queryOptionManager)
            .build();
    return new OperatorContextImpl(
        context.getConfig(),
        context.getDremioConfig(),
        allocator,
        optionManager,
        stats,
        BATCH_SIZE,
        context.getExpressionSplitCache());
  }

  private Field createFieldFromFooter(
      String topLevelFieldName,
      MutableParquetMetadata footer,
      SchemaDerivationHelper schemaDerivationHelper) {
    Type toLevelFieldType = footer.getFileMetaData().getSchema().getType(topLevelFieldName);
    Optional<Field> returnField =
        ParquetTypeHelper.toField(toLevelFieldType, schemaDerivationHelper);
    return returnField.orElseThrow(
        () ->
            UserException.invalidMetadataError()
                .message("Error while decoding [%s] field in checkpoint file", topLevelFieldName)
                .buildSilently());
  }

  private AllRowGroupsParquetReader createParquetReader(
      OperatorContext context, FileSystem fs, Path filePath, BatchSchema schema) {
    final InputStreamProviderFactory inputStreamProviderFactory =
        context
            .getConfig()
            .getInstance(
                InputStreamProviderFactory.KEY,
                InputStreamProviderFactory.class,
                InputStreamProviderFactory.DEFAULT);
    final ParquetReaderFactory readerFactory =
        UnifiedParquetReader.getReaderFactory(context.getConfig());
    List<String> dataset = new ArrayList<>();
    dataset.add(filePath.toString());
    return new AllRowGroupsParquetReader(
        context,
        filePath,
        dataset,
        fs,
        inputStreamProviderFactory,
        readerFactory,
        schema,
        ParquetScanProjectedColumns.fromSchemaPaths(META_PROJECTED_COLS),
        ParquetFilters.NONE,
        ParquetReaderOptions.from(context.getOptions()));
  }

  private InputStreamProvider createInputStreamProvider(
      OperatorContext context, FileSystem fs, Path filePath, long fileSize, long lTime) {
    try {
      List<String> dataset = new ArrayList<>();
      dataset.add(filePath.toString());
      // creating an InputStreamProvider based on InputStreamProviderFactory
      final InputStreamProviderFactory inputStreamProviderFactory =
          context
              .getConfig()
              .getInstance(
                  InputStreamProviderFactory.KEY,
                  InputStreamProviderFactory.class,
                  InputStreamProviderFactory.DEFAULT);
      InputStreamProvider inputStreamProvider =
          inputStreamProviderFactory.create(
              fs,
              context,
              filePath,
              fileSize,
              maxFooterLen,
              ParquetScanProjectedColumns.fromSchemaPaths(META_PROJECTED_COLS),
              null,
              null,
              f -> 0,
              false,
              dataset,
              lTime,
              false,
              false,
              ParquetFilters.NONE,
              ParquetFilterCreator.DEFAULT,
              InputStreamProviderFactory.DEFAULT_NON_PARTITION_COLUMN_RF);
      return inputStreamProvider;
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  private void resetVectors() {
    addVector.reset();
    protocolVector.reset();
    metaDataVector.reset();
  }

  private MutableParquetMetadata readCheckpointParquetFooter(
      FileSystem fs, OperatorContext operatorContext, Path filePath, long fileSize, long lTime)
      throws Exception {
    try (InputStreamProvider inputStreamProvider =
        createInputStreamProvider(operatorContext, fs, filePath, fileSize, lTime)) {
      return inputStreamProvider.getFooter();
    }
  }

  private void prepareValueVectors(SampleMutator mutator) {
    protocolVector = (StructVector) mutator.getVector(DELTA_FIELD_PROTOCOL);
    metaDataVector = (StructVector) mutator.getVector(DELTA_FIELD_METADATA);
    schemaStringVector =
        (VarCharVector) metaDataVector.getChild(DELTA_FIELD_METADATA_SCHEMA_STRING);
    partitionColsVector = (ListVector) metaDataVector.getChild(DELTA_FIELD_METADATA_PARTITION_COLS);
    configurationVector = (MapVector) metaDataVector.getChild(DELTA_FIELD_METADATA_CONFIGURATION);
    addVector = (StructVector) mutator.getVector(DELTA_FIELD_ADD);
  }

  private boolean assertMinReaderVersion() throws IOException {
    for (int i = 0; i < BATCH_SIZE; ++i) {
      if (!protocolVector.isNull(i)) {
        int minReaderVersion =
            ((IntVector) protocolVector.getChild(PROTOCOL_MIN_READER_VERSION)).get(i);
        if (minReaderVersion > minReaderVersionSupported) {
          throw new IOException(
              String.format(
                  FORMAT_NOT_SUPPORTED_VERSION, minReaderVersion, minReaderVersionSupported));
        }
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
        partitionCols =
            partitionColsVector.getObject(i).stream()
                .map(Object::toString)
                .collect(Collectors.toList());
        logger.debug("Partition cols are {}", partitionCols);
      }

      if (!configurationVector.isNull(i)) {
        configuration =
            configurationVector.getObject(i).stream()
                .map(e -> (JsonStringHashMap<String, Object>) e)
                .collect(
                    Collectors.toMap(
                        e -> e.get(MapVector.KEY_NAME).toString(),
                        e -> e.get(MapVector.VALUE_NAME).toString()));
        logger.debug("Configuration is {}", configuration);
      }
    }
    return schemaFound;
  }

  private void populateAddedFiles(List<BlockMetaData> blockMetaDataList) throws IOException {
    for (BlockMetaData blockMetaData : blockMetaDataList) {
      long numRows = blockMetaData.getRowCount();
      try {
        ColumnChunkMetaData addColChunk =
            blockMetaData.getColumns().stream()
                .filter(
                    colChunk -> colChunk.getPath().equals(ColumnPath.get(DELTA_FIELD_ADD, "path")))
                .findFirst()
                .orElse(null);
        if (addColChunk == null) {
          continue;
        }
        long numFilesAdded = numRows - addColChunk.getStatistics().getNumNulls();
        netFilesAdded += numFilesAdded;
      } catch (NoSuchElementException e) {
        logger.error("Path for added or removed files does not exist in add/remove columns");
        throw new IOException("Error occurred while reading deltalake table", e);
      }
    }
  }

  private int getStats(
      OperatorContext operatorContext,
      FileSystem fs,
      Path rootFolder,
      int recordCount,
      boolean isFullRowCount)
      throws Exception {
    int numFilesRead = 0;
    BigIntVector sizeVector = (BigIntVector) addVector.getChild(DELTA_FIELD_ADD_SIZE);
    StructVector statsStruct = (StructVector) addVector.getChild(DELTA_FIELD_ADD_STATS_PARSED);
    VarCharVector statsJson = (VarCharVector) addVector.getChild(SCHEMA_STATS);

    VarCharVector pathVector = (VarCharVector) addVector.getChild(DELTA_FIELD_ADD_PATH);

    for (int i = 0; i < recordCount; ++i) {
      if (!addVector.isNull(i)) {
        numFilesRead++;
        netBytesAdded += sizeVector.get(i);
        boolean statsNotFound = true;

        // read stats_parsed first, which is the case that delta.checkpoint.writeStatsAsStruct is
        // set to true
        if (statsStruct != null) {
          BigIntVector numRecordsVector =
              (BigIntVector) (statsStruct.getChild(STATS_PARSED_NUM_RECORDS));
          if ((numRecordsVector != null) && !numRecordsVector.isNull(i)) {
            netRecordsAdded += numRecordsVector.get(i);
            statsNotFound = false;
          }
        } else if (statsJson != null && !statsJson.isNull(i)) { // read stats json
          if (isFullRowCount) {
            String statsJsonString = new String(statsJson.get(i));
            Long numRecords = parseStatsFromJson(statsJsonString);
            statsNotFound = numRecords == null;
            if (!statsNotFound) {
              netRecordsAdded += numRecords;
            }
          } else {
            JsonNode statsJsonNode = OBJECT_MAPPER.readTree(statsJson.get(i));
            JsonNode numRecordsNode = statsJsonNode.get(STATS_PARSED_NUM_RECORDS);
            statsNotFound = numRecordsNode == null;
            if (!statsNotFound) {
              netRecordsAdded += numRecordsNode.asInt();
            }
          }
        }

        if (statsNotFound) {
          long fileSize = sizeVector.get(i);
          if (isFullRowCount) {
            // for full row count, if there is no stats, we keep track of the file size added,
            // then calculate the rowcount after finding the schema
            noStatsFileSize += fileSize;
          } else {
            if (rootFolder != null) {
              // numRecords is not available
              Path fullFilePath =
                  rootFolder.resolve(new String(pathVector.get(i), StandardCharsets.UTF_8));
              fullFilePath =
                  Path.of(URLDecoder.decode(fullFilePath.toString(), StandardCharsets.UTF_8));

              long numRecords;
              try {
                numRecords = estimateRecordsAdded(operatorContext, fs, fullFilePath, fileSize);
                logger.debug(
                    "Num records not available in {}, fileSize {}, estimated row count {}",
                    pathVector.getObject(i),
                    fileSize,
                    numRecords);
              } catch (FileNotFoundException fnfe) {
                // Should never happen in production. If this happens in production, this means that
                // the metadata is inconsistent
                // The query will anyway fail at execution time
                numRecords = NUM_ROWS_IN_DATA_FILE;
                logger.debug(
                    "Data file {} not found, estimated row count {}",
                    pathVector.getObject(i),
                    numRecords);
              }
              netRecordsAdded += numRecords;
            }
          }
        }
      }
    }

    return numFilesRead;
  }

  private long estimateRecordsAdded(
      OperatorContext operatorContext, FileSystem fs, Path filePath, long fileSize)
      throws Exception {
    if (fileSize < SMALL_PARQUET_FILE_SIZE) {
      if (rowSizeEstimateForSmallFile > 0L) {
        double estimate = (fileSize * 1.0d) / rowSizeEstimateForSmallFile;
        estimate *= estimationFactor;
        return Math.round(estimate);
      } else {
        logger.debug("Finding rowSizeEstimate for small files using {}", filePath);
        long totalRecordCount =
            readCheckpointParquetFooter(fs, operatorContext, filePath, fileSize, 0)
                .getBlocks()
                .stream()
                .mapToLong(x -> x.getRowCount())
                .sum();

        if (totalRecordCount > 0) {
          rowSizeEstimateForSmallFile = Math.round((fileSize * 1.0d) / totalRecordCount);
          logger.debug(
              "Number of records={}, fileSize={}, estimatedRowSize={}",
              totalRecordCount,
              fileSize,
              rowSizeEstimateForSmallFile);
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
      long totalRecordCount =
          readCheckpointParquetFooter(fs, operatorContext, filePath, fileSize, 0)
              .getBlocks()
              .stream()
              .mapToLong(x -> x.getRowCount())
              .sum();

      if (totalRecordCount > 0) {
        rowSizeEstimateForLargeFile = Math.round((fileSize * 1.0d) / totalRecordCount);
        logger.debug(
            "Number of records={}, fileSize={}, estimatedRowSize={}",
            totalRecordCount,
            fileSize,
            rowSizeEstimateForLargeFile);
      }
      return totalRecordCount;
    }
  }

  private List<DatasetSplit> generateSplits(
      FileAttributes fileAttributes, long version, MutableParquetMetadata currentMetadata) {
    FileProtobuf.FileSystemCachedEntity fileProto =
        FileProtobuf.FileSystemCachedEntity.newBuilder()
            .setPath(fileAttributes.getPath().toString())
            .setLength(fileAttributes.size())
            .setLastModificationTime(
                fileAttributes
                    .lastModifiedTime()
                    .toMillis()) // makes everything go mutable way to handle the case that
            // checkpoint file get modified
            .build();

    int rowGrpIdx = 0;

    final List<DatasetSplit> datasetSplits = new ArrayList<>(currentMetadata.getBlocks().size());
    for (BlockMetaData blockMetaData : currentMetadata.getBlocks()) {
      final DeltaLakeProtobuf.DeltaCommitLogSplitXAttr deltaExtended =
          DeltaLakeProtobuf.DeltaCommitLogSplitXAttr.newBuilder()
              .setRowGroupIndex(rowGrpIdx)
              .setVersion(version)
              .build();
      rowGrpIdx++;

      final EasyProtobuf.EasyDatasetSplitXAttr splitExtended =
          EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
              .setPath(fileAttributes.getPath().toString())
              .setStart(blockMetaData.getStartingPos())
              .setLength(blockMetaData.getCompressedSize())
              .setUpdateKey(fileProto)
              .setExtendedProperty(deltaExtended.toByteString())
              .build();
      datasetSplits.add(
          DatasetSplit.of(
              Collections.emptyList(),
              blockMetaData.getCompressedSize(),
              blockMetaData.getRowCount(),
              splitExtended::writeTo));
    }
    return datasetSplits;
  }

  private static boolean isRecordEstimateConverged(
      long prevRecordCntEstimate, long newRecordCntEstimate) {
    long diff = Math.abs(newRecordCntEstimate - prevRecordCntEstimate);
    int deltaPercentage = 30;
    return diff * 100 * 1.0 / prevRecordCntEstimate < deltaPercentage;
  }
}
