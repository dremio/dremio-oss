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

import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES_PARSED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.easy.EasyScanOperatorCreator;
import com.dremio.exec.store.dfs.easy.EasySubScan;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.parquet.GlobalDictionaries;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.InputStreamProviderFactory;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.ParquetDictionaryConvertor;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.exec.store.parquet.ParquetScanProjectedColumns;
import com.dremio.exec.store.parquet.ParquetSubScan;
import com.dremio.exec.store.parquet.ParquetTypeHelper;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.exec.store.parquet.SingleStreamProvider;
import com.dremio.exec.store.parquet.UnifiedParquetReader;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Helps in creating ParquetRecordReader from DeltaLakeFormat specific EasyScan splits.
 */
@NotThreadSafe
public class DeltaCheckpointParquetSplitReaderCreator {
    private static final Logger logger = LoggerFactory.getLogger(DeltaCheckpointParquetSplitReaderCreator.class);
    private final OperatorContext opCtx;
    private final EasySubScan easyConfig;
    private final boolean isArrowCachingEnabled;
    private final InputStreamProviderFactory inputStreamProviderFactory;
    private final FileSystem fs;
    private MutableParquetMetadata lastFooter;
    private String lastPath;
    private final long maxFooterLen;

    public DeltaCheckpointParquetSplitReaderCreator(FileSystem fs, OperatorContext opCtx, EasySubScan easySubScanConfig) {
        this.opCtx = opCtx;
        this.easyConfig = easySubScanConfig;
        this.isArrowCachingEnabled = opCtx.getOptions().getOption(ExecConstants.ENABLE_BOOSTING);
        this.inputStreamProviderFactory = opCtx.getConfig().getInstance(InputStreamProviderFactory.KEY,
                InputStreamProviderFactory.class, InputStreamProviderFactory.DEFAULT);
        this.maxFooterLen = opCtx.getOptions().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR);
        this.fs = fs;
    }

    public RecordReader getParquetRecordReader(EasyScanOperatorCreator.SplitAndExtended input, boolean addWithPartitionCols) throws ExecutionSetupException{
        final EasyProtobuf.EasyDatasetSplitXAttr easyXAttr = input.getExtended();

        if (!easyXAttr.getPath().endsWith("parquet")) {
            throw new ExecutionSetupException("Invalid split file type. Expected json | parquet. Path - " + easyXAttr.getPath());
        }

        // need not read partitions related fields in add field for an unpartitioned table
        List<SchemaPath> fieldsToRead = addWithPartitionCols ? easyConfig.getColumns() : removePartitionColumns(easyConfig.getColumns());

        try (AutoCloseables.RollbackCloseable rollbackCloseable = new AutoCloseables.RollbackCloseable()) {
            final ParquetProtobuf.ParquetDatasetSplitScanXAttr parquetXAttr = toParquetXAttr(easyXAttr);
          final ParquetScanProjectedColumns projectedCols = ParquetScanProjectedColumns.fromSchemaPaths(fieldsToRead);
          final List<List<String>> referencedTables = ImmutableList.of(easyConfig.getTableSchemaPath());
          final List<String> dataset = referencedTables.isEmpty() ? null : referencedTables.iterator().next();

          if (!parquetXAttr.getPath().equals(lastPath)) {
            lastFooter = null;
          }

          final InputStreamProvider inputStreamProvider = inputStreamProviderFactory.create(
                    fs,
                    opCtx,
                    Path.of(parquetXAttr.getPath()),
                    parquetXAttr.getFileLength(),
                    parquetXAttr.getLength(),
                    projectedCols,
                    lastFooter,
                    null,
                    f -> parquetXAttr.getRowGroupIndex(),
                    false,
                    dataset,
                    parquetXAttr.getLastModificationTime(),
                    isArrowCachingEnabled,
                    false);
          rollbackCloseable.add(inputStreamProvider);
          lastFooter = inputStreamProvider.getFooter();
          lastPath = inputStreamProvider.getStreamPath().toString();

          final ParquetReaderFactory readerFactory = UnifiedParquetReader.getReaderFactory(opCtx.getConfig());
          final MutableParquetMetadata footer = inputStreamProvider.getFooter();

          // Taking default options: Only possible date values are in the stats.
          final boolean readInt96AsTimeStamp = opCtx.getOptions().getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR);
          final boolean autoCorrectCorruptDates = opCtx.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR);

          final SchemaDerivationHelper.Builder schemaHelperBuilder = SchemaDerivationHelper.builder()
                    .readInt96AsTimeStamp(readInt96AsTimeStamp)
                    .dateCorruptionStatus(ParquetReaderUtility.detectCorruptDates(footer, projectedCols.getBatchSchemaProjectedColumns(),
                            autoCorrectCorruptDates));

          final ParquetSubScan parquetSubScanConfig = toParquetScanConfig(addWithPartitionCols, referencedTables,
            fieldsToRead, footer, parquetXAttr.getPath(), SchemaDerivationHelper.builder().build());

          schemaHelperBuilder.noSchemaLearning(parquetSubScanConfig.getFullSchema());
            SchemaDerivationHelper schemaHelper = schemaHelperBuilder.build();

            final GlobalDictionaries globalDictionaries = GlobalDictionaries.create(opCtx, fs, parquetSubScanConfig.getGlobalDictionaryEncodedColumns());
            final UnifiedParquetReader unifiedParquetReader = new UnifiedParquetReader(
                    opCtx,
                    readerFactory,
                    parquetSubScanConfig.getFullSchema(),
                    projectedCols,
                    Maps.newHashMap(),
                    Collections.EMPTY_LIST,//TODO pushdown add column value not null as a condition
                    readerFactory.newFilterCreator(opCtx, null, null, opCtx.getAllocator()),
                    ParquetDictionaryConvertor.DEFAULT,
                    parquetXAttr,
                    fs,
                    footer,
                    globalDictionaries,
                    schemaHelper,
                    true,
                    false,
                    true,
                    inputStreamProvider,
                    Collections.EMPTY_LIST
            );
            unifiedParquetReader.setIgnoreSchemaLearning(true);
            final CompositeReaderConfig readerConfig = CompositeReaderConfig.getCompound(opCtx, parquetSubScanConfig.getFullSchema(),
                    parquetSubScanConfig.getColumns(), parquetSubScanConfig.getPartitionColumns());
            final SplitAndPartitionInfo parquetSplit = toParquetSplit(input.getSplit(), parquetXAttr);
            RecordReader parquetReader = readerConfig.wrapIfNecessary(opCtx.getAllocator(), unifiedParquetReader, parquetSplit);
            if (addWithPartitionCols) {
                // Fetch list of partition columns from add.partitionValues_parsed
                final List<Field> partitionCols = parquetSubScanConfig.getFullSchema().findField(DELTA_FIELD_ADD).getChildren().stream()
                  .filter(field -> field.getName().equals(SCHEMA_PARTITION_VALUES_PARSED))
                  .flatMap(fields -> fields.getChildren().stream())
                  .collect(Collectors.toList());

                parquetReader = new DeltaLogCheckpointParquetRecordReader(opCtx, parquetReader, partitionCols, parquetSubScanConfig);
            }
            rollbackCloseable.commit();
            return parquetReader;
        } catch (Exception e) {
            throw new ExecutionSetupException(e);
        }
    }

  private List<SchemaPath> removePartitionColumns(List<SchemaPath> columns) {
    return columns.stream()
      .filter(s -> !s.equals(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES)) &&
        !s.equals(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES_PARSED)))
      .collect(Collectors.toList());
  }

  private ParquetSubScan toParquetScanConfig(boolean addWithPartitionCols, List<List<String>> referencedTables,
                                             List<SchemaPath> fieldsToRead, MutableParquetMetadata footer, String checkpointParquetPath, SchemaDerivationHelper schemaDerivationHelper) {
        // Checkpoint parquet should be scanned as a regular PARQUET instead of DELTA type
        final FileConfig formatSettings = ProtostuffUtil.copy(easyConfig.getFileConfig());
        formatSettings.setType(FileType.PARQUET);

        BatchSchema fullSchema = easyConfig.getFullSchema().clone();
        if (addWithPartitionCols) {
          // partitionValues field has different structure in json and checkpoint parquet, below we
          // modify partitionValues field to match type in checkpoint file
            Field addField = fullSchema.findField(DELTA_FIELD_ADD);
            Field addFieldCopy = new Field(addField.getName(), addField.getFieldType(), addField.getChildren());
            List<Field> children = addFieldCopy.getChildren();
            children.removeIf(f -> f.getName().equals(SCHEMA_PARTITION_VALUES)); // struct

            // get add.partitionValues field from parquet schema
          Type partitionValuesParquetField = footer.getFileMetaData()
            .getSchema()
            .getType(DELTA_FIELD_ADD)
            .asGroupType()
            .getType(SCHEMA_PARTITION_VALUES);
          if (partitionValuesParquetField == null) {
            throw UserException.invalidMetadataError().message("Checkpoint file [%s] does not have partitionValues field", checkpointParquetPath).build(logger);
          }

          Optional<Field> partitionValuesField = ParquetTypeHelper.toField(partitionValuesParquetField, schemaDerivationHelper);
          children.add(partitionValuesField.orElseThrow(
            () -> UserException.invalidMetadataError().message("Error while decoding partitionValues field in checkpoint file [%s]", checkpointParquetPath).build(logger)));

            List<Field> newFields = new ArrayList<>(fullSchema.getFields());
            newFields.removeIf(f -> f.getName().equals(DELTA_FIELD_ADD));
            newFields.add(addFieldCopy);

            fullSchema = new BatchSchema(newFields);
        }

    return new ParquetSubScan(
            easyConfig.getProps(),
            formatSettings,
            Collections.emptyList(), // initialise with no splits to avoid redundant footer read.
            fullSchema,
            referencedTables,
            Collections.emptyList(),
            easyConfig.getPluginId(),
            fieldsToRead,
            easyConfig.getPartitionColumns(),
            Collections.emptyList(),
            easyConfig.getExtendedProperty(),
            isArrowCachingEnabled);
    }

    private SplitAndPartitionInfo toParquetSplit(final SplitAndPartitionInfo split, ParquetProtobuf.ParquetDatasetSplitScanXAttr parquetXAttr) {
        final PartitionProtobuf.NormalizedDatasetSplitInfo splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo
                .newBuilder(split.getDatasetSplitInfo())
                .setExtendedProperty(parquetXAttr.toByteString())
                .build();
        return new SplitAndPartitionInfo(split.getPartitionInfo(), splitInfo);
    }

    private ParquetProtobuf.ParquetDatasetSplitScanXAttr toParquetXAttr(EasyProtobuf.EasyDatasetSplitXAttr splitAttributes) throws ExecutionSetupException {
        try {
            if (splitAttributes.getExtendedProperty().isEmpty()) {
                logger.info("Split {}:{} doesn't contain rowgroup information. Scanning the footer to identify this rowgroup.",
                        splitAttributes.getPath(), splitAttributes.getStart());
                return toParquetXAttrFromNoRowgroupSplit(splitAttributes);
            }

            final int rowGroupId = DeltaLakeProtobuf.DeltaCommitLogSplitXAttr.parseFrom(splitAttributes.getExtendedProperty())
                    .getRowGroupIndex();
            final ParquetProtobuf.ParquetDatasetSplitScanXAttr.Builder parquetXAttr = ParquetProtobuf.ParquetDatasetSplitScanXAttr.newBuilder()
                    .setPath(splitAttributes.getPath())
                    .setLength(splitAttributes.getLength())
                    .setStart(splitAttributes.getStart())
                    .setFileLength(splitAttributes.getUpdateKey().getLength())
                    .setRowGroupIndex(rowGroupId)
                    .setLastModificationTime(splitAttributes.getUpdateKey().getLastModificationTime());
            return parquetXAttr.build();
        } catch (IOException e) {
            throw new ExecutionSetupException(e);
        }
    }

    /**
     * Applies only in the upgrade scenario. The splits before 16.0 don't contain the rowgroup information.
     * This method serves as a fallback mechanism.
     * @param splitAttributes
     * @return
     */
    private ParquetProtobuf.ParquetDatasetSplitScanXAttr toParquetXAttrFromNoRowgroupSplit(EasyProtobuf.EasyDatasetSplitXAttr splitAttributes)
            throws IOException {
        if (lastFooter == null) {
            lastFooter = readFooter(splitAttributes.getPath(), splitAttributes.getUpdateKey().getLength());
        }
        int blockIdx = 0;
        for (BlockMetaData block : lastFooter.getBlocks()) {
            if (block.getStartingPos() <= splitAttributes.getStart() &&
                    (block.getStartingPos() + block.getCompressedSize()) > splitAttributes.getStart()) {
                final ParquetProtobuf.ParquetDatasetSplitScanXAttr.Builder parquetXAttr = ParquetProtobuf.ParquetDatasetSplitScanXAttr.newBuilder()
                        .setPath(splitAttributes.getPath())
                        .setLength(block.getCompressedSize())
                        .setStart(splitAttributes.getStart())
                        .setFileLength(splitAttributes.getUpdateKey().getLength())
                        .setRowGroupIndex(blockIdx)
                        .setLastModificationTime(splitAttributes.getUpdateKey().getLastModificationTime());
                return parquetXAttr.build();
            }
            blockIdx++;
        }

        // Block not found for a given starting position. Accumulate debug info and throw error.
        final String blockPositions = lastFooter.getBlocks().stream()
                .map(b -> String.format("[%d,%d]", b.getStartingPos(), (b.getStartingPos() + b.getCompressedSize())))
                .collect(Collectors.joining(","));
        logger.error("Not able to find rowgroup index on the split - [file {}, starting pos {}, blocks {}]",
                splitAttributes.getPath(), splitAttributes.getStart(), blockPositions);
        throw new IllegalStateException(String.format("Not able to find rowgroup index on the split - [file %s, starting pos %d]",
                splitAttributes.getPath(), splitAttributes.getStart()));
    }

    private MutableParquetMetadata readFooter(String filePath, long fileSize) throws IOException {
        try (SingleStreamProvider singleStreamProvider = new SingleStreamProvider(fs, Path.of(filePath), fileSize,
                maxFooterLen, false, null, null, false)) {
            final MutableParquetMetadata footer = singleStreamProvider.getFooter();
            return footer;
        }
    }
}
