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
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_KEY;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_KEY_VALUE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES_PARSED;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_VALUE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
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

        try (AutoCloseables.RollbackCloseable rollbackCloseable = new AutoCloseables.RollbackCloseable()) {
            final ParquetSubScan parquetSubScanConfig = toParquetScanConfig(addWithPartitionCols);
            final ParquetProtobuf.ParquetDatasetSplitScanXAttr parquetXAttr = toParquetXAttr(easyXAttr);
            final ParquetScanProjectedColumns projectedCols = ParquetScanProjectedColumns.fromSchemaPaths(parquetSubScanConfig.getColumns());
            final Collection<List<String>> referencedTables = parquetSubScanConfig.getTablePath();
            final List<String> dataset = referencedTables==null || referencedTables.isEmpty() ? null:referencedTables.iterator().next();

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
                    parquetSubScanConfig.isArrowCachingEnabled(),
                    false);
            rollbackCloseable.add(inputStreamProvider);
            lastFooter = inputStreamProvider.getFooter();

            final ParquetReaderFactory readerFactory = UnifiedParquetReader.getReaderFactory(opCtx.getConfig());
            final MutableParquetMetadata footer = inputStreamProvider.getFooter();

            // Taking default options: Only possible date values are in the stats.
            final boolean readInt96AsTimeStamp = opCtx.getOptions().getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR);
            final boolean autoCorrectCorruptDates = opCtx.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR);

            final SchemaDerivationHelper.Builder schemaHelperBuilder = SchemaDerivationHelper.builder()
                    .readInt96AsTimeStamp(readInt96AsTimeStamp)
                    .dateCorruptionStatus(ParquetReaderUtility.detectCorruptDates(footer, projectedCols.getBatchSchemaProjectedColumns(),
                            autoCorrectCorruptDates));
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

    private ParquetSubScan toParquetScanConfig(boolean addWithPartitionCols) {
        // Checkpoint parquet should be scanned as a regular PARQUET instead of DELTA type
        final FileConfig formatSettings = ProtostuffUtil.copy(easyConfig.getFileConfig());
        formatSettings.setType(FileType.PARQUET);
        List<SchemaPath> columns = new ArrayList<>(easyConfig.getColumns());
        BatchSchema fullSchema = easyConfig.getFullSchema().clone();
        if (addWithPartitionCols) {
            Field addField = fullSchema.findField(DELTA_FIELD_ADD);
            Field addFieldCopy = new Field(addField.getName(), addField.getFieldType(), addField.getChildren());
            List<Field> children = addFieldCopy.getChildren();
            children.removeIf(f -> f.getName().equals(SCHEMA_PARTITION_VALUES)); // struct

            final Field partitionKey = Field.nullablePrimitive(SCHEMA_KEY, new ArrowType.Utf8());
            final Field partitionVal = Field.nullablePrimitive(SCHEMA_VALUE, new ArrowType.Utf8());
            final Field partitionEntry = new Field("$data$", FieldType.nullable(new ArrowType.Struct()), ImmutableList.of(partitionKey, partitionVal));
            final Field partitionKeyVal = new Field(SCHEMA_KEY_VALUE, FieldType.nullable(new ArrowType.List()), ImmutableList.of(partitionEntry));
            final Field partitionValues = new Field(SCHEMA_PARTITION_VALUES, FieldType.nullable(new ArrowType.Struct()), ImmutableList.of(partitionKeyVal)); // Map type is currently not supported
            children.add(partitionValues); // map

            List<Field> newFields = new ArrayList<>(fullSchema.getFields());
            newFields.removeIf(f -> f.getName().equals(DELTA_FIELD_ADD));
            newFields.add(addFieldCopy);

            fullSchema = new BatchSchema(newFields);

        } else {
            // remove partition fields in projected columns
            columns.removeIf(s -> s.equals(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES)) ||
                    s.equals(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES_PARSED)));
        }

        final ParquetSubScan parquetConfig = new ParquetSubScan(
                easyConfig.getProps(),
                formatSettings,
                Collections.emptyList(), // initialise with no splits to avoid redundant footer read.
                fullSchema,
                ImmutableList.of(easyConfig.getTableSchemaPath()),
                Collections.emptyList(),
                easyConfig.getPluginId(),
                columns,
                easyConfig.getPartitionColumns(),
                Collections.emptyList(),
                easyConfig.getExtendedProperty(),
                isArrowCachingEnabled);
        return parquetConfig;
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
