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
package com.dremio.exec.store.parquet.copyinto;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties.PropertyKey;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties.Property;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder.Mode;
import com.dremio.exec.store.metadatarefresh.footerread.ParquetFooterReader;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.ParquetFilters;
import com.dremio.exec.store.parquet.ParquetSplitReaderCreatorIterator;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitScanXAttr;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.tuple.Pair;

public class CopyIntoTransformationParquetSplitReaderCreatorIterator
    extends ParquetSplitReaderCreatorIterator {

  protected final boolean isCopyIntoTransformations;
  protected CopyIntoTransformationProperties copyIntoTransformationProperties;
  private TableFunctionContext tableFunctionContext;
  protected BatchSchema targetSchema;
  protected final CopyIntoExtendedProperties copyIntoExtendedProperties;

  public CopyIntoTransformationParquetSplitReaderCreatorIterator(
      FragmentExecutionContext fragmentExecContext,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig config,
      boolean fromRowGroupBasedSplit,
      boolean produceFromBufferedSplits)
      throws ExecutionSetupException {
    super(
        fragmentExecContext,
        context,
        props,
        config,
        fromRowGroupBasedSplit,
        produceFromBufferedSplits);
    Optional<CopyIntoExtendedProperties> copyIntoExtendedPropertiesOptional =
        CopyIntoExtendedProperties.Util.getProperties(extendedProperties);
    if (copyIntoExtendedPropertiesOptional.isEmpty()) {
      throw new RuntimeException(
          "CopyIntoParquetSplitReaderCreatorIterator requires CopyIntoExtendedProperties");
    }
    copyIntoExtendedProperties = copyIntoExtendedPropertiesOptional.get();
    copyIntoTransformationProperties =
        copyIntoExtendedPropertiesOptional
            .get()
            .getProperty(
                PropertyKey.COPY_INTO_TRANSFORMATION_PROPERTIES,
                CopyIntoTransformationProperties.class);
    isCopyIntoTransformations = copyIntoTransformationProperties != null;
    if (isCopyIntoTransformations) {
      tableFunctionContext = config.getFunctionContext();
    }
  }

  @Override
  protected SplitReaderCreator constructReaderCreator(
      ParquetFilters filtersForCurrentRowGroup, ParquetDatasetSplitScanXAttr splitScanXAttr) {
    if (isCopyIntoTransformations) {
      return new CopyIntoTransformationParquetSplitReaderCreator(
          autoCorrectCorruptDates,
          context,
          enableDetailedTracing,
          fs,
          numSplitsToPrefetch,
          prefetchReader,
          readInt96AsTimeStamp,
          readerConfig,
          readerFactory,
          realFields,
          supportsColocatedReads,
          trimRowGroups,
          vectorize,
          currentSplitInfo,
          tablePath,
          filtersForCurrentRowGroup,
          columns,
          fullSchema,
          formatSettings,
          icebergSchemaFields,
          icebergDefaultNameMapping,
          pathToRowGroupsMap,
          this,
          splitScanXAttr,
          this.isIgnoreSchemaLearning(),
          isConvertedIcebergDataset,
          userDefinedSchemaSettings,
          extendedProperties,
          copyIntoTransformationProperties,
          targetSchema);
    } else {
      return super.constructReaderCreator(filtersForCurrentRowGroup, splitScanXAttr);
    }
  }

  @Override
  public InputStreamProvider createInputStreamProvider(
      InputStreamProvider lastInputStreamProvider,
      MutableParquetMetadata lastFooter,
      Path path,
      SplitAndPartitionInfo datasetSplit,
      Function<MutableParquetMetadata, Integer> rowGroupIndexProvider,
      long length,
      long mTime,
      String originalDataFilePath)
      throws IOException {
    if (isCopyIntoTransformations) {
      if (targetSchema == null) {
        targetSchema = fullSchema;
      }
      Pair<MutableParquetMetadata, BatchSchema> footerMetadata =
          getFooterSchema(lastFooter, originalDataFilePath, length);
      lastFooter = footerMetadata.getKey();
      fullSchema =
          getTransformationSchema(originalDataFilePath, footerMetadata.getValue(), targetSchema);
      columns = SchemaUtilities.allColPaths(fullSchema);
      realFields =
          new ImplicitFilesystemColumnFinder(
                  context.getOptions(), fs, columns, isAccelerator, Mode.ALL_IMPLICIT_COLUMNS)
              .getRealFields();
      readerConfig =
          CompositeReaderConfig.getCompound(
              context, fullSchema, columns, tableFunctionContext.getPartitionColumns());
      return super.createInputStreamProvider(
          lastInputStreamProvider,
          lastFooter,
          path,
          datasetSplit,
          rowGroupIndexProvider,
          length,
          mTime,
          originalDataFilePath);
    } else {
      return super.createInputStreamProvider(
          lastInputStreamProvider,
          lastFooter,
          path,
          datasetSplit,
          rowGroupIndexProvider,
          length,
          mTime,
          originalDataFilePath);
    }
  }

  /**
   * Retrieves the footer schema from the given Parquet metadata or by reading the footer from the
   * file system.
   *
   * @param lastParquetMetadata The mutable Parquet metadata. If not null, it is used to create the
   *     batch schema.
   * @param path The path of the Parquet file.
   * @param size The size of the Parquet file.
   * @return A pair consisting of the mutable Parquet metadata and the batch schema.
   * @throws IOException If an I/O error occurs while reading the Parquet footer.
   */
  private Pair<MutableParquetMetadata, BatchSchema> getFooterSchema(
      MutableParquetMetadata lastParquetMetadata, String path, long size) throws IOException {
    if (lastParquetMetadata != null) {
      ParquetFooterReader footerReader = new ParquetFooterReader(context, null, fs, 0, 0, false);
      return Pair.of(
          lastParquetMetadata,
          footerReader.createBatchSchemaIfNeeded(lastParquetMetadata, path, size));
    } else {
      ParquetFooterReader footerReader = new ParquetFooterReader(context, null, fs, 0, 0, true);
      MutableParquetMetadata mutableParquetMetadata = footerReader.readParquetMetadata(path, size);
      return Pair.of(
          mutableParquetMetadata,
          footerReader.createBatchSchemaIfNeeded(mutableParquetMetadata, path, size));
    }
  }

  /**
   * Generates the transformation schema by filtering out the columns from the footer schema that
   * were not used in transformations. The transformation schema is derived from the transformation
   * properties and contains column names with virtual column prefixes.
   *
   * @param path The path of the source file.
   * @param footerSchema The schema of the footer from which unused columns will be removed.
   * @param targetSchema The schema of the target table.
   * @return The transformation schema containing only the columns used in transformations.
   * @throws UserException If the transformation schema has no fields after filtering.
   */
  private BatchSchema getTransformationSchema(
      String path, BatchSchema footerSchema, BatchSchema targetSchema) {
    // remove those columns from the footer schema which were not used in transformations
    // the transformations schema is available in the transformation properties
    // the transformations schema contains column names with virtual colum prefixes
    Set<String> sourceTransformationColNames =
        copyIntoTransformationProperties.getProperties().stream()
            .map(Property::getSourceColNames)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    footerSchema.getFields().stream()
        .filter(field -> sourceTransformationColNames.contains(field.getName().toLowerCase()))
        .forEach(schemaBuilder::addField);

    targetSchema.getFields().stream()
        .filter(field -> field.getName().equalsIgnoreCase(ColumnUtils.COPY_HISTORY_COLUMN_NAME))
        .findFirst()
        .ifPresent(schemaBuilder::addField);

    BatchSchema transformationSchema = schemaBuilder.build();
    if (transformationSchema.getFields().isEmpty()) {
      throw UserException.validationError()
          .message(
              "Copy Into transformation select list [%s] addresses none of the columns from the source file '%s' with schema [%s]",
              String.join(", ", sourceTransformationColNames),
              path,
              footerSchema.getFields().stream()
                  .map(Field::getName)
                  .collect(Collectors.joining(", ")))
          .buildSilently();
    }

    return transformationSchema;
  }
}
