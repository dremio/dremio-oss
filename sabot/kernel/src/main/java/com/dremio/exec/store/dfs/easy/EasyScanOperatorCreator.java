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
package com.dremio.exec.store.dfs.easy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetSplitXAttr;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Easy scan batch creator from dataset config.
 */
public class EasyScanOperatorCreator implements ProducerOperator.Creator<EasySubScan>{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(com.dremio.exec.store.dfs.easy.EasyScanOperatorCreator.class);

  private final static Comparator<SplitAndExtended> SPLIT_COMPARATOR = new Comparator<SplitAndExtended>() {
    @Override
    public int compare(SplitAndExtended o1e, SplitAndExtended o2e) {
      EasyDatasetSplitXAttr o1 = o1e.getExtended();
      EasyDatasetSplitXAttr o2 = o2e.getExtended();

      // sort by path, and then by start. The most important point is to ensure that the first line of a file is read first,
      // as it may contain a header.
      int cmp = o1.getPath().compareTo(o2.getPath());
      if (cmp != 0) {
        return cmp;
      } else {
        return Long.compare(o1.getStart(), o2.getStart());
      }
    }
  };

  private static class SplitAndExtended {
    private final SplitAndPartitionInfo split;
    private final EasyDatasetSplitXAttr extended;
    public SplitAndExtended(SplitAndPartitionInfo split) {
      super();
      this.split = split;
      try {
        this.extended = LegacyProtobufSerializer.parseFrom(EasyDatasetSplitXAttr.PARSER,
          split.getDatasetSplitInfo().getExtendedProperty());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Could not deserialize split info", e);
      }
    }

    public SplitAndPartitionInfo getSplit() {
      return split;
    }

    public EasyDatasetSplitXAttr getExtended() {
      return extended;
    }
  }

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, final OperatorContext context, EasySubScan config) throws ExecutionSetupException {
    final FileSystemPlugin<?> plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());

    FileSystem fs;
    try {
      fs = plugin.createFS(config.getProps().getUserName(), context);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final FormatPluginConfig formatConfig = PhysicalDatasetUtils.toFormatPlugin(config.getFileConfig(), Collections.<String>emptyList());
    final EasyFormatPlugin<?> formatPlugin = (EasyFormatPlugin<?>) plugin.getFormatPlugin(formatConfig);

    FluentIterable<SplitAndExtended> unorderedWork = FluentIterable.from(config.getSplits())
      .transform(new Function<SplitAndPartitionInfo, SplitAndExtended>() {
        @Override
        public SplitAndExtended apply(SplitAndPartitionInfo split) {
          return new SplitAndExtended(split);
        }
      });

    final boolean sortReaders = context.getOptions().getOption(ExecConstants.SORT_FILE_BLOCKS);
    final List<SplitAndExtended> workList = sortReaders ?  unorderedWork.toSortedList(SPLIT_COMPARATOR) : unorderedWork.toList();
    final boolean selectAllColumns = selectsAllColumns(config.getFullSchema(), config.getColumns());
    final CompositeReaderConfig readerConfig = CompositeReaderConfig.getCompound(context, config.getFullSchema(), config.getColumns(), config.getPartitionColumns());
    final List<SchemaPath> innerFields = selectAllColumns ? ImmutableList.of(ColumnUtils.STAR_COLUMN) : readerConfig.getInnerColumns();

    FluentIterable<RecordReader> readers =
        FluentIterable.from(workList)
            .transform(
                new Function<SplitAndExtended, RecordReader>() {
                  @Override
                  public RecordReader apply(SplitAndExtended input) {
                    try {
                      // If a file source scheme has changed, then trigger a refresh to update the metadata.
                      if (!fs.supportsPath(Path.of(input.getExtended().getPath()))) {
                        throw UserException.invalidMetadataError()
                          .addContext(String.format("%s: Invalid FS for file '%s'", fs.getScheme(), input.getExtended().getPath()))
                          .addContext("File", input.getExtended().getPath())
                          .setAdditionalExceptionContext(
                            new InvalidMetadataErrorContext(
                              ImmutableList.copyOf(config.getReferencedTables())))
                          .build(logger);
                      }

                      RecordReader inner =
                          formatPlugin.getRecordReader(
                              context, fs, input.getExtended(), innerFields);
                      return readerConfig.wrapIfNecessary(
                          context.getAllocator(), inner, input.getSplit());
                    } catch (ExecutionSetupException e) {
                      if (e.getCause() instanceof FileNotFoundException) {
                        throw UserException.invalidMetadataError(e.getCause())
                            .addContext("File not found")
                            .addContext("File", input.getExtended().getPath())
                            .setAdditionalExceptionContext(
                                new InvalidMetadataErrorContext(
                                    ImmutableList.copyOf(config.getReferencedTables())))
                            .build(logger);
                      } else {
                        throw new RuntimeException(e);
                      }
                    }
                  }
                });

    return new ScanOperator(config, context, RecordReaderIterator.from(readers.iterator()));
  }

  /**
   * Checks if all columns (only root paths, and all root paths) in the dataset schema are being selected.
   *
   * @param datasetSchema schema of the dataset
   * @param projectedColumns projected columns
   * @return true iff all columns in the dataset schema are being selected
   */
  static boolean selectsAllColumns(final BatchSchema datasetSchema, final List<SchemaPath> projectedColumns) {
    final Set<String> columnsInTable = FluentIterable.from(datasetSchema)
        .transform(
            new Function<Field, String>() {
              @Override
              public String apply(Field input) {
                return input.getName();
              }})
        .filter(
            new Predicate<String>() {
              @Override
              public boolean apply(String input) {
                return !input.equals(IncrementalUpdateUtils.UPDATE_COLUMN);
              }})
        .toSet();
    final Set<String> selectedColumns = FluentIterable.from(projectedColumns)
        .transform(
            new Function<SchemaPath, String>() {
              @Override
              public String apply(SchemaPath input) {
                return input.getAsUnescapedPath();
              }
            })
        .toSet();
    return columnsInTable.equals(selectedColumns);
  }
}
