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
package com.dremio.exec.store.parquet;

import static com.dremio.exec.ExecConstants.READ_COLUMN_INDEXES;
import static com.dremio.exec.store.parquet.ParquetFormatDatasetAccessor.ACCELERATOR_STORAGEPLUGIN_NAME;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.RuntimeFilterEvaluator;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.EmptySplitReaderCreator;
import com.dremio.exec.store.dfs.PrefetchingIterator;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.deletes.ParquetRowLevelDeleteFileReaderFactory;
import com.dremio.exec.store.iceberg.deletes.RowLevelDeleteFilterFactory;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import io.protostuff.ByteString;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;

/**
 * An object that holds the relevant creation fields so we don't have to have an really long lambda.
 */
public class ParquetSplitReaderCreatorIterator implements SplitReaderCreatorIterator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ParquetSplitReaderCreatorIterator.class);
  protected final ParquetSubScan config;
  private final SupportsIcebergRootPointer plugin;
  protected final FileSystem fs;
  protected final boolean isAccelerator;
  protected final ParquetReaderFactory readerFactory;
  protected List<SchemaPath> realFields;
  protected final boolean vectorize;
  protected final boolean autoCorrectCorruptDates;
  protected final boolean readInt96AsTimeStamp;
  protected final boolean enableDetailedTracing;
  protected final boolean prefetchReader;
  protected final boolean trimRowGroups;
  protected final boolean supportsColocatedReads;
  protected final int numSplitsToPrefetch;
  protected CompositeReaderConfig readerConfig;
  protected final OperatorContext context;
  private final InputStreamProviderFactory factory;
  private final FragmentExecutionContext fragmentExecutionContext;
  protected final List<List<String>> tablePath;
  private final ParquetFilters filters;
  protected List<SchemaPath> columns;
  protected BatchSchema fullSchema;
  private final boolean arrowCachingEnabled;
  protected final boolean isConvertedIcebergDataset;
  protected final FileConfig formatSettings;
  private final RowLevelDeleteFilterFactory rowLevelDeleteFilterFactory;
  protected UserDefinedSchemaSettings userDefinedSchemaSettings;
  private List<SplitAndPartitionInfo> inputSplits;
  private boolean ignoreSchemaLearning = false;
  protected List<IcebergProtobuf.IcebergSchemaField> icebergSchemaFields;

  protected Iterator<ParquetBlockBasedSplit> sortedBlockSplitsIterator;
  protected Iterator<ParquetProtobuf.ParquetDatasetSplitScanXAttr> rowGroupSplitIterator;
  private Iterator<SplitAndPartitionInfo>
      splitAndPartitionInfoIterator; // used only if the main splits are row group based

  protected SplitReaderCreator first;
  protected SplitAndPartitionInfo currentSplitInfo;
  private boolean isFirstRowGroup;
  private InputStreamProvider inputStreamProviderOfFirstRowGroup;
  protected InputStreamProvider lastInputStreamProvider;
  private boolean fromRowGroupBasedSplit;
  private SplitsPathRowGroupsMap splitsPathRowGroupsMap;
  protected Map<String, Set<Integer>> pathToRowGroupsMap = new HashMap<>();
  private final List<RuntimeFilterEvaluator> runtimeFilterEvaluators = new ArrayList<>();
  private final List<RuntimeFilter> partitionColumnRFs = new ArrayList<>();
  private final List<RuntimeFilter> nonPartitionColumnRFs = new ArrayList<>();

  /* this is used for prefetching across record batches in scan table function
   * This is initially set to false, in which case the iterator wont return the final prefetched splitreadercreators
   * After there is no more splits to consume from upstream, this is set to true, in which case the prefetched creators are returned
   */
  private boolean produceFromBufferedSplits;
  protected final ByteString extendedProperties;

  public ParquetSplitReaderCreatorIterator(
      FragmentExecutionContext fragmentExecContext,
      final OperatorContext context,
      final ParquetSubScan config,
      boolean fromRowGroupBasedSplit)
      throws ExecutionSetupException {
    this(
        fragmentExecContext,
        context,
        config,
        CompositeReaderConfig.getCompound(
            context, config.getFullSchema(), config.getColumns(), config.getPartitionColumns()),
        fromRowGroupBasedSplit);
  }

  public ParquetSplitReaderCreatorIterator(
      FragmentExecutionContext fragmentExecContext,
      final OperatorContext context,
      final ParquetSubScan config,
      final CompositeReaderConfig readerConfig,
      boolean fromRowGroupBasedSplit)
      throws ExecutionSetupException {
    this.config = config;
    this.inputSplits = config.getSplits();
    this.tablePath = config.getTablePath();
    this.filters = new ParquetFilters(config.getConditions());
    this.columns = config.getColumns();
    this.fullSchema = config.getFullSchema();
    this.arrowCachingEnabled = config.isArrowCachingEnabled();
    this.isConvertedIcebergDataset =
        false; // Iceberg metadata queries always initialise using TableFunction
    this.formatSettings = config.getFormatSettings();
    this.context = context;
    this.factory =
        context
            .getConfig()
            .getInstance(
                InputStreamProviderFactory.KEY,
                InputStreamProviderFactory.class,
                InputStreamProviderFactory.DEFAULT);
    this.prefetchReader = context.getOptions().getOption(ExecConstants.PREFETCH_READER);
    this.numSplitsToPrefetch =
        (int) context.getOptions().getOption(ExecConstants.NUM_SPLITS_TO_PREFETCH);
    this.trimRowGroups = context.getOptions().getOption(ExecConstants.TRIM_ROWGROUPS_FROM_FOOTER);
    this.plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
    try {
      this.fs = plugin.createFS(null, config.getProps().getUserName(), context);
    } catch (IOException e) {
      throw new ExecutionSetupException("Cannot access plugin filesystem", e);
    }
    this.isAccelerator = config.getPluginId().getName().equals(ACCELERATOR_STORAGEPLUGIN_NAME);
    this.readerFactory = UnifiedParquetReader.getReaderFactory(context.getConfig());

    if (DatasetHelper.isIcebergFile(config.getFormatSettings())) {
      this.realFields = getRealIcebergFields(config.getColumns());
      this.icebergSchemaFields =
          getIcebergColumnIDList(config.getExtendedProperty().asReadOnlyByteBuffer());
    } else {
      this.realFields =
          new ImplicitFilesystemColumnFinder(
                  context.getOptions(),
                  fs,
                  config.getColumns(),
                  isAccelerator,
                  ImplicitFilesystemColumnFinder.Mode.ALL_IMPLICIT_COLUMNS)
              .getRealFields();
    }

    this.vectorize = context.getOptions().getOption(ExecConstants.PARQUET_READER_VECTORIZE);

    this.autoCorrectCorruptDates =
        context.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR)
            && autoCorrectCorruptDatesFromFileFormat(config.getFormatSettings());
    this.readInt96AsTimeStamp =
        context.getOptions().getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR);
    this.enableDetailedTracing =
        context.getOptions().getOption(ExecConstants.ENABLED_PARQUET_TRACING);
    this.supportsColocatedReads = plugin.supportsColocatedReads();
    this.readerConfig = readerConfig;

    this.fragmentExecutionContext = fragmentExecContext;
    this.fromRowGroupBasedSplit = fromRowGroupBasedSplit;
    sortedBlockSplitsIterator = Collections.emptyIterator();
    splitsPathRowGroupsMap = null;
    this.produceFromBufferedSplits =
        true; // in non-v2 case there is no prefetching across batches, so set it to true
    this.rowLevelDeleteFilterFactory = null;
    this.extendedProperties = config.getExtendedProperty();
    processSplits();
    if (prefetchReader) {
      initSplits(null, numSplitsToPrefetch);
    }
  }

  public ParquetSplitReaderCreatorIterator(
      FragmentExecutionContext fragmentExecContext,
      final OperatorContext context,
      OpProps props,
      final TableFunctionConfig config,
      boolean fromRowGroupBasedSplit,
      boolean produceFromBufferedSplits)
      throws ExecutionSetupException {
    this.config = null;
    this.userDefinedSchemaSettings = config.getFunctionContext().getUserDefinedSchemaSettings();
    this.inputSplits = null;
    this.tablePath = config.getFunctionContext().getTablePath();
    ScanFilter scanFilter = config.getFunctionContext().getScanFilter();
    this.filters =
        new ParquetFilters(
            scanFilter != null ? ((ParquetScanFilter) scanFilter).getConditions() : null);
    this.columns = config.getFunctionContext().getColumns();
    this.fullSchema = config.getFunctionContext().getFullSchema();
    this.arrowCachingEnabled =
        config.getFunctionContext().isArrowCachingEnabled()
            || context.getOptions().getOption(ExecConstants.ENABLE_PARQUET_ARROW_CACHING);
    this.isConvertedIcebergDataset = config.getFunctionContext().isConvertedIcebergDataset();
    this.formatSettings = config.getFunctionContext().getFormatSettings();
    this.context = context;
    this.factory =
        context
            .getConfig()
            .getInstance(
                InputStreamProviderFactory.KEY,
                InputStreamProviderFactory.class,
                InputStreamProviderFactory.DEFAULT);
    this.prefetchReader = context.getOptions().getOption(ExecConstants.PREFETCH_READER);
    this.numSplitsToPrefetch =
        (int) context.getOptions().getOption(ExecConstants.NUM_SPLITS_TO_PREFETCH);
    this.trimRowGroups = context.getOptions().getOption(ExecConstants.TRIM_ROWGROUPS_FROM_FOOTER);
    this.plugin = fragmentExecContext.getStoragePlugin(config.getFunctionContext().getPluginId());
    try {
      // hive iceberg tables go through native iceberg path. so, need async options injected
      if (config.getFunctionContext().getInternalTablePluginId() == null) {
        this.fs =
            plugin.createFSWithAsyncOptions(
                config.getFunctionContext().getFormatSettings().getLocation(),
                props.getUserName(),
                context);
      } else {
        // fs native iceberg, or all internal iceberg tables are handled correctly by respective
        // plugins
        this.fs =
            plugin.createFS(
                config.getFunctionContext().getFormatSettings().getLocation(),
                props.getUserName(),
                context);
      }
    } catch (IOException e) {
      throw new ExecutionSetupException("Cannot access plugin filesystem", e);
    }
    this.isAccelerator =
        config.getFunctionContext().getPluginId().getName().equals(ACCELERATOR_STORAGEPLUGIN_NAME);
    this.readerFactory = UnifiedParquetReader.getReaderFactory(context.getConfig());

    if (DatasetHelper.isIcebergFile(config.getFunctionContext().getFormatSettings())) {
      this.realFields = getRealIcebergFields(config.getFunctionContext().getColumns());
    } else {
      this.realFields =
          new ImplicitFilesystemColumnFinder(
                  context.getOptions(),
                  fs,
                  config.getFunctionContext().getColumns(),
                  isAccelerator,
                  ImplicitFilesystemColumnFinder.Mode.ALL_IMPLICIT_COLUMNS)
              .getRealFields();
    }

    this.vectorize = context.getOptions().getOption(ExecConstants.PARQUET_READER_VECTORIZE);

    this.autoCorrectCorruptDates =
        context.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR)
            && autoCorrectCorruptDatesFromFileFormat(
                config.getFunctionContext().getFormatSettings());
    this.readInt96AsTimeStamp =
        context.getOptions().getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR);
    this.enableDetailedTracing =
        context.getOptions().getOption(ExecConstants.ENABLED_PARQUET_TRACING);
    this.supportsColocatedReads = plugin.supportsColocatedReads();
    this.readerConfig =
        CompositeReaderConfig.getCompound(
            context,
            config.getFunctionContext().getFullSchema(),
            config.getFunctionContext().getColumns(),
            config.getFunctionContext().getPartitionColumns());

    this.fragmentExecutionContext = fragmentExecContext;
    this.fromRowGroupBasedSplit = fromRowGroupBasedSplit;
    sortedBlockSplitsIterator = Collections.emptyIterator();
    splitsPathRowGroupsMap = null;
    this.produceFromBufferedSplits =
        produceFromBufferedSplits; // initially set to false, after no more to consume from upstream
    // this is set to true
    this.rowLevelDeleteFilterFactory =
        DatasetHelper.isIcebergFile(config.getFunctionContext().getFormatSettings())
            ? new RowLevelDeleteFilterFactory(
                context,
                new ParquetRowLevelDeleteFileReaderFactory(
                    factory, readerFactory, fs, Iterables.getFirst(tablePath, null), fullSchema))
            : null;
    this.extendedProperties = config.getFunctionContext().getExtendedProperty();
    processSplits();
    if (prefetchReader) {
      initSplits(null, numSplitsToPrefetch);
    }
  }

  private void processSplits() {
    if (inputSplits == null) {
      rowGroupSplitIterator = Collections.emptyIterator();
      sortedBlockSplitsIterator = Collections.emptyIterator();
      return;
    }

    if (fromRowGroupBasedSplit) {
      List<ParquetProtobuf.ParquetDatasetSplitScanXAttr> scanXAttrList = new LinkedList<>();
      List<SplitAndPartitionInfo> splitAndPartitionInfos = new ArrayList<>();
      inputSplits.stream()
          .map(
              s -> {
                try {
                  return Pair.of(
                      s,
                      LegacyProtobufSerializer.parseFrom(
                          ParquetProtobuf.ParquetDatasetSplitScanXAttr.PARSER,
                          s.getDatasetSplitInfo().getExtendedProperty()));
                } catch (InvalidProtocolBufferException e) {
                  throw new RuntimeException(
                      "Could not deserialize parquet dataset split scan attributes", e);
                }
              })
          .sorted(
              (a, b) -> {
                int retVal = a.getRight().getPath().compareTo(b.getRight().getPath());
                if (retVal != 0) {
                  return retVal;
                }

                if (a.getRight().hasStart() && b.getRight().hasStart()) {
                  return Long.compare(a.getRight().getStart(), b.getRight().getStart());
                }

                return Integer.compare(
                    a.getRight().getRowGroupIndex(), b.getRight().getRowGroupIndex());
              })
          .forEach(
              p -> {
                splitAndPartitionInfos.add(p.getLeft());
                scanXAttrList.add(p.getRight());
              });
      splitAndPartitionInfoIterator = splitAndPartitionInfos.iterator();
      rowGroupSplitIterator = new RemovingIterator<>(scanXAttrList.iterator());
      splitsPathRowGroupsMap = null;
      pathToRowGroupsMap = new HashMap<>();
      scanXAttrList.forEach(
          s ->
              pathToRowGroupsMap
                  .computeIfAbsent(s.getPath(), k -> new HashSet<>())
                  .add(s.getRowGroupIndex()));
      currentSplitInfo =
          splitAndPartitionInfoIterator.hasNext() ? splitAndPartitionInfoIterator.next() : null;
    } else {
      rowGroupSplitIterator = Collections.emptyIterator();
      List<ParquetBlockBasedSplit> blockBasedSplits =
          inputSplits.stream()
              .map(ParquetBlockBasedSplit::new)
              .sorted()
              .collect(Collectors.toCollection(LinkedList::new));
      splitsPathRowGroupsMap =
          new SplitsPathRowGroupsMap(blockBasedSplits, realFields.size(), context.getOptions());
      sortedBlockSplitsIterator = new RemovingIterator<>(blockBasedSplits.iterator());
      splitAndPartitionInfoIterator = inputSplits.iterator();
      pathToRowGroupsMap = null;
    }
  }

  // iceberg has no implicit columns.
  private static List<SchemaPath> getRealIcebergFields(List<SchemaPath> columns) {
    Set<SchemaPath> selectedPaths = new LinkedHashSet<>();
    if (columns == null || ColumnUtils.isStarQuery(columns)) {
      selectedPaths.addAll(GroupScan.ALL_COLUMNS);
    } else {
      selectedPaths.addAll(columns);
    }
    return ImmutableList.copyOf(selectedPaths);
  }

  public ScanOperator createScan() throws Exception {
    PrefetchingIterator iterator = new PrefetchingIterator(this);
    try {
      return new ScanOperator(
          fragmentExecutionContext,
          config,
          context,
          iterator,
          fragmentExecutionContext.getForemanEndpoint(),
          fragmentExecutionContext.getQueryContextInformation());
    } catch (Exception ex) {
      AutoCloseables.close(iterator);
      throw ex;
    }
  }

  public RecordReaderIterator getRecordReaderIterator() {
    return new PrefetchingIterator(this);
  }

  public void addSplits(List<SplitAndPartitionInfo> splits) {
    this.inputSplits = splits;
    processSplits();
    if (prefetchReader && first == null) {
      // called either the first time addSplits is called
      // or when all splits were read in the previous batch
      initSplits(null, numSplitsToPrefetch);
    }
  }

  protected void initSplits(SplitReaderCreator curr, int splitsAhead) {
    while (splitsAhead > 0) {
      filterRowGroupSplits();
      while (!rowGroupSplitIterator.hasNext()) {
        if (!sortedBlockSplitsIterator.hasNext()) {
          currentSplitInfo = null;
          return;
        }
        try {
          expandBlockSplit(sortedBlockSplitsIterator.next());
        } catch (FileNotFoundException fnfe) {
          logger.error("One or more of the referred data files are absent.", fnfe);
          throw new RuntimeException(
              String.format(
                  "One or more of the referred data files are absent [%s].", fnfe.getMessage()),
              fnfe);
        } catch (IOException e) {
          throw new RuntimeException("Failed to read row groups from block split", e);
        }
      }

      if (curr == null) {
        first = createSplitReaderCreator();
        curr = first;
        splitsAhead--;
        filterRowGroupSplits();
      }
      while (rowGroupSplitIterator.hasNext() && splitsAhead > 0) {
        SplitReaderCreator creator = createSplitReaderCreator();
        curr.setNext(creator);
        curr = creator;
        splitsAhead--;
        filterRowGroupSplits();
      }
    }
  }

  protected void filterRowGroupSplits() {
    if (fromRowGroupBasedSplit) {
      while (shouldBeFiltered(currentSplitInfo)) {
        ParquetProtobuf.ParquetDatasetSplitScanXAttr splitScanXAttr = rowGroupSplitIterator.next();
        decrementRowGroupCount(splitScanXAttr.getOriginalPath());
        if (!splitAndPartitionInfoIterator.hasNext()) {
          Preconditions.checkArgument(!rowGroupSplitIterator.hasNext());
          currentSplitInfo = null;
          return;
        }
        currentSplitInfo = splitAndPartitionInfoIterator.next();
      }
    }
  }

  private boolean autoCorrectCorruptDatesFromFileFormat(FileConfig fileConfig) {
    boolean autoCorrect;
    if (DatasetHelper.isIcebergFile(fileConfig)) {
      autoCorrect =
          ((IcebergFileConfig) FileFormat.getForFile(fileConfig))
              .getParquetDataFormat()
              .getAutoCorrectCorruptDates();
    } else if (DatasetHelper.isDeltaLake(fileConfig)) {
      autoCorrect = false; // DeltaLake never writes parquet in epoch seconds.
    } else {
      autoCorrect =
          ((ParquetFileConfig) FileFormat.getForFile(fileConfig)).getAutoCorrectCorruptDates();
    }
    return autoCorrect;
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    if (runtimeFilter.getPartitionColumnFilter() != null) {
      final RuntimeFilterEvaluator filterEvaluator =
          new RuntimeFilterEvaluator(
              context.getAllocator(), context.getStats(), context.getOptions(), runtimeFilter);
      this.runtimeFilterEvaluators.add(filterEvaluator);
      this.partitionColumnRFs.add(runtimeFilter);
      logger.debug("Partition Column Runtime filter added to the iterator [{}]", runtimeFilter);
    } else {
      logger.debug("Non-partition Column Runtime filter added to the iterator [{}]", runtimeFilter);
      this.nonPartitionColumnRFs.add(runtimeFilter);
    }
  }

  @Override
  public List<RuntimeFilter> getRuntimeFilters() {
    return partitionColumnRFs;
  }

  @Override
  public void produceFromBufferedSplits(boolean toProduce) {
    this.produceFromBufferedSplits = toProduce;
  }

  @Override
  public boolean hasNext() {
    return rowGroupSplitIterator.hasNext()
        || sortedBlockSplitsIterator.hasNext()
        || (produceFromBufferedSplits && first != null);
  }

  @Override
  public SplitReaderCreator next() {
    Preconditions.checkArgument(hasNext());
    filterIfNecessary();
    if (first == null) {
      Preconditions.checkArgument(
          !rowGroupSplitIterator.hasNext() && !sortedBlockSplitsIterator.hasNext());
      return new EmptySplitReaderCreator(null, lastInputStreamProvider);
    }
    SplitReaderCreator curr = first;
    first = first.getNext();
    return curr;
  }

  protected void filterIfNecessary() {
    if (prefetchReader) {
      SplitReaderCreator prev = null;
      SplitReaderCreator curr = first;
      first = null;

      int numCreators = 0;
      while (curr != null) { // filter the already constructed splitReaderCreators
        if (shouldBeFiltered(curr.getSplit())) {
          decrementRowGroupCount(curr.getSplitXAttr().getOriginalPath());
          try {
            curr.close();
          } catch (Exception e) {
            throw new RuntimeException("Failed to close splitReaderCreator", e);
          }
          curr = curr.getNext();
          continue;
        }
        if (prev == null) {
          first = curr;
        } else {
          prev.setNext(curr);
        }
        prev = curr;
        curr = curr.getNext();
        numCreators++;
      }
      initSplits(prev, numSplitsToPrefetch - numCreators + 1);
    } else {
      first = null;
      initSplits(null, 1);
    }
  }

  protected SplitReaderCreator constructReaderCreator(
      ParquetFilters filtersForCurrentRowGroup,
      ParquetProtobuf.ParquetDatasetSplitScanXAttr splitScanXAttr) {
    return new ParquetSplitReaderCreator(
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
        pathToRowGroupsMap,
        this,
        splitScanXAttr,
        this.isIgnoreSchemaLearning(),
        isConvertedIcebergDataset,
        userDefinedSchemaSettings,
        extendedProperties);
  }

  protected SplitReaderCreator createSplitReaderCreator() {
    ParquetProtobuf.ParquetDatasetSplitScanXAttr splitScanXAttr = rowGroupSplitIterator.next();
    String dataFilePath =
        splitScanXAttr.getOriginalPath().isEmpty()
            ? splitScanXAttr.getPath()
            : splitScanXAttr.getOriginalPath();

    ParquetFilters filtersForCurrentRowGroup =
        rowLevelDeleteFilterFactory != null
            ? filters.withRowLevelDeleteFilters(
                rowLevelDeleteFilterFactory.createPositionalDeleteFilter(dataFilePath),
                rowLevelDeleteFilterFactory.createEqualityDeleteFilter(
                    dataFilePath, icebergSchemaFields))
            : filters;
    SplitReaderCreator creator = constructReaderCreator(filtersForCurrentRowGroup, splitScanXAttr);

    if (!fromRowGroupBasedSplit && isFirstRowGroup) {
      creator.setInputStreamProvider(inputStreamProviderOfFirstRowGroup);
      isFirstRowGroup = false;
    }
    if (fromRowGroupBasedSplit) {
      context.getStats().addLongStat(ScanOperator.Metric.NUM_ROW_GROUPS, 1);
      if (splitAndPartitionInfoIterator.hasNext()) {
        currentSplitInfo = splitAndPartitionInfoIterator.next();
      } else {
        Preconditions.checkArgument(!rowGroupSplitIterator.hasNext());
        currentSplitInfo = null;
      }
    }
    return creator;
  }

  public void setLastInputStreamProvider(InputStreamProvider lastInputStreamProvider) {
    this.lastInputStreamProvider = lastInputStreamProvider;
  }

  public void trimRowGroupsFromFooter(
      final MutableParquetMetadata footer, String path, int rowGroupIndex) {
    if (splitsPathRowGroupsMap != null) {
      final Set<Integer> usedRowGroups =
          splitsPathRowGroupsMap.getPathRowGroups(path, footer, rowGroupIndex);
      if (usedRowGroups != null && trimRowGroups) {
        long numRowGroupsTrimmed = footer.removeUnusedRowGroups(usedRowGroups);
        context
            .getStats()
            .addLongStat(ScanOperator.Metric.NUM_ROW_GROUPS_TRIMMED, numRowGroupsTrimmed);
      }
    }
  }

  protected void expandBlockSplit(ParquetBlockBasedSplit blockSplit) throws IOException {
    if (shouldBeFiltered(blockSplit.getSplitAndPartitionInfo())) {
      decrementRowGroupCount(blockSplit.getPath());
      return;
    }

    Path splitPath = Path.of(blockSplit.getPath());
    if (fs != null && !fs.supportsPathsWithScheme()) {
      splitPath = Path.of(Path.getContainerSpecificRelativePath(splitPath));
    }

    long fileLength, fileLastModificationTime;
    if (blockSplit.hasFileLength() && blockSplit.hasLastModificationTime()) {
      fileLength = blockSplit.getFileLength();
      fileLastModificationTime = blockSplit.getLastModificationTime();
    } else {
      final FileAttributes fileAttributes = fs.getFileAttributes(splitPath);
      fileLength = fileAttributes.size();
      fileLastModificationTime = fileAttributes.lastModifiedTime().toMillis();
    }

    List<Integer> rowGroupNums = new ArrayList<>();
    final Consumer<MutableParquetMetadata> populateRowGroupNums =
        (f) -> {
          try {
            if (rowGroupNums.isEmpty()) { // make sure rowGroupNums is populated only once
              rowGroupNums.addAll(
                  ParquetReaderUtility.getRowGroupNumbersFromFileSplit(
                      blockSplit.getStart(), blockSplit.getLength(), f));
              trimRowGroupsFromFooter(
                  f, blockSplit.getPath(), rowGroupNums.stream().min(Integer::compareTo).orElse(0));
            }
          } catch (IOException e) {
            throw UserException.ioExceptionError(e).buildSilently();
          }
        };

    final Function<MutableParquetMetadata, Integer> rowGroupIndexProvider =
        (f) -> {
          populateRowGroupNums.accept(f);
          if (rowGroupNums.isEmpty()) {
            return -1;
          }
          return rowGroupNums.get(0);
        };

    if (lastInputStreamProvider != null
        && !splitPath.equals(lastInputStreamProvider.getStreamPath())) {
      logger.debug(
          "Block splits are for different files so reusing stream providers is not possible. Setting last input stream provider to null");
      setLastInputStreamProvider(null);
    }

    inputStreamProviderOfFirstRowGroup =
        createInputStreamProvider(
            lastInputStreamProvider,
            lastInputStreamProvider != null ? lastInputStreamProvider.getFooter() : null,
            splitPath,
            blockSplit.getSplitAndPartitionInfo(),
            rowGroupIndexProvider,
            fileLength,
            fileLastModificationTime,
            blockSplit.getPath());

    MutableParquetMetadata footer = safelyGetFooter();
    if (userDefinedSchemaSettings != null
        && userDefinedSchemaSettings.getSchemaImposedOutput()
        && !verifyColumnOverlap(footer)) {
      throw UserException.validationError()
          .message("Parquet file does not contain any of the fields expected in the output")
          .addContext("filename: '%s'", inputStreamProviderOfFirstRowGroup.getStreamPath())
          .buildSilently();
    }

    populateRowGroupNums.accept(footer);
    context.getStats().addLongStat(ScanOperator.Metric.NUM_ROW_GROUPS, rowGroupNums.size());

    // Notify the Iceberg filters factory of any additional row groups so that it can manage the
    // lifetime of associated
    // PositionalDeleteFilters.  Each row group adds a reference count on a filter, which is
    // decremented when the row
    // group reader is closed.  In the case where this split expanded to no row groups, we need to
    // decrement the
    // reference count by 1.
    if (rowLevelDeleteFilterFactory != null) {
      int delta = rowGroupNums.size() - 1;
      if (delta != 0) {
        rowLevelDeleteFilterFactory.adjustRowGroupCount(blockSplit.getPath(), delta);
      }
    }

    List<ParquetProtobuf.ParquetDatasetSplitScanXAttr> rowGroupSplitAttrs =
        createRowGroupSplitList(
            rowGroupNums, splitPath, blockSplit, fileLength, fileLastModificationTime);

    rowGroupSplitIterator = new RemovingIterator<>(rowGroupSplitAttrs.iterator());
    currentSplitInfo = blockSplit.getSplitAndPartitionInfo();
    isFirstRowGroup = true;
  }

  protected List<ParquetProtobuf.ParquetDatasetSplitScanXAttr> createRowGroupSplitList(
      List<Integer> rowGroupNums,
      Path splitPath,
      ParquetBlockBasedSplit blockSplit,
      long fileLength,
      long fileLastModificationTime) {
    List<ParquetProtobuf.ParquetDatasetSplitScanXAttr> rowGroupSplitAttrs = new LinkedList<>();
    for (int rowGroupNum : rowGroupNums) {
      rowGroupSplitAttrs.add(
          ParquetProtobuf.ParquetDatasetSplitScanXAttr.newBuilder()
              .setRowGroupIndex(rowGroupNum)
              .setPath(splitPath.toString())
              .setStart(0L)
              .setLength(blockSplit.getLength()) // max row group size possible
              .setFileLength(fileLength)
              .setLastModificationTime(fileLastModificationTime)
              .setOriginalPath(blockSplit.getPath())
              .build());
    }
    return rowGroupSplitAttrs;
  }

  private boolean verifyColumnOverlap(MutableParquetMetadata footer) {
    Set<String> columnNames =
        columns.stream()
            .map(sp -> sp.getNameSegments().get(0).toLowerCase())
            .collect(Collectors.toSet());
    for (String[] fieldPath : footer.getFileMetaData().getSchema().getPaths()) {
      if (columnNames.contains(fieldPath[0].toLowerCase())) {
        return true;
      }
    }
    return false;
  }

  private MutableParquetMetadata safelyGetFooter() throws IOException {
    try {
      return inputStreamProviderOfFirstRowGroup.getFooter();
    } catch (IOException e) {
      // Close the inputStreamProvider
      try {
        inputStreamProviderOfFirstRowGroup.close();
      } catch (Exception ex) {
        logger.debug("Ignoring the exception on inputStreamProvider close.", ex);
      }
      throw e;
    }
  }

  private boolean shouldBeFiltered(SplitAndPartitionInfo split) {
    if (split == null) {
      return false;
    }
    final List<NameValuePair<?>> nameValuePairs =
        this.readerConfig.getPartitionNVPairs(this.context.getAllocator(), split);
    try {
      for (RuntimeFilterEvaluator runtimeFilterEvaluator : runtimeFilterEvaluators) {
        if (runtimeFilterEvaluator.canBeSkipped(split, nameValuePairs)) {
          return true;
        }
      }
      return false;
    } finally {
      com.dremio.common.AutoCloseables.close(RuntimeException.class, nameValuePairs);
    }
  }

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

    final Path lastPath =
        (lastInputStreamProvider != null) ? lastInputStreamProvider.getStreamPath() : null;
    MutableParquetMetadata validLastFooter = null;
    InputStreamProvider validLastInputStreamProvider = null;
    if (path.equals(lastPath)) {
      validLastFooter = lastFooter;
      validLastInputStreamProvider = lastInputStreamProvider;
    }

    final boolean readFullFile =
        length < context.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_THRESHOLD)
            && ((float) columns.size()) / fullSchema.getFieldCount()
                > context.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_COLUMN_RATIO);

    final Collection<List<String>> referencedTables = tablePath;
    final List<String> dataset =
        referencedTables == null || referencedTables.isEmpty()
            ? null
            : referencedTables.iterator().next();

    Preconditions.checkArgument(
        formatSettings.getType() != FileType.ICEBERG || icebergSchemaFields != null);
    // while we add all columns used in equality delete conditions into projectedColumn when
    // creating the inner reader of the ParquetSplitReader,
    // we need make sure they are added to the InputStreamProvider
    List<Integer> equalityIds =
        rowLevelDeleteFilterFactory != null
            ? rowLevelDeleteFilterFactory.getEqualityIds(originalDataFilePath)
            : Collections.emptyList();
    // if there are equalityIds, we combine realFields and equality fields into projected columns
    List<SchemaPath> projectedColumnFields =
        equalityIds.isEmpty()
            ? realFields
            : Stream.concat(
                    ParquetReaderUtility.getColumnsFromEqualityIds(equalityIds, icebergSchemaFields)
                        .stream(),
                    realFields.stream())
                .collect(Collectors.toList());

    ParquetScanProjectedColumns projectedColumns =
        ParquetScanProjectedColumns.fromSchemaPathAndIcebergSchema(
            projectedColumnFields,
            icebergSchemaFields,
            isConvertedIcebergDataset,
            context,
            fullSchema);

    ParquetReaderFactory.ManagedSchemaType schemaType = null;
    if (!isConvertedIcebergDataset && DatasetHelper.isIcebergFile(formatSettings)) {
      schemaType = ParquetReaderFactory.ManagedSchemaType.ICEBERG;
    } else if (DatasetHelper.isDeltaLake(formatSettings)) {
      schemaType = ParquetReaderFactory.ManagedSchemaType.ICEBERG;
    }
    // If the ExecOption to ReadColumnIndexes is True and the configuration has a Filter, set
    // readColumnIndices to true.
    boolean readColumnIndices =
        context.getOptions().getOption(READ_COLUMN_INDEXES) && filters.hasPushdownFilters();
    return factory.create(
        fs,
        context,
        path,
        length,
        datasetSplit.getPartitionInfo().getSize(),
        projectedColumns,
        validLastFooter,
        validLastInputStreamProvider,
        rowGroupIndexProvider,
        readFullFile,
        dataset,
        mTime,
        arrowCachingEnabled,
        readColumnIndices,
        filters,
        readerFactory.newFilterCreator(context, schemaType, null, context.getAllocator()),
        nonPartitionColumnRFs);
  }

  public void setIcebergExtendedProperty(byte[] extendedProperty) {
    this.icebergSchemaFields = getIcebergColumnIDList(ByteBuffer.wrap(extendedProperty));
    SplitReaderCreator curr = first;
    while (curr != null) {
      curr.setIcebergSchemaFields(icebergSchemaFields);
      curr = curr.getNext();
    }
  }

  private List<IcebergProtobuf.IcebergSchemaField> getIcebergColumnIDList(
      ByteBuffer extendedProperty) {
    if (formatSettings.getType() != FileType.ICEBERG) {
      return null;
    }

    try {
      IcebergProtobuf.IcebergDatasetXAttr icebergDatasetXAttr =
          LegacyProtobufSerializer.parseFrom(
              IcebergProtobuf.IcebergDatasetXAttr.PARSER, extendedProperty);
      return icebergDatasetXAttr.getColumnIdsList();
    } catch (InvalidProtocolBufferException ie) {
      try {
        ParquetProtobuf.ParquetDatasetXAttr parquetDatasetXAttr =
            LegacyProtobufSerializer.parseFrom(
                ParquetProtobuf.ParquetDatasetXAttr.PARSER, extendedProperty);
        // found XAttr from 5.0.1 release. return null
        return null;
      } catch (InvalidProtocolBufferException pe) {
        throw new RuntimeException("Could not deserialize Parquet dataset info", pe);
      }
    }
  }

  private void decrementRowGroupCount(String path) {
    if (rowLevelDeleteFilterFactory != null) {
      rowLevelDeleteFilterFactory.adjustRowGroupCount(path, -1);
    }
  }

  public void setDataFileInfoForBatch(
      Map<String, RowLevelDeleteFilterFactory.DataFileInfo> dataFileInfo) {
    Preconditions.checkNotNull(rowLevelDeleteFilterFactory);
    rowLevelDeleteFilterFactory.setDataFileInfoForBatch(dataFileInfo);
  }

  public boolean isIgnoreSchemaLearning() {
    return ignoreSchemaLearning;
  }

  public void setIgnoreSchemaLearning(boolean ignoreSchemaLearning) {
    this.ignoreSchemaLearning = ignoreSchemaLearning;
  }

  @Override
  public void close() throws Exception {
    List<SplitReaderCreator> remainingCreators = new ArrayList<>();
    SplitReaderCreator curr = first;
    while (curr != null) {
      remainingCreators.add(curr);
      curr = curr.getNext();
    }
    if (remainingCreators.size() == 0) {
      AutoCloseables.close(inputStreamProviderOfFirstRowGroup);
    }
    com.dremio.common.AutoCloseables.close(remainingCreators);
    if (rowLevelDeleteFilterFactory != null) {
      rowLevelDeleteFilterFactory.close();
    }
  }

  private static class RemovingIterator<E> implements Iterator<E> {
    Iterator<E> it;

    public RemovingIterator(Iterator<E> it) {
      this.it = it;
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public E next() {
      E elem = it.next();
      it.remove();
      return elem;
    }
  }
}
