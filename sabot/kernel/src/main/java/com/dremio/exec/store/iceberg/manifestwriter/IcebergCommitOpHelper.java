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
package com.dremio.exec.store.iceberg.manifestwriter;

import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;
import static com.dremio.exec.store.iceberg.IcebergUtils.isIncrementalRefresh;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.expr.fn.impl.ByteArrayWrapper;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.exec.store.dfs.HistoryEventHandler;
import com.dremio.exec.store.dfs.HistoryEventHandlerProvider;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.dfs.NoopHistoryEventHandler;
import com.dremio.exec.store.dfs.copyinto.CopyIntoHistoryEventHandler;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.committer.ReadSignatureProvider;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.exec.util.VectorUtil;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.dremio.sabot.op.writer.WriterCommitterOutputHandler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.util.Text;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class for flushing out commits */
public class IcebergCommitOpHelper implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(IcebergCommitOpHelper.class);
  private static final ControlsInjector injector =
      ControlsInjectorFactory.getInjector(IcebergCommitOpHelper.class);

  @VisibleForTesting
  public static final String INJECTOR_BEFORE_TABLE_COMMIT_ERROR =
      "error-between-history-and-iceberg-table-commit";

  @VisibleForTesting
  public static final String INJECTOR_HISTORY_REVERT_ERROR = "error-during-history-revert";

  @VisibleForTesting
  public static final String INJECTOR_AFTER_TABLE_COMMIT_ERROR = "error-after-iceberg-commit";

  private static final Set<IcebergCommandType> CUSTOM_OUTPUT_COMMANDS =
      ImmutableSet.of(IcebergCommandType.OPTIMIZE);

  protected final WriterCommitterPOP config;
  protected final OperatorContext context;
  protected final FileSystem fs;
  protected VarBinaryVector icebergMetadataVector;
  protected IntVector operationTypeVector;
  // Set of existing data files in the iceberg table referenced by the commit
  protected VarBinaryVector referencedDataFiles;
  protected VarCharVector pathVector;
  protected ListVector partitionDataVector;
  protected IcebergOpCommitter icebergOpCommitter;
  private final Set<IcebergPartitionData> addedPartitions =
      new HashSet<>(); // partitions with new files
  private final Set<IcebergPartitionData> deletedPartitions =
      new HashSet<>(); // partitions with deleted files
  private final Set<String> partitionPaths;
  private final Set<String> partitionPathsWithDeletedFiles = new HashSet<>();
  protected Predicate<String> partitionExistsPredicate = path -> true;
  private FileSystem fsToCheckIfPartitionExists;
  protected ReadSignatureProvider readSigProvider = (added, deleted) -> ByteString.EMPTY;
  protected List<ManifestFile> icebergManifestFiles = new ArrayList<>();
  protected boolean success;
  protected final FileIO fileIO;
  private VarBinaryVector metadataVector;
  private BigIntVector rejectedRecordCountVector;
  private HistoryEventHandler historyEventHandler;

  // orphan files to delete, used for small file compaction.
  private Set<String> orphanFiles = new HashSet<>();
  // number of retries to delete orphan files
  private static final int DELETE_NUM_RETRIES = 3;
  private static final int DEFAULT_THREAD_POOL_SIZE = 4;
  private final ExecutionControls executionControls;

  public static IcebergCommitOpHelper getInstance(
      OperatorContext context, WriterCommitterPOP config, FileSystem fs) {
    if (config.getIcebergTableProps() == null) {
      return new NoOpIcebergCommitOpHelper(context, config, fs);
    } else if (config.getIcebergTableProps().isDetectSchema()) {
      return new SchemaDiscoveryIcebergCommitOpHelper(context, config, fs);
    } else {
      return new IcebergCommitOpHelper(context, config, fs);
    }
  }

  protected IcebergCommitOpHelper(
      OperatorContext context, WriterCommitterPOP config, FileSystem fs) {
    this.config = config;
    this.context = context;
    this.fs = fs;
    Preconditions.checkArgument(
        config.getPlugin() instanceof SupportsIcebergRootPointer,
        "Invalid plugin in IcebergCommitOpHelper - plugin does not support Iceberg");
    String pluginId =
        config.getSourceTablePluginId() != null
            ? config.getSourceTablePluginId().getName()
            : config.getPluginId().getName();
    this.fileIO =
        ((SupportsIcebergRootPointer) config.getPlugin())
            .createIcebergFileIO(
                fs, context, config.getDatasetPath().getPathComponents(), pluginId, null);
    this.partitionPaths = createPartitionPathsSet(config);
    this.historyEventHandler = new NoopHistoryEventHandler();
    this.executionControls = context.getExecutionControls();
  }

  public void setup(VectorAccessible incoming) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    // TODO: doesn't track wait times currently. need to use dremioFileIO after implementing
    // newOutputFile method
    IcebergTableProps icebergTableProps = config.getIcebergTableProps();

    final IcebergModel icebergModel;
    final IcebergTableIdentifier icebergTableIdentifier;
    final SupportsIcebergMutablePlugin icebergMutablePlugin =
        (SupportsIcebergMutablePlugin) config.getPlugin();
    icebergModel =
        icebergMutablePlugin.getIcebergModel(
            icebergTableProps, config.getProps().getUserName(), context, fileIO);
    icebergTableIdentifier =
        icebergModel.getTableIdentifier(icebergMutablePlugin.getTableLocation(icebergTableProps));

    TypedFieldId metadataFileId =
        RecordWriter.SCHEMA.getFieldId(
            SchemaPath.getSimplePath(RecordWriter.ICEBERG_METADATA_COLUMN));
    TypedFieldId operationTypeId =
        RecordWriter.SCHEMA.getFieldId(
            SchemaPath.getSimplePath(RecordWriter.OPERATION_TYPE_COLUMN));
    TypedFieldId pathId =
        RecordWriter.SCHEMA.getFieldId(SchemaPath.getSimplePath(RecordWriter.PATH_COLUMN));
    icebergMetadataVector =
        incoming
            .getValueAccessorById(VarBinaryVector.class, metadataFileId.getFieldIds())
            .getValueVector();
    operationTypeVector =
        incoming
            .getValueAccessorById(IntVector.class, operationTypeId.getFieldIds())
            .getValueVector();
    pathVector =
        incoming.getValueAccessorById(VarCharVector.class, pathId.getFieldIds()).getValueVector();
    partitionDataVector =
        (ListVector)
            VectorUtil.getVectorFromSchemaPath(incoming, RecordWriter.PARTITION_DATA_COLUMN);
    metadataVector =
        (VarBinaryVector)
            VectorUtil.getVectorFromSchemaPath(incoming, RecordWriter.METADATA_COLUMN);
    rejectedRecordCountVector =
        (BigIntVector)
            VectorUtil.getVectorFromSchemaPath(incoming, RecordWriter.REJECTED_RECORDS_COLUMN);
    referencedDataFiles =
        (VarBinaryVector)
            VectorUtil.getVectorFromSchemaPath(incoming, RecordWriter.REFERENCED_DATA_FILES_COLUMN);

    switch (icebergTableProps.getIcebergOpType()) {
      case CREATE:
        icebergOpCommitter =
            icebergModel.getCreateTableCommitter(
                icebergTableProps.getTableName(),
                icebergTableIdentifier,
                icebergTableProps.getFullSchema(),
                icebergTableProps.getPartitionColumnNames(),
                context.getStats(),
                icebergTableProps.getPartitionSpec() != null
                    ? IcebergSerDe.deserializePartitionSpec(
                        deserializedJsonAsSchema(icebergTableProps.getIcebergSchema()),
                        icebergTableProps.getPartitionSpec().toByteArray())
                    : null,
                icebergTableProps.getSortOrder() != null
                    ? IcebergSerDe.deserializeSortOrderFromJson(
                        (deserializedJsonAsSchema(icebergTableProps.getIcebergSchema())),
                        icebergTableProps.getSortOrder())
                    : null,
                icebergTableProps.getTableProperties());
        break;
      case INSERT:
        icebergOpCommitter =
            icebergModel.getInsertTableCommitter(
                icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                context.getStats());
        break;
      case UPDATE:
      case DELETE:
      case MERGE:
        icebergOpCommitter =
            icebergModel.getDmlCommitter(
                context,
                icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                config.getDatasetConfig().get(),
                icebergTableProps.getIcebergOpType(),
                config.getTableFormatOptions().getSnapshotId(),
                DmlUtils.isMergeOnReadDmlOperation(config.getTableFormatOptions())
                    ? RowLevelOperationMode.MERGE_ON_READ
                    : RowLevelOperationMode.COPY_ON_WRITE);
        break;
      case OPTIMIZE:
        icebergOpCommitter =
            icebergModel.getOptimizeCommitter(
                context.getStats(),
                icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                config.getDatasetConfig().get(),
                config.getTableFormatOptions().getMinInputFilesBeforeOptimize(),
                config.getTableFormatOptions().getSnapshotId(),
                icebergTableProps,
                getFS(config));
        break;
      case FULL_METADATA_REFRESH:
        createReadSignProvider(icebergTableProps, true);
        icebergOpCommitter =
            icebergModel.getFullMetadataRefreshCommitter(
                icebergTableProps.getTableName(),
                config.getDatasetPath().getPathComponents(),
                icebergTableProps.getDataTableLocation(),
                icebergTableProps.getUuid(),
                icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                icebergTableProps.getFullSchema(),
                icebergTableProps.getPartitionColumnNames(),
                config.getDatasetConfig().get(),
                context.getStats(),
                null,
                icebergTableProps.getFileType());
        break;
      case PARTIAL_METADATA_REFRESH:
      case INCREMENTAL_METADATA_REFRESH:
        createReadSignProvider(icebergTableProps, false);
        icebergOpCommitter =
            icebergModel.getIncrementalMetadataRefreshCommitter(
                context,
                icebergTableProps.getTableName(),
                config.getDatasetPath().getPathComponents(),
                icebergTableProps.getDataTableLocation(),
                icebergTableProps.getUuid(),
                icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                icebergTableProps.getPersistedFullSchema(),
                icebergTableProps.getPartitionColumnNames(),
                false,
                config.getDatasetConfig().get(),
                getFS(config),
                icebergTableProps.getMetadataExpireAfterMs(),
                icebergTableProps.getIcebergOpType(),
                icebergTableProps.getFileType(),
                config.getTableFormatOptions().getSnapshotId(),
                icebergTableProps.getErrorOnConcurrentRefresh());
        icebergOpCommitter.updateSchema(icebergTableProps.getFullSchema());
        break;
    }
    addMetricStat(
        WriterCommitterOperator.Metric.ICEBERG_COMMIT_SETUP_TIME,
        stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }

  private List<String> getPartitionPaths() {
    return config.getIcebergTableProps().getPartitionPaths();
  }

  public boolean hasCustomOutput() {
    if (config.getIcebergTableProps() == null) {
      return false;
    }
    IcebergCommandType icebergCommandType = config.getIcebergTableProps().getIcebergOpType();
    return icebergCommandType != null && CUSTOM_OUTPUT_COMMANDS.contains(icebergCommandType);
  }

  public void consumeData(int records) throws Exception {
    for (int i = 0; i < records; ++i) {
      OperationType operationType = getOperationType(i);
      switch (operationType) {
        case ADD_MANIFESTFILE:
          consumeManifestFile(getManifestFile(i));
          consumeManifestPartitionData(i);
          break;
        case DELETE_DATAFILE:
          if (isDmlCommandType()) {
            consumeDeleteDataFilePath(i);
          } else {
            consumeDeletedDataFile(getDeleteDataFile(i));
          }
          consumeDeletedDataFilePartitionData(i);
          break;
        case ADD_DATAFILE:
          DataFile addedDataFile =
              IcebergSerDe.deserializeDataFile(
                  getIcebergMetadataInformation(i).getIcebergMetadataFileByte());
          // Consuming operations: (Merge-On-Read) INSERT, UPDATE, MERGE
          if (DmlUtils.isMergeOnReadDmlOperation(config.getTableFormatOptions())) {
            consumeMergeOnReadDataFile(addedDataFile);
            break;
          }
          // Consuming operations: OPTIMIZE TABLE
          icebergOpCommitter.consumeAddDataFile(addedDataFile);
          break;
        case ADD_DELETEFILE:
          DeleteFile deleteFile =
              IcebergSerDe.deserializeDeleteFile(
                  getIcebergMetadataInformation(i).getIcebergMetadataFileByte());

          // compress referenced files to lighten workload
          Set<ByteArrayWrapper> referencedDataFiles = getReferencedDataFiles(i);
          if (referencedDataFiles.isEmpty()) {
            // Since referenced Data-Files can be sent independently of the associated Delete-File,
            // it is presumed OK to consume a Delete-File with zero Data-File references attached.
            // Their references were likely coupled to the previously-written file.
            // ... However, we will keep note of this in the logs for safety.
            logger.warn(
                String.format(
                    "Position Delete-File '%s' was consumed with zero references to Data-Files. "
                        + "If Concurrent Iceberg Commits underwent unexpected behavior, "
                        + "this may be a culprit.",
                    deleteFile.path()));
          }
          consumePositionalDeleteFile(deleteFile, referencedDataFiles);

          break;
        case DELETE_DELETEFILE:
          // Consuming operations: OPTIMIZE TABLE
          consumeDeletedDeleteFile(getDeleteFile(i));
          consumeDeletedDataFilePartitionData(i);
          break;
        case COPY_HISTORY_EVENT:
          consumeHistoryEvent(i);
          break;
        case ORPHAN_DATAFILE:
          consumeOrphanFile(i);
          break;
        default:
          throw new Exception("Unsupported File Type: " + operationType);
      }
    }
  }

  private void consumePositionalDeleteFile(
      DeleteFile positionalDeleteFile, Set<ByteArrayWrapper> referencedDataFiles) {
    logger.debug("Adding delete file: {}", positionalDeleteFile.path());
    icebergOpCommitter.consumePositionalDeleteFile(positionalDeleteFile, referencedDataFiles);
  }

  private void consumeMergeOnReadDataFile(DataFile mergeOnReadDataFile) {
    logger.debug("Adding delete file: {}", mergeOnReadDataFile.path());
    icebergOpCommitter.consumeMergeOnReadAddDataFile(mergeOnReadDataFile);
  }

  public void consumeOrphanFile(int row) {
    String orphanFilePath = getContentFilePath(row);
    orphanFiles.add(orphanFilePath);
  }

  private void deleteOrphanFiles() {
    if (orphanFiles.size() == 0) {
      return;
    }

    ExecutorService executorService =
        ThreadPools.newWorkerPool("delete-orphan-file", DEFAULT_THREAD_POOL_SIZE);
    try {
      doDeleteOrphanFiles(executorService);
    } finally {
      CloseableSchedulerThreadPool.close(executorService, logger);
    }
  }

  private void doDeleteOrphanFiles(ExecutorService executorService) {
    logger.debug("Files to delete: {}", orphanFiles);
    Tasks.foreach(orphanFiles)
        .retry(DELETE_NUM_RETRIES)
        .stopRetryOn(NotFoundException.class)
        .suppressFailureWhenFinished()
        .executeWith(executorService)
        .onFailure(
            (filePath, exc) -> {
              logger.warn("Fail to remove file: {}", filePath, exc);
            })
        .run(
            filePath -> {
              try {
                fileIO.deleteFile(filePath);
              } catch (UncheckedIOException e) {
                logger.warn("Unable to remove newly added file: {}", filePath);
                // Not an error condition if cleanup fails.
              }
            });
  }

  /**
   * Consumes and processes a file history event at the specified row in the metadataVector and
   * recordCountVector. If the historyEventHandler is not initialized, it initializes the handler
   * using the {@link HistoryEventHandlerProvider} with the provided configuration. If the
   * historyEventHandler is an instance of {@link CopyIntoHistoryEventHandler}, it constructs a
   * {@link CopyIntoFileLoadInfo} object from the event information in the metadataVector, and the
   * record count from the recordCountVector, and the snapshot ID from the configuration's table
   * format options. Then, it processes the constructed {@link CopyIntoFileLoadInfo} object using
   * the historyEventHandler.
   *
   * @param row The row number indicating the event to be consumed and processed.
   */
  private void consumeHistoryEvent(int row) throws Exception {
    if (historyEventHandler instanceof NoopHistoryEventHandler) {
      historyEventHandler = HistoryEventHandlerProvider.get(context, config);
    }

    if (historyEventHandler instanceof CopyIntoHistoryEventHandler) {
      FileLoadInfo info =
          new CopyIntoFileLoadInfo.Builder(
                  FileLoadInfo.Util.getInfo(
                      new String(metadataVector.get(row)), CopyIntoFileLoadInfo.class))
              .setRecordsRejectedCount(rejectedRecordCountVector.get(row))
              .setSnapshotId(config.getTableFormatOptions().getSnapshotId())
              .build();
      historyEventHandler.process(info);
    }
  }

  protected void consumeManifestFile(ManifestFile manifestFile) {
    logger.debug("Adding manifest file: {}", manifestFile.path());
    icebergManifestFiles.add(manifestFile);
    icebergOpCommitter.consumeManifestFile(manifestFile);
  }

  protected void consumeDeletedDataFile(DataFile deletedDataFile) {
    logger.debug("Removing data file: {}", deletedDataFile.path());
    icebergOpCommitter.consumeDeleteDataFile(deletedDataFile);

    // Record the partition paths for deleted files during incremental metadata refresh - this will
    // help limit the
    // number of existence checks we need to perform when updating the read signature.
    // TODO: DX-58354: Logic for managing the read signature needs to be refactored into the
    // IcebergOpCommitter
    // implementations.
    if (isIncrementalRefresh(config.getIcebergTableProps().getIcebergOpType())) {
      String partitionPath = getPartitionPathFromFilePath(deletedDataFile.path().toString());
      if (partitionPath != null) {
        partitionPathsWithDeletedFiles.add(partitionPath);
      } else {
        // Just log and ignore the case where we can't match the deleted data file to a known
        // partition path.
        // For well-formed tables this should not occur.
        logger.warn(
            "Failed to find a corresponding partition path for deleted data file {}",
            deletedDataFile.path());
      }
    }
  }

  private String getContentFilePath(int row) {
    // For Dml commands, the data file paths to be deleted are carried in through RecordWriter.Path
    // field.
    Preconditions.checkNotNull(pathVector);
    // For inserted rows in MERGE, we don't have a deleted data file path but is passed here as
    // null. As a result, we need to do a null check.
    byte[] filePath = pathVector.get(row);
    if (filePath == null) {
      return null;
    }
    Text text = new Text(pathVector.get(row));
    return text.toString();
  }

  protected void consumeDeleteDataFilePath(int row) {
    String deletedDataFilePath = getContentFilePath(row);
    if (deletedDataFilePath == null) {
      return;
    }
    icebergOpCommitter.consumeDeleteDataFilePath(deletedDataFilePath);
  }

  protected void consumeDeletedDeleteFile(DeleteFile deletedDeleteFile) {
    logger.debug("Removing delete file: {}", deletedDeleteFile.path());
    icebergOpCommitter.consumeDeleteDeleteFile(deletedDeleteFile);
  }

  protected IcebergMetadataInformation getIcebergMetadataInformation(int row)
      throws IOException, ClassNotFoundException {
    return IcebergSerDe.deserializeFromByteArray(icebergMetadataVector.get(row));
  }

  protected ManifestFile getManifestFile(int row) throws IOException, ClassNotFoundException {
    return IcebergSerDe.deserializeManifestFile(
        getIcebergMetadataInformation(row).getIcebergMetadataFileByte());
  }

  protected DataFile getDeleteDataFile(int row) throws IOException, ClassNotFoundException {
    return IcebergSerDe.deserializeDataFile(
        getIcebergMetadataInformation(row).getIcebergMetadataFileByte());
  }

  protected DeleteFile getDeleteFile(int row) throws IOException, ClassNotFoundException {
    return IcebergSerDe.deserializeDeleteFile(
        getIcebergMetadataInformation(row).getIcebergMetadataFileByte());
  }

  protected Set<ByteArrayWrapper> getReferencedDataFiles(int row)
      throws IOException, ClassNotFoundException {
    return IcebergSerDe.deserializeFromByteArray(referencedDataFiles.get(row));
  }

  protected boolean isDmlCommandType() {
    IcebergCommandType commandType = config.getIcebergTableProps().getIcebergOpType();
    return commandType == IcebergCommandType.DELETE
        || commandType == IcebergCommandType.UPDATE
        || commandType == IcebergCommandType.MERGE;
  }

  protected OperationType getOperationType(int row) {
    return OperationType.valueOf(operationTypeVector.get(row));
  }

  public void commit(WriterCommitterOutputHandler outputHandler) throws Exception {
    if (icebergOpCommitter == null) {
      logger.warn("Skipping iceberg commit because opCommitter isn't initialized");
      return;
    }

    // commit history event records first, before committing real data records
    // if there is an error during event commit, we should skip te rest
    historyEventHandler.commit();

    Stopwatch stopwatch = Stopwatch.createStarted();
    ByteString newReadSignature = readSigProvider.compute(addedPartitions, deletedPartitions);
    addMetricStat(
        WriterCommitterOperator.Metric.READ_SIGNATURE_COMPUTE_TIME,
        stopwatch.elapsed(TimeUnit.MILLISECONDS));
    icebergOpCommitter.updateReadSignature(newReadSignature);
    try (AutoCloseable ac = OperatorStats.getWaitRecorder(context.getStats())) {
      injector.injectChecked(
          executionControls, INJECTOR_BEFORE_TABLE_COMMIT_ERROR, RuntimeException.class);
      icebergOpCommitter.commit(outputHandler);
    } catch (Exception ex) {
      injector.injectChecked(
          executionControls, INJECTOR_HISTORY_REVERT_ERROR, RuntimeException.class);
      revertHistoryEvents(ex);
      icebergOpCommitter.cleanup(fileIO);
      throw ex;
    }

    injector.injectChecked(
        executionControls, INJECTOR_AFTER_TABLE_COMMIT_ERROR, RuntimeException.class);
    success = true;
  }

  @VisibleForTesting
  void revertHistoryEvents(Exception ex) {
    historyEventHandler.revert(
        QueryIdHelper.getQueryId(context.getFragmentHandle().getQueryId()), ex);
  }

  protected void createReadSignProvider(
      IcebergTableProps icebergTableProps, boolean isFullRefresh) {
    if (config.isReadSignatureEnabled()) {
      ByteString existingReadSignature = null;
      if (!isFullRefresh) {
        existingReadSignature =
            ByteString.copyFrom(
                config
                    .getDatasetConfig()
                    .get()
                    .getReadDefinition()
                    .getReadSignature()
                    .toByteArray()); // TODO avoid copy
      }
      createPartitionExistsPredicate(config, isFullRefresh);

      readSigProvider =
          config
              .getSourceTablePlugin()
              .createReadSignatureProvider(
                  existingReadSignature,
                  icebergTableProps.getDataTableLocation(),
                  context.getFunctionContext().getContextInformation().getQueryStartTime(),
                  getPartitionPaths(),
                  partitionExistsPredicate,
                  isFullRefresh,
                  config.isPartialRefresh());
    }
  }

  private void consumeDeletedDataFilePartitionData(int recordIndex) {
    if (config.isReadSignatureEnabled()) {
      List<IcebergPartitionData> partitionData = getPartitionData(recordIndex);
      if (!partitionData.isEmpty()) {
        deletedPartitions.add(partitionData.get(0));
      }
    }
  }

  private void consumeManifestPartitionData(int recordIndex) {
    if (config.isReadSignatureEnabled()) {
      addedPartitions.addAll(getPartitionData(recordIndex));
    }
  }

  protected List<IcebergPartitionData> getPartitionData(int i) {
    List<IcebergPartitionData> partitionDataList = new ArrayList<>();
    UnionListReader partitionDataVectorReader = partitionDataVector.getReader();
    partitionDataVectorReader.setPosition(i);
    int size = partitionDataVectorReader.size();
    FieldReader partitionDataBinaryReader = partitionDataVectorReader.reader();
    for (int j = 0; j < size; j++) {
      partitionDataBinaryReader.setPosition(j);
      byte[] bytes = partitionDataBinaryReader.readByteArray();
      IcebergPartitionData ipd = IcebergSerDe.deserializePartitionData(bytes);
      if (ipd.get(0) != null) {
        partitionDataList.add(ipd);
      }
    }
    return partitionDataList;
  }

  protected void createPartitionExistsPredicate(WriterCommitterPOP config, boolean isFullRefresh) {
    partitionExistsPredicate =
        (path) -> {
          // We need to check existence of partition dirs in two cases:
          // 1. Full refresh - even if Hive reports a partition directory as part of the table, it
          // can be deleted, and
          //    in that case we need to exclude it from the read signature.
          // 2. Incremental refresh for directories where we found deleted data files - in these
          // cases we need to
          //    check whether the entire directory was removed.
          // For other directories during incremental refresh, we skip existence checks as they can
          // be expensive.  If
          // we did not find any deleted files in a partition directory, it must still exist.
          if (isFullRefresh || partitionPathsWithDeletedFiles.contains(path)) {
            try (AutoCloseable ac = OperatorStats.getWaitRecorder(context.getStats())) {
              return getFS(path, config).exists(Path.of(path));
            } catch (Exception e) {
              throw UserException.ioExceptionError(e).buildSilently();
            }
          }

          return true;
        };
  }

  protected FileSystem getFS(WriterCommitterPOP config) {
    try {
      return config
          .getPlugin()
          .createFS(
              Optional.ofNullable(config.getTempLocation()).orElse(config.getFinalLocation()),
              config.getProps().getUserName(),
              context);
    } catch (IOException ioe) {
      throw UserException.ioExceptionError(ioe).buildSilently();
    }
  }

  protected FileSystem getFS(String path, WriterCommitterPOP config) throws IOException {
    if (fsToCheckIfPartitionExists == null) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      fsToCheckIfPartitionExists =
          config.getSourceTablePlugin().createFS(path, config.getProps().getUserName(), context);
      addMetricStat(
          WriterCommitterOperator.Metric.FILE_SYSTEM_CREATE_TIME,
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
    return fsToCheckIfPartitionExists;
  }

  public void cleanUpManifestFiles(FileSystem fs) throws IOException, ClassNotFoundException {
    if (icebergMetadataVector == null) {
      return;
    }
    for (int i = 0; i < icebergMetadataVector.getValueCount(); i++) {
      IcebergMetadataInformation information = getIcebergMetadataInformation(i);
      OperationType operationType = getOperationType(i);
      if (operationType == OperationType.ADD_MANIFESTFILE) {
        Path p =
            Path.of(
                IcebergSerDe.deserializeManifestFile(information.getIcebergMetadataFileByte())
                    .path());
        if (fs.exists(p)) {
          fs.delete(p, false);
        }
      }
    }
  }

  @VisibleForTesting
  IcebergOpCommitter getIcebergOpCommitter() {
    return icebergOpCommitter;
  }

  @Override
  public void close() throws Exception {
    try {
      // cleanup the files if having exception before the committer commits snapshot
      if (!success) {
        // check the commit status from the icebergOpCommiter if status is not NOT_STARTED
        // we can safely delete the files as exception thrown before
        if ((icebergOpCommitter == null
            || (icebergOpCommitter != null && !icebergOpCommitter.isIcebergTableUpdated()))) {
          deleteManifestFiles(fileIO, icebergManifestFiles, false);
        }
      }
    } catch (Exception e) {
      logger.warn("Cleaning manifest files failed", e);
    }
    icebergManifestFiles.clear();
    icebergOpCommitter = null;
    AutoCloseables.close(fsToCheckIfPartitionExists, historyEventHandler);
    deleteOrphanFiles();
  }

  private void addMetricStat(WriterCommitterOperator.Metric metric, long time) {
    if (context.getStats() != null) {
      context.getStats().addLongStat(metric, time);
    }
  }

  /** Delete all data files referenced in a manifestFile */
  private static void deleteDataFilesInManifestFile(FileIO fileIO, ManifestFile manifestFile) {
    try {
      // ManifestFiles.readPaths requires snapshotId not null, created manifestFile has null
      // snapshot id
      // use a NonNullSnapshotIdManifestFileWrapper to provide a non-null dummy snapshotId
      ManifestFile nonNullSnapshotIdManifestFile =
          manifestFile.snapshotId() == null
              ? GenericManifestFile.copyOf(manifestFile).withSnapshotId(-1L).build()
              : manifestFile;
      CloseableIterable<String> dataFiles =
          ManifestFiles.readPaths(nonNullSnapshotIdManifestFile, fileIO);
      dataFiles.forEach(f -> fileIO.deleteFile(f));
    } catch (Exception e) {
      logger.warn(
          String.format("Failed to delete up data files in manifestFile %s", manifestFile), e);
    }
  }

  public static void deleteManifestFiles(
      FileIO fileIO, List<ManifestFile> manifestFiles, boolean deleteDataFiles) {
    try {
      for (ManifestFile manifestFile : manifestFiles) {
        // delete data files
        if (deleteDataFiles) {
          deleteDataFilesInManifestFile(fileIO, manifestFile);
        }

        // delete manifest file and corresponding crc file
        ManifestWritesHelper.deleteManifestFileIfExists(fileIO, manifestFile.path());
      }
    } catch (Exception e) {
      logger.warn("Failed to clean up manifest files", e);
    }
  }

  protected String getPartitionPathFromFilePath(String path) {
    // data files could be in some subdirectory of a partition dir, so traverse parent dirs until we
    // find a
    // partition dir (or give up)
    Path parent = Path.of(path).getParent();
    while (parent != null) {
      path = parent.toString();
      if (partitionPaths.contains(path)) {
        return path;
      }
      parent = parent.getParent();
    }

    return null;
  }

  private static Set<String> createPartitionPathsSet(WriterCommitterPOP config) {
    // If this is an INCREMENTAL_REFRESH op, cache all partition paths in a set for fast lookup when
    // evaluating
    // deleted data files.  If the table is unpartitioned add the table root as a single 'partition
    // path'.  If this
    // is not an Iceberg table, return an empty set.
    if (config.getIcebergTableProps() != null
        && isIncrementalRefresh(config.getIcebergTableProps().getIcebergOpType())) {
      List<String> partitionPaths = config.getIcebergTableProps().getPartitionPaths();
      String dataTableLocation = config.getIcebergTableProps().getDataTableLocation();
      if (partitionPaths != null && !partitionPaths.isEmpty()) {
        return ImmutableSet.copyOf(partitionPaths);
      } else if (dataTableLocation != null) {
        return ImmutableSet.of(dataTableLocation);
      }
    }

    return ImmutableSet.of();
  }
}
