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

import static com.dremio.common.map.CaseInsensitiveImmutableBiMap.newImmutableMap;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.map.CaseInsensitiveImmutableBiMap;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.FieldIdBroker;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPOP;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import io.protostuff.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.avro.file.DataFileConstants;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManifestWritesHelper {
  private static class LazyManifestWriterPool {
    static Integer DEFAULT_POOL_SIZE = 5;
    private ThreadPoolExecutor executor;
    int poolSize;

    public LazyManifestWriterPool(final int poolSize) {
      this.poolSize = poolSize;
      this.executor = null;
    }

    public LazyManifestWriterPool() {
      this(DEFAULT_POOL_SIZE);
    }

    public ThreadPoolExecutor getPool() {
      if (executor == null) {
        executor =
            new ThreadPoolExecutor(
                poolSize,
                poolSize,
                1,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("manifest-writers"));
        executor.allowCoreThreadTimeOut(true);
      }
      return executor;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(ManifestWritesHelper.class);
  private static final String outputExtension = "avro";
  private static final int DATAFILES_PER_MANIFEST_THRESHOLD = 10000;
  private static final String CRC_FILE_EXTENTION = "crc";
  private final String ICEBERG_METADATA_FOLDER = "metadata";
  private final List<String> listOfFilesCreated;

  protected LazyManifestWriter manifestWriter;
  protected IcebergManifestWriterPOP writer;
  protected long currentNumDataFileAdded = 0;
  protected DataFile currentDataFile;
  protected DeleteFile currentPositionalDeleteFile;
  protected FileSystem fs;
  protected FileIO fileIO;
  protected VarBinaryVector inputDatafiles;
  protected IntVector operationTypes;

  // data files referenced by positional delete rows (V2 Iceberg Table Operations)
  protected VarBinaryVector referencedDataFiles;

  protected Map<DataFile, byte[]> deletedDataFiles =
      new LinkedHashMap<>(); // required that removed file is cleared with each row.

  // Merge On Read DML data-files
  protected Map<DataFile, byte[]> mergeOnReadDataFiles = new LinkedHashMap<>();

  // Merge On Read DML Positional Delete Files
  protected Map<DeleteFile, DeleteFileMetaInfo> positionalDeleteFiles = new LinkedHashMap<>();

  protected Optional<Integer> partitionSpecId = Optional.empty();
  private final Set<IcebergPartitionData> partitionDataInCurrentManifest = new HashSet<>();
  private final byte[] schema;
  private Set<DataFile> orphanFiles = new HashSet<>();

  private final LazyManifestWriterPool threadPool;

  private final List<CompletableFuture> manifestWriterFutures = new ArrayList<>();

  public static ManifestWritesHelper getInstance(
      IcebergManifestWriterPOP writer, OperatorContext context) {
    if (writer
        .getOptions()
        .getTableFormatOptions()
        .getIcebergSpecificOptions()
        .getIcebergTableProps()
        .isDetectSchema()) {
      return new SchemaDiscoveryManifestWritesHelper(writer, context);
    } else {
      return new ManifestWritesHelper(writer);
    }
  }

  protected ManifestWritesHelper(IcebergManifestWriterPOP writer) {
    this.writer = writer;
    IcebergTableProps tableProps =
        writer
            .getOptions()
            .getTableFormatOptions()
            .getIcebergSpecificOptions()
            .getIcebergTableProps();
    this.schema = tableProps.getFullSchema().serialize();
    this.listOfFilesCreated = Lists.newArrayList();
    try {
      fs =
          writer
              .getPlugin()
              .createFS(tableProps.getTableLocation(), SystemUser.SYSTEM_USERNAME, null);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create File System", e);
    }

    Preconditions.checkArgument(
        writer.getPlugin() instanceof SupportsIcebergRootPointer,
        "Invalid plugin in ManifestWritesHelper - plugin does not support Iceberg");
    this.fileIO =
        ((SupportsIcebergRootPointer) writer.getPlugin())
            .createIcebergFileIO(fs, null, null, null, null);
    this.threadPool = new LazyManifestWriterPool();
  }

  public void setIncoming(VectorAccessible incoming) {
    inputDatafiles =
        (VarBinaryVector) getVectorFromSchemaPath(incoming, RecordWriter.ICEBERG_METADATA_COLUMN);
    operationTypes =
        (IntVector) getVectorFromSchemaPath(incoming, RecordWriter.OPERATION_TYPE_COLUMN);
    if (DmlUtils.isMergeOnReadDmlOperation(writer.getOptions())) {
      referencedDataFiles =
          (VarBinaryVector)
              getVectorFromSchemaPath(incoming, RecordWriter.REFERENCED_DATA_FILES_COLUMN);
    }
  }

  public void startNewWriter() {
    this.currentNumDataFileAdded = 0;
    final WriterOptions writerOptions = writer.getOptions();
    final String baseMetadataLocation =
        writerOptions
                .getTableFormatOptions()
                .getIcebergSpecificOptions()
                .getIcebergTableProps()
                .getTableLocation()
            + Path.SEPARATOR
            + ICEBERG_METADATA_FOLDER;
    final PartitionSpec partitionSpec = getPartitionSpec(writer.getOptions());
    this.partitionSpecId = Optional.of(partitionSpec.specId());
    final String icebergManifestFileExt = "." + outputExtension;
    final String manifestLocation =
        baseMetadataLocation + Path.SEPARATOR + UUID.randomUUID() + icebergManifestFileExt;
    listOfFilesCreated.add(manifestLocation);
    partitionDataInCurrentManifest.clear();
    this.manifestWriter = new LazyManifestWriter(fileIO, manifestLocation, partitionSpec);
  }

  public LazyManifestWriter getManifestWriter() {
    return manifestWriter;
  }

  public void processIncomingRow(int recordIndex) throws IOException {
    try {
      Preconditions.checkNotNull(manifestWriter);
      final byte[] metaInfoBytes = inputDatafiles.get(recordIndex);
      final Integer operationTypeValue = operationTypes.get(recordIndex);
      final OperationType operationType = OperationType.valueOf(operationTypeValue);
      if (operationType == OperationType.COPY_HISTORY_EVENT) {
        return;
      }

      final IcebergMetadataInformation icebergMetadataInformation =
          IcebergSerDe.deserializeFromByteArray(metaInfoBytes);

      if (operationType == OperationType.ADD_DELETEFILE) {
        currentPositionalDeleteFile =
            IcebergSerDe.deserializeDeleteFile(
                icebergMetadataInformation.getIcebergMetadataFileByte());
      } else {
        currentDataFile =
            IcebergSerDe.deserializeDataFile(
                icebergMetadataInformation.getIcebergMetadataFileByte());
      }
      if (currentDataFile == null && currentPositionalDeleteFile == null) {
        throw new IOException("Iceberg data file cannot be empty or null.");
      }
      switch (operationType) {
        case ADD_DATAFILE:
          if (DmlUtils.isMergeOnReadDmlOperation(writer.getOptions())) {
            // Merge On Read DML data-files
            logger.debug(
                String.format(
                    "Processing data-file '%s' in manifest writer", currentDataFile.path()));
            mergeOnReadDataFiles.put(currentDataFile, metaInfoBytes);
          } else {
            addDataFile(currentDataFile);
            currentNumDataFileAdded++;
          }
          break;
        case ADD_DELETEFILE:
          // Merge On Read DML Positional Delete Files
          logger.debug(
              String.format(
                  "Processing positional delete-file '%s' in manifest writer",
                  currentPositionalDeleteFile.path()));

          positionalDeleteFiles.put(
              currentPositionalDeleteFile,
              new DeleteFileMetaInfo(metaInfoBytes, referencedDataFiles.get(recordIndex)));

          break;
        case DELETE_DATAFILE:
          deletedDataFiles.put(currentDataFile, metaInfoBytes);
          break;
        case ORPHAN_DATAFILE:
          orphanFiles.add(currentDataFile);
          break;
        case DELETE_DELETEFILE:
          break;
        default:
          throw new IOException("Unsupported File type - " + operationType);
      }
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  protected void addDataFile(DataFile dataFile) {
    logger.trace("Adding data file: {}", dataFile.path());
    IcebergPartitionData ipd =
        IcebergPartitionData.fromStructLike(
            getPartitionSpec(writer.getOptions()), dataFile.partition());
    if (writer.getOptions().isReadSignatureSupport()) {
      partitionDataInCurrentManifest.add(ipd);
    }
    manifestWriter.getInstance().add(dataFile);
  }

  public void processDeletedFiles(BiConsumer<DataFile, byte[]> processLogic) {
    deletedDataFiles.forEach(processLogic);
    deletedDataFiles.clear();
  }

  public void processOrphanFiles(Consumer<DataFile> processLogic) {
    orphanFiles.forEach(processLogic);
    orphanFiles.clear();
  }

  /**
   * Process the Merge-On-Read positional delete files to build the manifest file
   *
   * @param processLogic - logic to process the positional delete files
   */
  public void processPositionalDeleteFiles(
      BiConsumer<DeleteFile, DeleteFileMetaInfo> processLogic) {
    positionalDeleteFiles.forEach(processLogic);
    positionalDeleteFiles.clear();
  }

  /**
   * Process the Merge On Read DML data-files to build the manifest file
   *
   * @param processLogic - logic to process the data-files
   */
  public void processMergeOnReadDataFiles(BiConsumer<DataFile, byte[]> processLogic) {
    mergeOnReadDataFiles.forEach(processLogic);
    mergeOnReadDataFiles.clear();
  }

  public long length() {
    Preconditions.checkNotNull(manifestWriter);
    return manifestWriter.getInstance().length();
  }

  public boolean hasReachedMaxLen() {
    return (length() + DataFileConstants.DEFAULT_SYNC_INTERVAL
            >= TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT)
        || (currentNumDataFileAdded >= DATAFILES_PER_MANIFEST_THRESHOLD);
  }

  public void prepareWrite() {}

  public boolean canWrite() {
    if (currentNumDataFileAdded == 0) {
      deleteRunningManifestFile();
      return false;
    }
    return true;
  }

  public ManifestFile write() throws IOException {
    manifestWriter.getInstance().close();
    return manifestWriter.getInstance().toManifestFile();
  }

  /** Async version of writer() */
  public void write(
      ManifestFileRecordWriter.WritingContext context,
      BiConsumer<ManifestFile, ManifestFileRecordWriter.WritingContext>
          processGeneratedManifestFileCall)
      throws IOException {
    CompletableFuture writerFuture =
        CompletableFuture.supplyAsync(
                () -> {
                  try {
                    context.getManifestWriter().getInstance().close();
                    return context.getManifestWriter().getInstance().toManifestFile();
                  } catch (Exception ex) {
                    throw new CompletionException(ex);
                  }
                },
                threadPool.getPool())
            .whenComplete(
                (manifestFile, ex) -> {
                  if (ex != null) {
                    throw new CompletionException(ex);
                  }

                  try {
                    processGeneratedManifestFileCall.accept(manifestFile, context);
                  } catch (Exception e) {
                    throw new CompletionException(e);
                  }
                })
            .exceptionally(
                e -> {
                  CompletionException exp = (CompletionException) e;
                  if (exp.getCause() instanceof IOException) {
                    logger.error("Error while writing manifest file", exp);
                  }
                  throw new CompletionException(e);
                });

    manifestWriterFutures.add(writerFuture);
  }

  public void getFutureResults() {
    manifestWriterFutures.forEach(CompletableFuture::join);
  }

  public byte[] getWrittenSchema() {
    return schema;
  }

  PartitionSpec getPartitionSpec(WriterOptions writerOptions) {
    PartitionSpec partitionSpec =
        Optional.ofNullable(
                writerOptions
                    .getTableFormatOptions()
                    .getIcebergSpecificOptions()
                    .getIcebergTableProps())
            .map(props -> props.getDeserializedPartitionSpec())
            .orElse(null);

    if (partitionSpec != null) {
      return partitionSpec;
    }

    IcebergTableProps tableProps =
        writerOptions.getTableFormatOptions().getIcebergSpecificOptions().getIcebergTableProps();
    List<String> partitionColumns = tableProps.getPartitionColumnNames();
    BatchSchema batchSchema = tableProps.getFullSchema();

    Schema icebergSchema = null;
    if (writerOptions.getExtendedProperty() != null) {
      icebergSchema =
          getIcebergSchema(
              writerOptions.getExtendedProperty(), batchSchema, tableProps.getTableName());
    }

    return IcebergUtils.getIcebergPartitionSpec(batchSchema, partitionColumns, icebergSchema);
    /*
    TODO: currently we don't support partition spec update for by default spec ID will be 0. in future if
          we start supporting partition spec id. then Id must be inherited from data files(input to this writer)
    */
  }

  protected Schema getIcebergSchema(
      ByteString extendedProperty, BatchSchema batchSchema, String tableName) {
    try {
      IcebergProtobuf.IcebergDatasetXAttr icebergDatasetXAttr =
          LegacyProtobufSerializer.parseFrom(
              IcebergProtobuf.IcebergDatasetXAttr.PARSER, extendedProperty.toByteArray());
      List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs =
          icebergDatasetXAttr.getColumnIdsList();
      Map<String, Integer> icebergColumns = new HashMap<>();
      icebergColumnIDs.forEach(field -> icebergColumns.put(field.getSchemaPath(), field.getId()));
      CaseInsensitiveImmutableBiMap<Integer> icebergColumnIDMap = newImmutableMap(icebergColumns);
      if (icebergColumnIDMap != null && icebergColumnIDMap.size() > 0) {
        FieldIdBroker.SeededFieldIdBroker fieldIdBroker =
            new FieldIdBroker.SeededFieldIdBroker(icebergColumnIDMap);
        SchemaConverter schemaConverter =
            SchemaConverter.getBuilder().setTableName(tableName).build();
        return schemaConverter.toIcebergSchema(batchSchema, fieldIdBroker);
      } else {
        return null;
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Could not deserialize Parquet dataset info", e);
    }
  }

  protected void deleteRunningManifestFile() {
    try {
      if (manifestWriter == null || !manifestWriter.isInitialized()) {
        return;
      }
      manifestWriter.getInstance().close();
      ManifestFile manifestFile = manifestWriter.getInstance().toManifestFile();
      logger.debug("Removing {} as it'll be re-written with a new schema", manifestFile.path());
      deleteManifestFileIfExists(fileIO, manifestFile.path());
      manifestWriter = null;
    } catch (Exception e) {
      logger.warn("Error while closing stale manifest", e);
    }
  }

  Set<IcebergPartitionData> partitionDataInCurrentManifest() {
    return partitionDataInCurrentManifest;
  }

  protected void abort() {
    for (String path : this.listOfFilesCreated) {
      ManifestWritesHelper.deleteManifestFileIfExists(fileIO, path);
    }
  }

  public static boolean deleteManifestFileIfExists(FileIO fileIO, String filePath) {
    try {
      fileIO.deleteFile(filePath);
      deleteManifestCrcFileIfExists(fileIO, filePath);
      return true;
    } catch (Exception e) {
      logger.warn("Error while deleting file {}", filePath, e);
      return false;
    }
  }

  public static boolean deleteManifestCrcFileIfExists(FileIO fileIO, String manifestFilePath) {
    try {
      com.dremio.io.file.Path p = com.dremio.io.file.Path.of(manifestFilePath);
      String fileName = p.getName();
      com.dremio.io.file.Path parentPath = p.getParent();
      String crcFilePath =
          parentPath
              + com.dremio.io.file.Path.SEPARATOR
              + "."
              + fileName
              + "."
              + CRC_FILE_EXTENTION;
      fileIO.deleteFile(crcFilePath);
      return true;
    } catch (Exception e) {
      logger.warn("Error while deleting crc file for {}", manifestFilePath, e);
      return false;
    }
  }

  /** Positional Delete File Meta-Info. Each instance contains essential iceberg commit phase. */
  public static class DeleteFileMetaInfo {

    // Summarized metrics on the positional delete file which will be readable in the metadata.json
    private final byte[] metaInfoBytes;

    // Set of distinct data-files referenced by the positional delete file.
    // Essential for concurrency validation
    private final byte[] referencedDataFiles;

    public DeleteFileMetaInfo(byte[] metaInfoBytes, byte[] referencedDataFiles) {
      this.metaInfoBytes = metaInfoBytes;
      this.referencedDataFiles = referencedDataFiles;
    }

    public byte[] getMetaInfoBytes() {
      return metaInfoBytes;
    }

    public byte[] getReferencedDataFiles() {
      return referencedDataFiles;
    }
  }
}
