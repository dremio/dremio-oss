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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.avro.file.DataFileConstants;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.map.CaseInsensitiveImmutableBiMap;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.FieldIdBroker;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPOP;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import io.protostuff.ByteString;

public class ManifestWritesHelper {
  private static final Logger logger = LoggerFactory.getLogger(ManifestWritesHelper.class);
  private static final String outputExtension = "avro";
  private static final int DATAFILES_PER_MANIFEST_THRESHOLD = 10000;
  private static final String CRC_FILE_EXTENTION = "crc";
  private final String ICEBERG_METADATA_FOLDER = "metadata";
  private final List<String> listOfFilesCreated;

  protected ManifestWriter<DataFile> manifestWriter;
  protected IcebergManifestWriterPOP writer;
  protected long currentNumDataFileAdded = 0;
  protected DremioFileIO dremioFileIO;

  protected VarBinaryVector inputDatafiles;
  protected IntVector operationTypes;
  protected Map<DataFile, byte[]> deletedDataFiles = new LinkedHashMap<>(); // required that removed file is cleared with each row.
  protected Optional<Integer> partitionSpecId = Optional.empty();
  private final Set<IcebergPartitionData> partitionDataInCurrentManifest = new HashSet<>();
  private final byte[] schema;

  public static ManifestWritesHelper getInstance(IcebergManifestWriterPOP writer, int columnLimit) {
    if (writer.getOptions().getIcebergTableProps().isDetectSchema()) {
      return new SchemaDiscoveryManifestWritesHelper(writer, columnLimit);
    } else {
      return new ManifestWritesHelper(writer);
    }
  }

  protected ManifestWritesHelper(IcebergManifestWriterPOP writer) {
    this.writer = writer;
    this.schema = writer.getOptions().getIcebergTableProps().getFullSchema().serialize();
    this.listOfFilesCreated = Lists.newArrayList();
    FileSystem fs  = null;
    try {
      fs = writer.getPlugin().createFS(writer.getOptions().getIcebergTableProps().getTableLocation(), SystemUser.SYSTEM_USERNAME, null);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create File System", e);
    }

    this.dremioFileIO = new DremioFileIO(fs, writer.getPlugin().getFsConfCopy(), writer.getPlugin());
  }

  public void setIncoming(VectorAccessible incoming) {
    inputDatafiles = (VarBinaryVector) getVectorFromSchemaPath(incoming, RecordWriter.ICEBERG_METADATA_COLUMN);
    operationTypes = (IntVector) getVectorFromSchemaPath(incoming, RecordWriter.OPERATION_TYPE_COLUMN);
  }

  public void startNewWriter() {
    this.currentNumDataFileAdded = 0;
    final WriterOptions writerOptions = writer.getOptions();
    final String baseMetadataLocation = writerOptions.getIcebergTableProps().getTableLocation() + Path.SEPARATOR + ICEBERG_METADATA_FOLDER;
    final PartitionSpec partitionSpec = getPartitionSpec(writer.getOptions());
    this.partitionSpecId = Optional.of(partitionSpec.specId());
    final String icebergManifestFileExt = "." + outputExtension;
    final OutputFile manifestLocation = dremioFileIO.newOutputFile(baseMetadataLocation + Path.SEPARATOR + UUID.randomUUID() + icebergManifestFileExt);
    listOfFilesCreated.add(manifestLocation.location());
    partitionDataInCurrentManifest.clear();
    this.manifestWriter = ManifestFiles.write(partitionSpec, manifestLocation);
  }

  public void processIncomingRow(int recordIndex) throws IOException {
    try {
      Preconditions.checkNotNull(manifestWriter);
      final byte[] metaInfoBytes = inputDatafiles.get(recordIndex);
      final Integer operationTypeValue = operationTypes.get(recordIndex);
      final IcebergMetadataInformation icebergMetadataInformation = IcebergSerDe.deserializeFromByteArray(metaInfoBytes);
      final OperationType operationType = OperationType.valueOf(operationTypeValue);
      switch (operationType) {
        case ADD_DATAFILE:
          final DataFile dataFile = IcebergSerDe.deserializeDataFile(icebergMetadataInformation.getIcebergMetadataFileByte());
          addDataFile(dataFile);
          currentNumDataFileAdded++;
          break;
        case DELETE_DATAFILE:
          deletedDataFiles.put(IcebergSerDe.deserializeDataFile(icebergMetadataInformation.getIcebergMetadataFileByte()), metaInfoBytes);
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
    IcebergPartitionData ipd = IcebergPartitionData.fromStructLike(getPartitionSpec(writer.getOptions()), dataFile.partition());
    if (writer.getOptions().isReadSignatureSupport()) {
      partitionDataInCurrentManifest.add(ipd);
    }
    manifestWriter.add(dataFile);
  }

  public void processDeletedFiles(BiConsumer<DataFile, byte[]> processLogic) {
    deletedDataFiles.forEach(processLogic);
    deletedDataFiles.clear();
  }

  public long length() {
    Preconditions.checkNotNull(manifestWriter);
    return manifestWriter.length();
  }

  public boolean hasReachedMaxLen() {
    return (length() + DataFileConstants.DEFAULT_SYNC_INTERVAL >= TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT)
            || (currentNumDataFileAdded >= DATAFILES_PER_MANIFEST_THRESHOLD);
  }

  public Optional<ManifestFile> write() throws IOException {
    if (currentNumDataFileAdded == 0) {
      deleteRunningManifestFile();
      return Optional.empty();
    }
    manifestWriter.close();
    return Optional.of(manifestWriter.toManifestFile());
  }

  public byte[] getWrittenSchema() {
    return schema;
  }

  PartitionSpec getPartitionSpec(WriterOptions writerOptions) {
    PartitionSpec partitionSpec = writer.getOptions().getDeserializedPartitionSpec();
    if (partitionSpec != null) {
      return partitionSpec;
    }

    List<String> partitionColumns = writerOptions.getIcebergTableProps().getPartitionColumnNames();
    BatchSchema batchSchema = writerOptions.getIcebergTableProps().getFullSchema();

    Schema icebergSchema = null;
    if (writerOptions.getExtendedProperty() != null) {
      icebergSchema = getIcebergSchema(writerOptions.getExtendedProperty(), batchSchema, writerOptions.getIcebergTableProps().getTableName());
    }

    return IcebergUtils.getIcebergPartitionSpec(batchSchema, partitionColumns, icebergSchema);
        /*
         TODO: currently we don't support partition spec update for by default spec ID will be 0. in future if
               we start supporting partition spec id. then Id must be inherited from data files(input to this writer)
         */
  }

  protected Schema getIcebergSchema(ByteString extendedProperty, BatchSchema batchSchema, String tableName) {
    try {
      IcebergProtobuf.IcebergDatasetXAttr icebergDatasetXAttr = LegacyProtobufSerializer.parseFrom(IcebergProtobuf.IcebergDatasetXAttr.PARSER,
              extendedProperty.toByteArray());
      List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = icebergDatasetXAttr.getColumnIdsList();
      Map<String, Integer> icebergColumns = new HashMap<>();
      icebergColumnIDs.forEach(field -> icebergColumns.put(field.getSchemaPath(), field.getId()));
      CaseInsensitiveImmutableBiMap<Integer> icebergColumnIDMap = newImmutableMap(icebergColumns);
      if (icebergColumnIDMap != null && icebergColumnIDMap.size() > 0) {
        FieldIdBroker.SeededFieldIdBroker fieldIdBroker = new FieldIdBroker.SeededFieldIdBroker(icebergColumnIDMap);
        SchemaConverter schemaConverter = SchemaConverter.getBuilder().setTableName(tableName).build();
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
      if (manifestWriter == null) {
        return;
      }
      manifestWriter.close();
      ManifestFile manifestFile = manifestWriter.toManifestFile();
      logger.debug("Removing {} as it'll be re-written with a new schema", manifestFile.path());
      deleteManifestFileIfExists(dremioFileIO, manifestFile.path());
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
      ManifestWritesHelper.deleteManifestFileIfExists(dremioFileIO, path);
    }
  }

  public static boolean deleteManifestFileIfExists(DremioFileIO dremioFileIO, String filePath) {
    try {
      dremioFileIO.deleteFile(filePath);
      deleteManifestCrcFileIfExists(dremioFileIO, filePath);
      return true;
    } catch (Exception e) {
      logger.warn("Error while deleting file {}", filePath, e);
      return false;
    }
  }

  public static boolean deleteManifestCrcFileIfExists(DremioFileIO dremioFileIO, String manifestFilePath) {
    try{
      com.dremio.io.file.Path p = com.dremio.io.file.Path.of(manifestFilePath);
      String fileName = p.getName();
      com.dremio.io.file.Path parentPath = p.getParent();
      String crcFilePath = parentPath + com.dremio.io.file.Path.SEPARATOR + "." + fileName + "." + CRC_FILE_EXTENTION;
      dremioFileIO.deleteFile(crcFilePath);
      return true;
    } catch (Exception e) {
      logger.warn("Error while deleting crc file for {}", manifestFilePath, e);
      return false;
    }
  }
}
